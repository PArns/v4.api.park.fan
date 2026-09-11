import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { DataSource } from "typeorm";
import { DowntimeReconstructionProcessor } from "./downtime-reconstruction.processor";
import { DowntimeProfileService } from "../../analytics/downtime-profile.service";
import { DowntimeRecoveryService } from "../../analytics/downtime-recovery.service";
import {
  OUTAGE_EXPOSURE_SQL,
  OUTAGE_INTERVALS_SQL,
} from "../../analytics/utils/outage-reconstruction.sql";
import { CLOSURE_GAP_INTERVALS_SQL } from "../../common/utils/closure-gap.sql";

/**
 * What the nightly reconstruction is allowed to DELETE.
 *
 * The job rewrites its rolling window whole: everything in range goes, and the
 * two statements' output is written back. Keyed on `parkId` alone that is a
 * data-loss bug rather than a refresh, because the two sides of it are not the
 * same population. The delete covers the park; the insert covers the rides the
 * statements still return. A ride that left `tracked` between two runs — a
 * merge moving `last_merged_at` into the scan window, `retired_at`, a flip to
 * `open_with_park`, the park losing its schedule — falls out of the second set
 * and not the first, so its reconstructed history is deleted and never written
 * again. Nothing else writes these tables, and the job never revisits a row
 * once it has aged out of the window, so the loss is permanent and silent.
 *
 * These tests pin the predicate rather than the wording: the delete names the
 * rides this run covered, that set is read off the results already in hand
 * (`exposure` is one row per tracked ride per operating day), and a run that
 * produced nothing deletes nothing instead of everything.
 */
describe("DowntimeReconstructionProcessor", () => {
  /** A stand-in for one reconstruction's results. */
  interface Fixture {
    intervals?: Array<Record<string, unknown>>;
    exposure?: Array<Record<string, unknown>>;
    closureGaps?: Array<Record<string, unknown>>;
    scanStart?: Date;
  }

  const SCAN_START = new Date("2026-08-12T00:00:00.000Z");

  const interval = (attractionId: string, parkId = "park-1") => ({
    attractionId,
    parkId,
    startedAt: new Date("2026-09-01T10:00:00.000Z"),
    endedAt: new Date("2026-09-01T11:00:00.000Z"),
    operatingMinutes: 60,
    observedOperatingMinutes: 60,
    wallMinutes: 60,
    operatingDays: 1,
    endReason: "recovered",
    startCensored: false,
    likelyWorksPeriod: false,
    rowsInSpell: 12,
    heartbeatRows: 0,
    startOpDay: "2026-09-01",
  });

  const exposureDay = (attractionId: string, parkId = "park-1") => ({
    attractionId,
    parkId,
    opDay: "2026-09-01",
    parkOpenMinutes: 600,
    operatingMinutes: 600,
    downMinutes: 0,
    downMinutesObserved: 0,
    closedMinutes: 0,
    refurbishmentMinutes: 0,
    absentMinutes: 0,
    unobservedMinutes: 0,
  });

  const closureGap = (attractionId: string, parkId = "park-1") => ({
    attractionId,
    parkId,
    startedAt: new Date("2026-09-01T14:00:00.000Z"),
    endedAt: new Date("2026-09-01T14:30:00.000Z"),
    startOpDay: "2026-09-01",
    wallMinutes: 30,
  });

  /** Every `manager.query` the transaction issued, in order. */
  let writes: Array<{ sql: string; params: unknown[] }>;
  let inserted: Array<{ table: unknown; rows: Array<Record<string, unknown>> }>;

  const run = async (fixture: Fixture): Promise<void> => {
    writes = [];
    inserted = [];

    const query = jest.fn(async (sql: string) => {
      if (sql.includes("AS scan_start")) {
        return [{ scan_start: fixture.scanStart ?? SCAN_START }];
      }
      if (sql === OUTAGE_INTERVALS_SQL) return fixture.intervals ?? [];
      if (sql === OUTAGE_EXPOSURE_SQL) return fixture.exposure ?? [];
      if (sql === CLOSURE_GAP_INTERVALS_SQL) return fixture.closureGaps ?? [];
      // The retention prune, which runs on the DataSource rather than in the
      // transaction.
      return [];
    });

    const manager = {
      query: jest.fn(async (sql: string, params: unknown[]) => {
        writes.push({ sql, params });
        return [];
      }),
      createQueryBuilder: () => {
        const builder = {
          insert: () => builder,
          into: (table: unknown) => {
            builder._table = table;
            return builder;
          },
          values: (rows: Array<Record<string, unknown>>) => {
            inserted.push({ table: builder._table, rows });
            return builder;
          },
          orIgnore: () => builder,
          execute: async () => undefined,
          _table: undefined as unknown,
        };
        return builder;
      },
    };

    const dataSource = {
      query,
      transaction: jest.fn(async (cb: (m: typeof manager) => Promise<void>) =>
        cb(manager),
      ),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DowntimeReconstructionProcessor,
        { provide: DataSource, useValue: dataSource },
        { provide: DowntimeProfileService, useValue: { rebuild: jest.fn() } },
        { provide: DowntimeRecoveryService, useValue: { rebuild: jest.fn() } },
      ],
    }).compile();

    const processor = module.get(DowntimeReconstructionProcessor);
    await processor.handleReconstruct({
      data: {},
    } as Job<{ parkIds?: string[]; windowDays?: number }>);
  };

  /** The two deletes the transaction opens with, in order. */
  const deletes = () => {
    const outages = writes.find((w) =>
      w.sql.includes("DELETE FROM attraction_outages"),
    );
    const exposure = writes.find((w) =>
      w.sql.includes("DELETE FROM attraction_exposure_days"),
    );
    expect(outages).toBeDefined();
    expect(exposure).toBeDefined();
    return { outages: outages!, exposure: exposure! };
  };

  it("deletes only the rides this run covered, so a ride that left `tracked` keeps its history", async () => {
    // `kept` is the ride that fell out: it has stored rows in the window from
    // an earlier run and appears in nothing this one returned. Under the old
    // park-wide predicate its rows went with everybody else's and were never
    // written back.
    await run({
      intervals: [interval("ride-down")],
      exposure: [exposureDay("ride-down"), exposureDay("ride-quiet")],
    });

    const { outages, exposure } = deletes();
    for (const del of [outages, exposure]) {
      expect(del.sql).toContain('"attractionId" = ANY($3::uuid[])');
      const ids = del.params[2] as string[];
      expect(ids).toEqual(expect.arrayContaining(["ride-down", "ride-quiet"]));
      expect(ids).not.toContain("ride-gone");
    }
  });

  it("takes the covered set from every statement, deduplicated", async () => {
    // `exposure` is the tracked population — one row per tracked ride per
    // operating day — but it is not the whole story: the closure-gap statement
    // serves blind parks and, unlike `tracked`, does not exclude free-flow
    // rides, so a ride can be written without appearing there.
    await run({
      intervals: [interval("ride-a"), interval("ride-a")],
      exposure: [exposureDay("ride-a"), exposureDay("ride-b")],
      closureGaps: [closureGap("ride-c")],
    });

    const ids = deletes().outages.params[2] as string[];
    expect([...ids].sort()).toEqual(["ride-a", "ride-b", "ride-c"]);
  });

  it("deletes nothing when the run produced nothing", async () => {
    // The same bug at its widest: a reconstruction that returns no rows at all
    // — every statement failing, a park filter that matches nothing — used to
    // erase the whole window and write nothing back. An empty uuid[] matches no
    // row, so the delete is a no-op rather than a wipe.
    await run({});

    const { outages, exposure } = deletes();
    expect(outages.params[2]).toEqual([]);
    expect(exposure.params[2]).toEqual([]);
    expect(inserted).toHaveLength(0);
  });

  it("keeps the range and park bounds it always had", async () => {
    // The id predicate NARROWS the delete; it does not replace what was there.
    // Dropping either of the other two would widen it back across parks or
    // past the scan window, which no insert covers.
    await run({ exposure: [exposureDay("ride-a")] });

    const { outages, exposure } = deletes();
    expect(outages.sql).toContain("started_at >= $1");
    expect(outages.sql).toContain('"parkId" = ANY($2::uuid[])');
    expect(outages.params[0]).toEqual(SCAN_START);
    expect(outages.params[1]).toBeNull();
    expect(exposure.sql).toContain("op_day >= $1::date");
    expect(exposure.sql).toContain('"parkId" = ANY($2::uuid[])');
  });

  it("still rewrites a covered ride's window whole", async () => {
    // The reason the delete exists at all: an outage can GROW between runs, so
    // a covered ride's rows are cleared and re-inserted rather than upserted.
    // Narrowing the predicate must not turn that into an append.
    await run({
      intervals: [interval("ride-a")],
      exposure: [exposureDay("ride-a")],
      closureGaps: [closureGap("ride-a")],
    });

    expect(deletes().outages.params[2]).toEqual(["ride-a"]);
    const rows = inserted.flatMap((batch) => batch.rows);
    expect(rows.filter((row) => row.signal === "down")).toHaveLength(1);
    expect(rows.filter((row) => row.signal === "closed_gap")).toHaveLength(1);
  });
});
