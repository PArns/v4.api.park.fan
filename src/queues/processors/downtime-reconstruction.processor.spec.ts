import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { DataSource } from "typeorm";
import {
  DowntimeReconstructionProcessor,
  LOCK_TIMEOUT_MS,
  READ_TIMEOUT_FLOOR_MS,
  READ_TIMEOUT_PER_SCAN_DAY_MS,
  reconstructionReadLimits,
  WRITE_LIMITS,
} from "./downtime-reconstruction.processor";
import { statementLimitSql } from "../../common/utils/statement-limits.util";
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
    /** Make the closure-gap statement throw, the one failure the job swallows. */
    closureGapsFail?: boolean;
    /** The message it throws with. */
    closureGapsError?: string;
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

  /** Every `manager.query` any transaction issued, in order. */
  let writes: Array<{ sql: string; params: unknown[] }>;
  /** The same statements, grouped by the transaction that issued them. */
  let transactions: string[][];
  let inserted: Array<{ table: unknown; rows: Array<Record<string, unknown>> }>;

  const run = async (
    fixture: Fixture,
    data: { parkIds?: string[]; windowDays?: number } = {},
  ): Promise<void> => {
    writes = [];
    transactions = [];
    inserted = [];

    const route = async (sql: string): Promise<unknown> => {
      if (sql.includes("AS scan_start")) {
        return [{ scan_start: fixture.scanStart ?? SCAN_START }];
      }
      if (sql === OUTAGE_INTERVALS_SQL) return fixture.intervals ?? [];
      if (sql === OUTAGE_EXPOSURE_SQL) return fixture.exposure ?? [];
      if (sql === CLOSURE_GAP_INTERVALS_SQL) {
        if (fixture.closureGapsFail) {
          throw new Error(
            fixture.closureGapsError ??
              'relation "attraction_exposure_days" does not exist',
          );
        }
        return fixture.closureGaps ?? [];
      }
      // The deletes, the SET LOCALs and the retention prune.
      return [];
    };

    // Every statement now runs inside a transaction of its own, because that
    // is the only scope `SET LOCAL` has. A bare `dataSource.query` would run
    // with no deadline at all, so the stub fails the test if one is issued.
    const query = jest.fn(async (sql: string) => {
      throw new Error(`unscoped query outside a transaction: ${sql}`);
    });

    let current: string[] = [];
    const manager = {
      query: jest.fn(async (sql: string, params: unknown[]) => {
        writes.push({ sql, params });
        current.push(sql);
        return route(sql);
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

    // Transactions are run one at a time even where the processor starts two
    // at once (`Promise.all`), so each statement lands in the list of the
    // transaction that issued it.
    let queue: Promise<unknown> = Promise.resolve();
    const dataSource = {
      query,
      transaction: jest.fn((cb: (m: typeof manager) => Promise<unknown>) => {
        const next = queue.then(async () => {
          current = [];
          transactions.push(current);
          return cb(manager);
        });
        queue = next.catch(() => undefined);
        return next;
      }),
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
      data,
    } as Job<{ parkIds?: string[]; windowDays?: number }>);
  };

  /** The transaction that ran a given statement, SET LOCALs included. */
  const transactionOf = (sql: string): string[] => {
    const found = transactions.find((tx) => tx.includes(sql));
    expect(found).toBeDefined();
    return found!;
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
    // The ride that fell out is not a fixture and cannot be: it exists only as
    // rows in a table the processor never reads. What the processor decides is
    // the PREDICATE, so what has to be pinned is that the predicate is an
    // allowlist and that it is closed — every id in it came from this run's
    // results, so any id that did not (a merged, retired or newly free-flow
    // ride) is untouched by construction.
    //
    // Asserting exact equality rather than `arrayContaining` is the whole
    // point. A containment check passes just as well on the old park-wide
    // delete with an extra `= ANY(...)` bolted on that happens to list
    // everybody, and it passes on any future widening of the set.
    await run({
      intervals: [interval("ride-down")],
      exposure: [exposureDay("ride-down"), exposureDay("ride-quiet")],
    });

    const { outages, exposure } = deletes();
    for (const del of [outages, exposure]) {
      expect(del.sql).toContain('"attractionId" = ANY($3::uuid[])');
      expect([...(del.params[2] as string[])].sort()).toEqual([
        "ride-down",
        "ride-quiet",
      ]);
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

  it("does not delete the closure gaps it could not read", async () => {
    // The one statement of the three that is allowed to fail quietly, and the
    // catch block is a second route to the same data loss: `coveredIds` still
    // names every blind-park ride, because those come through `exposure`, which
    // succeeded. An unqualified delete then strips their stored `closed_gap`
    // rows while only the `down` rows are written back.
    //
    // An empty result and a failed statement are not the same claim, so the
    // delete has to see the difference — which is why the flag exists rather
    // than `closureGaps.length`.
    await run({
      exposure: [exposureDay("ride-a")],
      intervals: [interval("ride-a")],
      closureGapsFail: true,
    });

    const { outages, exposure } = deletes();
    expect(outages.sql).toContain("signal = 'down'");
    // Only the outage table carries the signal; the closure-gap statement
    // writes no exposure day, so that delete is unqualified either way.
    expect(exposure.sql).not.toContain("signal");
    // And the run still covers the ride, so its DOWN history is rewritten
    // whole. Restricting the signal must not turn the delete off.
    expect(outages.params[2]).toEqual(["ride-a"]);
  });

  it("deletes both signals when the closure-gap statement merely found nothing", async () => {
    // The other half of the same distinction. A successful statement returning
    // no rows IS a statement about the window: the gaps that used to be there
    // are gone — a ride that newly crossed MAX_GAP_DAY_SHARE, say — and the
    // stored rows have to go with them.
    await run({
      exposure: [exposureDay("ride-a")],
      closureGaps: [],
    });

    expect(deletes().outages.sql).not.toContain("signal");
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

  /**
   * PAR-820: the job ran with no deadline at all. The closure-gap statement's
   * plan collapsed into stacked nested loops and never finished over the
   * nightly 60-day scan, so the job held the queue's only slot until a deploy
   * killed it — and the server kept running the orphaned statement after that.
   */
  describe("deadlines and planner settings", () => {
    const timeoutOf = (tx: string[]): number => {
      const set = tx.find((sql) =>
        sql.startsWith("SET LOCAL statement_timeout"),
      );
      expect(set).toBeDefined();
      return Number(set!.split("=")[1]);
    };

    it("runs every read in its own transaction behind a statement and lock timeout", async () => {
      await run({ exposure: [exposureDay("ride-a")] });

      for (const sql of [
        OUTAGE_INTERVALS_SQL,
        OUTAGE_EXPOSURE_SQL,
        CLOSURE_GAP_INTERVALS_SQL,
      ]) {
        const tx = transactionOf(sql);
        // The limits come first, so they are in force when the statement runs.
        expect(tx.indexOf(sql)).toBe(tx.length - 1);
        expect(timeoutOf(tx)).toBeGreaterThanOrEqual(READ_TIMEOUT_FLOOR_MS);
        expect(tx).toContain(`SET LOCAL lock_timeout = ${LOCK_TIMEOUT_MS}`);
      }
      const scan = transactions.find((tx) =>
        tx.some((sql) => sql.includes("AS scan_start")),
      );
      expect(scan).toBeDefined();
      expect(timeoutOf(scan!)).toBeGreaterThan(0);
    });

    it("turns nested loops off for the closure-gap statement and only for it", async () => {
      await run({ exposure: [exposureDay("ride-a")] });

      expect(transactionOf(CLOSURE_GAP_INTERVALS_SQL)).toContain(
        "SET LOCAL enable_nestloop = off",
      );
      // Statements 1 and 2 have run at ~30 s and ~52 s for months on the
      // planner's own choice, which includes index nested loops into
      // queue_data; they keep it.
      for (const sql of [OUTAGE_INTERVALS_SQL, OUTAGE_EXPOSURE_SQL]) {
        expect(
          transactionOf(sql).some((s) => s.includes("enable_nestloop")),
        ).toBe(false);
      }
    });

    it("opens the write transaction with its limits before the first DELETE", async () => {
      await run({ exposure: [exposureDay("ride-a")] });

      const tx = transactions.find((t) =>
        t.some((sql) => sql.includes("DELETE FROM attraction_outages")),
      )!;
      const firstDelete = tx.findIndex((sql) => sql.includes("DELETE"));
      expect(tx.slice(0, firstDelete)).toEqual(statementLimitSql(WRITE_LIMITS));
      expect(tx).toContain(
        `SET LOCAL idle_in_transaction_session_timeout = ${WRITE_LIMITS.idleInTransactionTimeoutMs}`,
      );
    });

    it("keeps the DOWN reconstruction when the closure-gap statement times out", async () => {
      // A timeout is the failure the deadline is FOR, and it must land in the
      // swallowed branch: one night without closure gaps, not one night
      // without the reconstruction.
      await run({
        intervals: [interval("ride-a")],
        exposure: [exposureDay("ride-a")],
        closureGapsFail: true,
        closureGapsError: "canceling statement due to statement timeout",
      });

      const { outages } = deletes();
      expect(outages.sql).toContain("signal = 'down'");
      const rows = inserted.flatMap((batch) => batch.rows);
      expect(rows.filter((row) => row.signal === "down")).toHaveLength(1);
    });
  });
});

describe("reconstructionReadLimits", () => {
  const AS_OF = new Date("2026-10-09T05:00:00.000Z");
  const daysBefore = (days: number) =>
    new Date(AS_OF.getTime() - days * 24 * 60 * 60 * 1000);

  it("gives the nightly 60-day scan the ten-minute floor", () => {
    // ~11x the slowest statement measured over that scan (52 s).
    expect(reconstructionReadLimits(daysBefore(60), AS_OF)).toEqual({
      statementTimeoutMs: 10 * 60 * 1000,
      lockTimeoutMs: 30 * 1000,
    });
  });

  it("never goes below the floor for a short targeted repair", () => {
    expect(
      reconstructionReadLimits(daysBefore(1), AS_OF).statementTimeoutMs,
    ).toBe(READ_TIMEOUT_FLOOR_MS);
    // A scan start at or after asOf is one day, not zero or negative.
    expect(reconstructionReadLimits(AS_OF, AS_OF).statementTimeoutMs).toBe(
      READ_TIMEOUT_FLOOR_MS,
    );
  });

  it("grows with the scan for a staged hand-run fill", () => {
    // windowDays 400 can pin the scan to its 800-day floor.
    expect(
      reconstructionReadLimits(daysBefore(800), AS_OF).statementTimeoutMs,
    ).toBe(800 * READ_TIMEOUT_PER_SCAN_DAY_MS);
  });
});
