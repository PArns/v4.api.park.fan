import { Test, TestingModule } from "@nestjs/testing";
import { DataSource } from "typeorm";
import { AttractionMergeService } from "./attraction-merge.service";
import { Attraction } from "../entities/attraction.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";

/**
 * 147 pairs of rows describe the same ride twice inside one park, because the
 * sync used to key on a source-scoped externalId. `ParkMergeService` only ever
 * merged ACROSS parks, so there was no way to collapse them.
 *
 * The surviving row must end up on the base slug: that is the URL in the
 * sitemap and in Google's index, and attractions have no alias table to
 * redirect from the "-2" one.
 */
describe("AttractionMergeService", () => {
  let service: AttractionMergeService;

  const manager = {
    findOne: jest.fn(),
    query: jest.fn().mockResolvedValue([]),
    delete: jest.fn().mockResolvedValue({ affected: 1 }),
    update: jest.fn().mockResolvedValue({ affected: 1 }),
  };
  const dataSource = {
    transaction: jest.fn(async (fn: any) => fn(manager)),
    query: jest.fn().mockResolvedValue([]),
  };
  const revalidationService = { revalidateTags: jest.fn() };
  const redis = {
    keys: jest.fn().mockResolvedValue([]),
    del: jest.fn(),
    pipeline: jest.fn(() => ({ del: jest.fn(), exec: jest.fn() })),
  };

  const baseRow = {
    id: "row-base",
    slug: "alice-in-wonderland",
    name: "Alice in Wonderland",
    parkId: "park-blackpool",
  };
  const suffixRow = {
    id: "row-suffix",
    slug: "alice-in-wonderland-2",
    name: "Alice in Wonderland",
    parkId: "park-blackpool",
  };

  const givenRows = (rows: Record<string, unknown>[]) =>
    manager.findOne.mockImplementation((_entity: unknown, opts: any) =>
      Promise.resolve(rows.find((r) => r.id === opts.where.id) ?? null),
    );

  beforeEach(async () => {
    jest.clearAllMocks();
    manager.query.mockResolvedValue([]);
    manager.delete.mockResolvedValue({ affected: 1 });
    manager.update.mockResolvedValue({ affected: 1 });
    dataSource.transaction.mockImplementation(async (fn: any) => fn(manager));

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AttractionMergeService,
        { provide: DataSource, useValue: dataSource },
        { provide: REDIS_CLIENT, useValue: redis },
        { provide: RevalidationService, useValue: revalidationService },
      ],
    }).compile();

    service = module.get(AttractionMergeService);
  });

  it("refuses to merge a row into itself", async () => {
    givenRows([baseRow]);

    await expect(
      service.mergeAttractions("row-base", "row-base"),
    ).rejects.toThrow(/itself/i);
  });

  it("refuses to merge across parks", async () => {
    givenRows([baseRow, { ...suffixRow, parkId: "park-other" }]);

    await expect(
      service.mergeAttractions("row-base", "row-suffix"),
    ).rejects.toThrow(/same park/i);
  });

  it("refuses when a row does not exist", async () => {
    givenRows([baseRow]);

    await expect(
      service.mergeAttractions("row-base", "row-missing"),
    ).rejects.toThrow(/not found/i);
  });

  it("deletes the losing row", async () => {
    givenRows([baseRow, suffixRow]);

    await service.mergeAttractions("row-base", "row-suffix");

    expect(manager.delete).toHaveBeenCalledWith(Attraction, "row-suffix");
  });

  it("moves the survivor onto the base slug when it held the suffixed one", async () => {
    givenRows([baseRow, suffixRow]);

    // The suffixed row carries the richer data and wins, but the base slug is
    // the indexed URL, so the survivor has to take it over.
    await service.mergeAttractions("row-suffix", "row-base");

    expect(manager.update).toHaveBeenCalledWith(
      Attraction,
      "row-suffix",
      expect.objectContaining({ slug: "alice-in-wonderland" }),
    );
  });

  it("leaves the slug alone when the survivor already holds the base", async () => {
    givenRows([baseRow, suffixRow]);

    await service.mergeAttractions("row-base", "row-suffix");

    // The update always fires now, because `lastMergedAt` has to be stamped
    // whether or not anything else changed: the downtime reconstruction reads it
    // to skip a ride whose history has just become two interleaved series. What
    // this test is about is that the update carries NOTHING ELSE.
    expect(manager.update).toHaveBeenCalledTimes(1);
    const payload = (manager.update as jest.Mock).mock.calls[0][2];
    expect(Object.keys(payload)).toEqual(["lastMergedAt"]);
    expect(payload.lastMergedAt).toBeInstanceOf(Date);
  });

  it("renames only after the loser is gone, so the unique slug index allows it", async () => {
    givenRows([baseRow, suffixRow]);
    const order: string[] = [];
    manager.delete.mockImplementation(async () => {
      order.push("delete");
      return { affected: 1 };
    });
    manager.update.mockImplementation(async () => {
      order.push("update");
      return { affected: 1 };
    });

    await service.mergeAttractions("row-suffix", "row-base");

    expect(order).toEqual(["delete", "update"]);
  });

  it("takes over metadata the survivor is missing", async () => {
    // The two sources each fill in different columns: across the 147 real
    // pairs, 33 have the queue-times id only on the suffixed row and 29 have
    // the coordinates only there. Dropping the loser without harvesting those
    // would cut live ingestion off from the surviving row.
    givenRows([
      { ...baseRow, queueTimesEntityId: null, latitude: null, longitude: null },
      {
        ...suffixRow,
        queueTimesEntityId: "12979",
        latitude: 53.7906,
        longitude: -3.0553,
        landName: "Family Rides",
      },
    ]);

    await service.mergeAttractions("row-base", "row-suffix");

    expect(manager.update).toHaveBeenCalledWith(
      Attraction,
      "row-base",
      expect.objectContaining({
        queueTimesEntityId: "12979",
        latitude: 53.7906,
        longitude: -3.0553,
        landName: "Family Rides",
      }),
    );
  });

  it("never overwrites metadata the survivor already has", async () => {
    givenRows([
      { ...baseRow, queueTimesEntityId: "111", latitude: 1, longitude: 2 },
      { ...suffixRow, queueTimesEntityId: "999", latitude: 8, longitude: 9 },
    ]);

    await service.mergeAttractions("row-base", "row-suffix");

    // Same as above: the merge stamp always travels, and nothing else may.
    const payload = (manager.update as jest.Mock).mock.calls[0][2];
    expect(Object.keys(payload)).toEqual(["lastMergedAt"]);
  });

  it("tells the frontend to drop its cached attraction pages", async () => {
    givenRows([baseRow, suffixRow]);

    await service.mergeAttractions("row-base", "row-suffix");

    expect(revalidationService.revalidateTags).toHaveBeenCalledWith(
      expect.arrayContaining(["attractions"]),
    );
  });

  it("lifts the TimescaleDB decompression limit before moving queue data", async () => {
    givenRows([baseRow, suffixRow]);

    await service.mergeAttractions("row-base", "row-suffix");

    const statements = manager.query.mock.calls.map((c) => c[0] as string);
    const lift = statements.findIndex((s) =>
      s.includes("max_tuples_decompressed_per_dml_transaction = 0"),
    );
    const queueMove = statements.findIndex((s) => s.includes("queue_data"));

    expect(lift).toBeGreaterThanOrEqual(0);
    expect(lift).toBeLessThan(queueMove);
  });
});

describe("AttractionMergeService — batch", () => {
  let service: AttractionMergeService;

  const dataSource = { query: jest.fn(), transaction: jest.fn() };
  const revalidationService = { revalidateTags: jest.fn() };
  const redis = {
    keys: jest.fn().mockResolvedValue([]),
    del: jest.fn(),
    pipeline: jest.fn(() => ({ del: jest.fn(), exec: jest.fn() })),
  };

  /** Shape returned by the detection query: one row per base/suffix pair. */
  const pairRow = (over: Record<string, unknown> = {}) => ({
    park_id: "park-1",
    park_name: "Blackpool Pleasure Beach",
    base_id: "base-1",
    base_slug: "alice-in-wonderland",
    base_name: "Alice in Wonderland",
    base_qt: "12979",
    // Cross-source, which is what a real duplicate is: a wiki row
    // beside the Queue-Times row describing the same ride.
    base_external: "11111111-1111-1111-1111-111111111111",
    base_geo: false,
    base_recent: 100,
    base_total: 4596,
    base_created: new Date("2025-12-24"),
    suffix_id: "suffix-1",
    suffix_slug: "alice-in-wonderland-2",
    suffix_name: "Alice in Wonderland",
    suffix_qt: "12979",
    suffix_external: "qt-ride-1",
    suffix_geo: true,
    suffix_recent: 50,
    suffix_total: 2450,
    suffix_created: new Date("2026-04-26"),
    ...over,
  });

  beforeEach(async () => {
    jest.clearAllMocks();
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AttractionMergeService,
        { provide: DataSource, useValue: dataSource },
        { provide: REDIS_CLIENT, useValue: redis },
        { provide: RevalidationService, useValue: revalidationService },
      ],
    }).compile();
    service = module.get(AttractionMergeService);
  });

  it("classifies a same-name pair as auto-mergeable", async () => {
    dataSource.query.mockResolvedValue([pairRow()]);

    const [pair] = await service.findDuplicatePairs();

    expect(pair.safe).toBe(true);
    expect(pair.winnerId).toBe("base-1");
    expect(pair.survivingSlug).toBe("alice-in-wonderland");
  });

  it("flags two different rides that collided on a slug for review", async () => {
    dataSource.query.mockResolvedValue([
      pairRow({
        base_name: "Main Train",
        suffix_name: "Choco Chip Creek (215)",
        base_qt: null,
        // Cross-source, which is what a real duplicate is: a wiki row
        // beside the Queue-Times row describing the same ride.
        base_external: "11111111-1111-1111-1111-111111111111",
        suffix_qt: null,
        suffix_external: "qt-ride-1",
      }),
    ]);

    const [pair] = await service.findDuplicatePairs();

    expect(pair.safe).toBe(false);
    expect(pair.reason).toMatch(/name/i);
  });

  it("merges nothing on a dry run", async () => {
    dataSource.query.mockResolvedValue([pairRow()]);
    const merge = jest.spyOn(service, "mergeAttractions");

    const report = await service.mergeDuplicates({ dryRun: true });

    expect(merge).not.toHaveBeenCalled();
    expect(report.planned).toHaveLength(1);
    expect(report.merged).toBe(0);
  });

  it("never merges a pair that needs review", async () => {
    dataSource.query.mockResolvedValue([
      pairRow({
        base_name: "Main Train",
        suffix_name: "Choco Chip Creek",
        base_qt: null,
        // Cross-source, which is what a real duplicate is: a wiki row
        // beside the Queue-Times row describing the same ride.
        base_external: "11111111-1111-1111-1111-111111111111",
        suffix_qt: null,
        suffix_external: "qt-ride-1",
      }),
    ]);
    const merge = jest
      .spyOn(service, "mergeAttractions")
      .mockResolvedValue({} as any);

    const report = await service.mergeDuplicates({ dryRun: false });

    expect(merge).not.toHaveBeenCalled();
    expect(report.skipped).toHaveLength(1);
  });

  it("merges the safe pairs and reports failures without aborting the run", async () => {
    dataSource.query.mockResolvedValue([
      pairRow(),
      pairRow({
        base_id: "base-2",
        suffix_id: "suffix-2",
        base_slug: "icon",
        suffix_slug: "icon-2",
        base_name: "ICON",
        suffix_name: "ICON",
      }),
    ]);
    const merge = jest
      .spyOn(service, "mergeAttractions")
      .mockResolvedValueOnce({} as any)
      .mockRejectedValueOnce(new Error("deadlock"));

    const report = await service.mergeDuplicates({ dryRun: false });

    expect(merge).toHaveBeenCalledTimes(2);
    expect(report.merged).toBe(1);
    expect(report.failed).toHaveLength(1);
    expect(report.failed[0].error).toMatch(/deadlock/);
  });

  it("honours a limit so a first run can be kept small", async () => {
    dataSource.query.mockResolvedValue([
      pairRow(),
      pairRow({ base_id: "base-2", suffix_id: "suffix-2" }),
      pairRow({ base_id: "base-3", suffix_id: "suffix-3" }),
    ]);
    const merge = jest
      .spyOn(service, "mergeAttractions")
      .mockResolvedValue({} as any);

    await service.mergeDuplicates({ dryRun: false, limit: 2 });

    expect(merge).toHaveBeenCalledTimes(2);
  });
});

/**
 * The detector finds duplicate pairs two ways — a base slug beside a numbered
 * one, and two rows sharing a Queue-Times id — and many pairs satisfy both.
 * A UNION only removes exactly equal tuples, so without a canonical order the
 * same two rows arrive twice with their roles swapped: 63 rows for 53 real
 * pairs, and every mirrored twin would be merged a second time against a row
 * the first merge had already deleted.
 */
describe("AttractionMergeService — pair ordering", () => {
  const findPairsSql = async (): Promise<string> => {
    const query = jest.fn().mockResolvedValue([]);
    const service = new AttractionMergeService(
      { query } as never,
      {} as never,
      {} as never,
    );
    await service.findDuplicatePairs();
    return query.mock.calls[0][0] as string;
  };

  it("orders both branches canonically so the UNION can dedupe", async () => {
    const sql = await findPairsSql();

    // Both CTEs must key on ids, not on which row happens to hold the base
    // slug — which row is "base" carries no meaning, since
    // chooseDuplicateWinner decides the survivor and is symmetric.
    expect(sql.match(/LEAST\([^)]*\) AS base_id/g) ?? []).toHaveLength(2);
    expect(sql.match(/GREATEST\([^)]*\) AS suffix_id/g) ?? []).toHaveLength(2);
  });

  it("never offers a retired attraction for merge", async () => {
    const sql = await findPairsSql();

    expect(sql.match(/retired_at IS NULL/g)?.length).toBeGreaterThanOrEqual(3);
  });
});

/**
 * The rehearsal that was not one.
 *
 * `/v1/admin/merge-duplicate-attractions` documents `dryRun` as defaulting to
 * true, and the admin has a "Probelauf" button beside every candidate pair.
 * The single-pair branch ignored the flag and called `mergeAttractions`, so
 * the rehearsal deleted the losing row inside a transaction with no undo and
 * then displayed what it had just destroyed as a preview. Worse, the real
 * merge button is disabled for pairs the detector marked "prüfen" and the
 * rehearsal was not — so the pairs least safe to merge were the ones most
 * likely to be rehearsed.
 */
describe("AttractionMergeService — previewMerge", () => {
  const rows = [
    {
      id: "row-base",
      slug: "alice-in-wonderland",
      name: "Alice in Wonderland",
      parkId: "park-blackpool",
      queueTimesEntityId: null,
      latitude: null,
    },
    {
      id: "row-suffix",
      slug: "alice-in-wonderland-2",
      name: "Alice in Wonderland",
      parkId: "park-blackpool",
      queueTimesEntityId: 4711,
      latitude: null,
    },
    {
      id: "row-elsewhere",
      slug: "alice-in-wonderland",
      name: "Alice in Wonderland",
      parkId: "park-efteling",
    },
  ];

  const manager = {
    findOne: jest.fn(),
    query: jest.fn().mockResolvedValue([]),
    delete: jest.fn(),
    update: jest.fn(),
  };

  /**
   * Curated ride profiles by attraction id, answered through the raw `query`
   * the dependency planner reads with. Empty by default, which is the real
   * catalog's ordinary case: very few rides carry a profile at all.
   */
  const serviceWith = (
    profiles: Record<string, Array<Record<string, unknown>>> = {},
  ) => {
    const findOne = jest.fn(({ where }: { where: { id: string } }) =>
      Promise.resolve(rows.find((row) => row.id === where.id) ?? null),
    );
    const dataSource = {
      getRepository: jest.fn(() => ({ findOne })),
      transaction: jest.fn(async (fn: (m: unknown) => unknown) => fn(manager)),
      query: jest.fn((_sql: string, params: unknown[] = []) =>
        Promise.resolve(profiles[String(params[0])] ?? []),
      ),
    };
    return {
      service: new AttractionMergeService(
        dataSource as never,
        {} as never,
        {} as never,
      ),
      dataSource,
    };
  };

  beforeEach(() => jest.clearAllMocks());

  it("reports the surviving slug without writing anything", async () => {
    const { service, dataSource } = serviceWith();

    const preview = await service.previewMerge("row-suffix", "row-base");

    expect(preview).toMatchObject({
      dryRun: true,
      survivingSlug: "alice-in-wonderland",
      removedSlug: "alice-in-wonderland",
      renamed: true,
    });
    // The whole point: no transaction was opened, so nothing was deleted.
    expect(dataSource.transaction).not.toHaveBeenCalled();
    expect(manager.delete).not.toHaveBeenCalled();
    expect(manager.update).not.toHaveBeenCalled();
  });

  it("names the columns the survivor would inherit", async () => {
    const { service } = serviceWith();

    const preview = await service.previewMerge("row-base", "row-suffix");

    expect(preview.inheritedColumns).toContain("queueTimesEntityId");
    expect(preview.renamed).toBe(false);
  });

  it("refuses the pairs the real merge refuses, and just as early", async () => {
    const { service } = serviceWith();

    await expect(service.previewMerge("row-base", "row-base")).rejects.toThrow(
      /itself/i,
    );
    await expect(
      service.previewMerge("row-base", "row-elsewhere"),
    ).rejects.toThrow(/same park/i);
    await expect(
      service.previewMerge("row-base", "row-missing"),
    ).rejects.toThrow(/not found/i);
  });

  /**
   * The half PAR-179 added. Until then the rehearsal reported a slug and a
   * handful of inherited columns and said nothing at all about the dependent
   * rows — including the one the merge destroys for good.
   */
  describe("the curated rows it would destroy", () => {
    const layout = {
      attractionId: "row-suffix",
      elements: ["lifthill", "first-drop", "vertical-loop"],
      types: ["launch-coaster"],
    };
    const stub = {
      attractionId: "row-base",
      elements: [],
      types: [],
      manufacturer_name: "Mack Rides",
    };

    it("reports nothing where nothing is lost", async () => {
      // Both the ordinary pair (no profile anywhere) and the inheriting one
      // (only the loser has a profile, so it simply moves across). A preview
      // that cried wolf on the second would be worse than silent: the button
      // beside it is the one the admin is deciding about.
      const { service } = serviceWith();
      expect(
        (await service.previewMerge("row-base", "row-suffix")).droppedCurations,
      ).toEqual([]);

      const inheriting = serviceWith({ "row-suffix": [layout] }).service;
      expect(
        (await inheriting.previewMerge("row-base", "row-suffix"))
          .droppedCurations,
      ).toEqual([]);
    });

    it("names the losing row, with its contents", async () => {
      const { service } = serviceWith({
        "row-base": [{ ...layout, attractionId: "row-base" }],
        "row-suffix": [{ ...stub, attractionId: "row-suffix" }],
      });

      const preview = await service.previewMerge("row-base", "row-suffix");

      expect(preview.droppedCurations).toEqual([
        {
          table: "attraction_ride_profiles",
          from: "loser",
          row: { ...stub, attractionId: "row-suffix" },
        },
      ]);
    });

    it("names the SURVIVOR's row where the loser's says more", async () => {
      // The case that makes the report worth reading, and the one a preview
      // built on "the winner keeps its own" could not express at all: under
      // PAR-179 the richer profile wins, so the row about to disappear is the
      // survivor's stub.
      const { service } = serviceWith({
        "row-base": [stub],
        "row-suffix": [layout],
      });

      const preview = await service.previewMerge("row-base", "row-suffix");

      expect(preview.droppedCurations).toEqual([
        { table: "attraction_ride_profiles", from: "winner", row: stub },
      ]);
    });

    it("still writes nothing", async () => {
      // The reason the preview exists. Reading both sides is what lets it name
      // the row, and it must not tempt anybody into resolving the collision
      // here — `previewMerge` opens no transaction and issues no statement
      // other than the SELECTs.
      const { service, dataSource } = serviceWith({
        "row-base": [stub],
        "row-suffix": [layout],
      });

      await service.previewMerge("row-base", "row-suffix");

      expect(dataSource.transaction).not.toHaveBeenCalled();
      for (const [sql] of dataSource.query.mock.calls) {
        expect(sql).toMatch(/^SELECT /);
      }
    });
  });
});

/**
 * The works period travels as a set, not column by column (PAR-297).
 *
 * `AdminCurationService` refuses an end before its start and clears "that end
 * is only an estimate" whenever the end goes away. A merge that filled the
 * three columns one at a time could build either state without going near
 * those checks — and since PAR-287 the window is served as `worksPeriod`, so
 * the result reaches a reader.
 *
 * The set is held back in both directions: when the WINNER already holds part
 * of a window, and when the LOSER's window is one the endpoint would have
 * refused to write.
 *
 * Every assertion below names the absent column AND the presence that proves
 * the merge got far enough to consider it (G-44): the pairs are built so that
 * a second, unrelated column is always inherited in the same call.
 */
describe("AttractionMergeService — the works period survives as a set", () => {
  const WINDOW = [
    "curatedOutOfServiceFrom",
    "curatedOutOfServiceTo",
    "curatedOutOfServiceToUncertain",
  ] as const;

  const ride = (overrides: Record<string, unknown> = {}) => ({
    id: "row-base",
    slug: "alice-in-wonderland",
    name: "Alice in Wonderland",
    parkId: "park-blackpool",
    queueTimesEntityId: null,
    curatedOutOfServiceFrom: null,
    curatedOutOfServiceTo: null,
    curatedOutOfServiceToUncertain: null,
    ...overrides,
  });

  const inheritedFrom = (
    winner: Record<string, unknown>,
    loser: Record<string, unknown>,
  ) => {
    const rows = [
      { ...winner, id: "row-base" },
      { ...loser, id: "row-suffix", slug: "alice-in-wonderland-2" },
    ];
    const findOne = jest.fn(({ where }: { where: { id: string } }) =>
      Promise.resolve(rows.find((row) => row.id === where.id) ?? null),
    );
    const dataSource = {
      getRepository: jest.fn(() => ({ findOne })),
      transaction: jest.fn(),
      query: jest.fn().mockResolvedValue([]),
    };
    const service = new AttractionMergeService(
      dataSource as never,
      {} as never,
      {} as never,
    );
    return service.previewMerge("row-base", "row-suffix");
  };

  it("carries the whole window when the winner holds none of it", async () => {
    const preview = await inheritedFrom(
      ride({ queueTimesEntityId: null }),
      ride({
        queueTimesEntityId: 4711,
        curatedOutOfServiceFrom: "2026-01-16",
        curatedOutOfServiceTo: "2026-03-03",
        curatedOutOfServiceToUncertain: true,
      }),
    );

    expect(preview.inheritedColumns).toEqual(
      expect.arrayContaining([...WINDOW, "queueTimesEntityId"]),
    );
  });

  it("carries an open-ended window as the one column it really has", async () => {
    // A start with no end is the ordinary state while work is going on, and
    // the flag is null beside it. Inheriting a null would write a column the
    // loser never held.
    const preview = await inheritedFrom(
      ride(),
      ride({
        queueTimesEntityId: 4711,
        curatedOutOfServiceFrom: "2026-01-16",
      }),
    );

    expect(preview.inheritedColumns).toContain("curatedOutOfServiceFrom");
    expect(preview.inheritedColumns).toContain("queueTimesEntityId");
    expect(preview.inheritedColumns).not.toContain("curatedOutOfServiceTo");
    expect(preview.inheritedColumns).not.toContain(
      "curatedOutOfServiceToUncertain",
    );
  });

  it("never lets an end arrive beside a start it precedes", async () => {
    // Column by column, this is the pair that used to be built: the winner
    // keeps its March start, takes the loser's January end, and serves a
    // window that ends before it begins.
    const preview = await inheritedFrom(
      ride({ curatedOutOfServiceFrom: "2026-03-01" }),
      ride({
        queueTimesEntityId: 4711,
        curatedOutOfServiceFrom: "2026-01-02",
        curatedOutOfServiceTo: "2026-01-20",
      }),
    );

    // The merge did run and did inherit — it just left the window alone.
    expect(preview.inheritedColumns).toContain("queueTimesEntityId");
    for (const column of WINDOW) {
      expect(preview.inheritedColumns).not.toContain(column);
    }
  });

  it("never lets the estimate flag arrive without its date", async () => {
    // The winner holds a closed window, the loser a bare `toUncertain`.
    // Inheriting it alone hedges a date that was just confirmed.
    const preview = await inheritedFrom(
      ride({
        curatedOutOfServiceFrom: "2026-03-01",
        curatedOutOfServiceTo: "2026-03-30",
      }),
      ride({
        queueTimesEntityId: 4711,
        curatedOutOfServiceToUncertain: true,
      }),
    );

    expect(preview.inheritedColumns).toContain("queueTimesEntityId");
    expect(preview.inheritedColumns).not.toContain(
      "curatedOutOfServiceToUncertain",
    );
  });

  it("refuses a losing window that is nothing but the estimate flag", async () => {
    // The winner holds none of the three, so the set is free to move — and it
    // must not, because what would arrive is an estimate flag with no date to
    // qualify. `AdminCurationService` clears exactly this state on write, so a
    // row carrying it never went through the endpoint.
    const preview = await inheritedFrom(
      ride(),
      ride({
        queueTimesEntityId: 4711,
        curatedOutOfServiceToUncertain: true,
      }),
    );

    expect(preview.inheritedColumns).toContain("queueTimesEntityId");
    for (const column of WINDOW) {
      expect(preview.inheritedColumns).not.toContain(column);
    }
  });

  it("drops a stale estimate flag rather than the dates beside it", async () => {
    // A start and an estimate flag with no end — a state the endpoint clears
    // by dropping the flag and keeping the dates. The window itself is a real
    // curation and the only copy of it, so sinking it along with the flag
    // would be the loss this whole change removes.
    const preview = await inheritedFrom(
      ride(),
      ride({
        queueTimesEntityId: 4711,
        curatedOutOfServiceFrom: "2026-01-16",
        curatedOutOfServiceToUncertain: true,
      }),
    );

    expect(preview.inheritedColumns).toContain("curatedOutOfServiceFrom");
    expect(preview.inheritedColumns).not.toContain(
      "curatedOutOfServiceToUncertain",
    );
  });

  it("refuses a losing window that ends before it begins", async () => {
    // Unreachable through the endpoint — `AdminCurationService` rejects the
    // pair — but reachable by hand, and the winner holds nothing, so nothing
    // else would stop it. Arriving whole does not make an inverted window any
    // better: `isCuratedOutOfService` tests `from <= day <= to`, which no day
    // satisfies, so it suppresses nothing and is served to readers as a
    // `worksPeriod` running from March to January — on a row that outlives the
    // merge.
    const preview = await inheritedFrom(
      ride(),
      ride({
        queueTimesEntityId: 4711,
        curatedOutOfServiceFrom: "2026-03-01",
        curatedOutOfServiceTo: "2026-01-20",
      }),
    );

    expect(preview.inheritedColumns).toContain("queueTimesEntityId");
    for (const column of WINDOW) {
      expect(preview.inheritedColumns).not.toContain(column);
    }
  });

  it("leaves the window alone when the winner holds only the flag", async () => {
    // `toUncertain` with no dates is not a state the endpoint writes, but it
    // is a value on the winner, and the set does not half-fill around it.
    const preview = await inheritedFrom(
      ride({ curatedOutOfServiceToUncertain: false }),
      ride({
        queueTimesEntityId: 4711,
        curatedOutOfServiceFrom: "2026-01-02",
        curatedOutOfServiceTo: "2026-01-20",
      }),
    );

    expect(preview.inheritedColumns).toContain("queueTimesEntityId");
    for (const column of WINDOW) {
      expect(preview.inheritedColumns).not.toContain(column);
    }
  });
});
