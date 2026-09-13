import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { ParkMergeService } from "./park-merge.service";
import { Park } from "../entities/park.entity";
import { ScheduleEntry } from "../entities/schedule-entry.entity";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
import {
  ATTRACTION_DEPENDENCIES,
  RESTAURANT_DEPENDENCIES,
  SHOW_DEPENDENCIES,
} from "../utils/merge-dependencies";

/**
 * A merge deletes the losing park, and that park's path was live and indexed:
 * the Tampa row for Universal Islands of Adventure alone accounted for an
 * empty page in six locales. Deleting it without recording where it went
 * turns every one of those URLs into a 404 instead of a redirect.
 */
describe("ParkMergeService — the loser's path", () => {
  let service: ParkMergeService;

  const inserted: Record<string, unknown>[] = [];
  const manager = {
    findOne: jest.fn(),
    query: jest.fn().mockResolvedValue([]),
    delete: jest.fn().mockResolvedValue({}),
    createQueryBuilder: jest.fn(() => ({
      update: jest.fn().mockReturnThis(),
      set: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      execute: jest.fn().mockResolvedValue({ affected: 0 }),
      insert: jest.fn().mockReturnThis(),
      into: jest.fn().mockReturnThis(),
      values: jest.fn((v: Record<string, unknown>) => {
        inserted.push(v);
        return { orIgnore: () => ({ execute: async () => ({}) }) };
      }),
    })),
  };
  const dataSource = {
    transaction: jest.fn(async (fn: any) => fn(manager)),
    query: jest.fn().mockResolvedValue([]),
  };
  const revalidation = { revalidateTags: jest.fn() };
  const redis = {
    keys: jest.fn().mockResolvedValue([]),
    del: jest.fn(),
    pipeline: jest.fn(() => ({ del: jest.fn(), exec: jest.fn() })),
  };

  const park = (over: Partial<Park>): Park =>
    ({
      continentSlug: "north-america",
      countrySlug: "united-states",
      slug: "universal-islands-of-adventure",
      ...over,
    }) as Park;

  beforeEach(async () => {
    jest.clearAllMocks();
    inserted.length = 0;
    manager.query.mockResolvedValue([]);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkMergeService,
        { provide: getRepositoryToken(Park), useValue: {} },
        { provide: getRepositoryToken(ScheduleEntry), useValue: {} },
        { provide: getRepositoryToken(ExternalEntityMapping), useValue: {} },
        { provide: DataSource, useValue: dataSource },
        { provide: REDIS_CLIENT, useValue: redis },
        { provide: RevalidationService, useValue: revalidation },
      ],
    }).compile();

    service = module.get(ParkMergeService);
  });

  it("redirects the losing park's path to the winner", async () => {
    manager.findOne.mockImplementation(async (_e: unknown, opts: any) =>
      opts.where.id === "winner"
        ? park({ id: "winner", name: "IOA", citySlug: "orlando" })
        : park({ id: "loser", name: "IOA", citySlug: "tampa" }),
    );

    await service.mergeParks("winner", "loser");

    expect(inserted).toContainEqual(
      expect.objectContaining({
        parkId: "winner",
        citySlug: "tampa",
        slug: "universal-islands-of-adventure",
      }),
    );
  });

  it("records nothing when both parks shared a path", async () => {
    manager.findOne.mockImplementation(async () =>
      park({ id: "x", name: "IOA", citySlug: "orlando" }),
    );

    await service.mergeParks("winner", "loser");

    expect(inserted.filter((i) => "citySlug" in i)).toHaveLength(0);
  });

  it("tells the frontend to rebuild even when the path did not change", async () => {
    // A merge removes a park and reparents its rides, so the geo tree, the
    // park list and the attraction pages all change — regardless of whether
    // the surviving park's own URL moved. Without this the frontend serves
    // the deleted park for up to 24h.
    manager.findOne.mockImplementation(async () =>
      park({ id: "x", name: "IOA", citySlug: "orlando" }),
    );

    await service.mergeParks("winner", "loser");

    expect(revalidation.revalidateTags).toHaveBeenCalledWith(
      expect.arrayContaining(["geo", "parks", "attractions"]),
    );
  });

  it("does not fail the merge when revalidation is unreachable", async () => {
    manager.findOne.mockImplementation(async (_e: unknown, opts: any) =>
      opts.where.id === "winner"
        ? park({ id: "winner", name: "IOA", citySlug: "orlando" })
        : park({ id: "loser", name: "IOA", citySlug: "tampa" }),
    );
    revalidation.revalidateTags.mockRejectedValueOnce(
      new Error("frontend down"),
    );

    await expect(service.mergeParks("winner", "loser")).resolves.toBeDefined();
  });
});

/**
 * A show or a restaurant collides for the same reason a ride does — both
 * tables carry a unique `(parkId, slug)` and a ghost park is the same park
 * from a second source — and `migrateEntities` resolves it the same way: keep
 * the winner's row, delete the loser's. What hangs off the losing row does not
 * survive that delete on its own. `show_live_data` and `show_follows` cascade
 * with it, `show_schedule_patterns` is left pointing at nothing, and
 * `restaurant_live_data` cascades too — inside a transaction that then reports
 * success.
 *
 * This is the merge path an admin triggers and `park-repair.service.ts` runs
 * unattended, so it is the reachable one of the three (PAR-150).
 */
describe("ParkMergeService — a colliding show or restaurant", () => {
  let service: ParkMergeService;

  const WINNER_PARK = "11111111-1111-1111-1111-111111111111";
  const LOSER_PARK = "22222222-2222-2222-2222-222222222222";
  const WINNER_SHOW = "5150c0de-0000-0000-0000-00000000000a";
  const LOSER_SHOW = "5150c0de-0000-0000-0000-00000000000b";
  const MOVING_SHOW = "5150c0de-0000-0000-0000-00000000000c";
  const WINNER_RESTAURANT = "4e57c0de-0000-0000-0000-00000000000a";
  const LOSER_RESTAURANT = "4e57c0de-0000-0000-0000-00000000000b";
  const WINNER_RIDE = "21de0000-0000-0000-0000-00000000000a";
  const LOSER_RIDE = "21de0000-0000-0000-0000-00000000000b";

  type Recorded = { sql: string; params?: unknown[] };
  const calls: Recorded[] = [];

  /** Off by default: most cases here are about a merge with no ride in it. */
  let ridesCollide = false;

  const rowsFor = (table: string, parkId: string) => {
    if (table === "attractions") {
      if (!ridesCollide) return [];
      return parkId === WINNER_PARK
        ? [{ id: WINNER_RIDE, slug: "baron-1898", name: "Baron 1898" }]
        : [{ id: LOSER_RIDE, slug: "baron-1898", name: "Baron 1898" }];
    }
    if (table === "shows") {
      return parkId === WINNER_PARK
        ? [{ id: WINNER_SHOW, slug: "raveleijn", name: "Raveleijn" }]
        : [
            { id: LOSER_SHOW, slug: "raveleijn", name: "Raveleijn" },
            { id: MOVING_SHOW, slug: "aquanura", name: "Aquanura" },
          ];
    }
    if (table === "restaurants") {
      return parkId === WINNER_PARK
        ? [
            {
              id: WINNER_RESTAURANT,
              slug: "polles-keuken",
              name: "Polles Keuken",
            },
          ]
        : [
            {
              id: LOSER_RESTAURANT,
              slug: "polles-keuken",
              name: "Polles Keuken",
            },
          ];
    }
    return [];
  };

  const manager = {
    findOne: jest.fn(),
    query: jest.fn(async (sql: string, params?: unknown[]) => {
      calls.push({ sql, params });
      const entitySelect = /^\s*SELECT id, slug, name FROM (\w+)/i.exec(sql);
      if (entitySelect) {
        return rowsFor(entitySelect[1], String(params?.[0]));
      }
      return [];
    }),
    delete: jest.fn().mockResolvedValue({}),
    createQueryBuilder: jest.fn(() => ({
      update: jest.fn().mockReturnThis(),
      set: jest.fn().mockReturnThis(),
      where: jest.fn().mockReturnThis(),
      execute: jest.fn().mockResolvedValue({ affected: 0 }),
      insert: jest.fn().mockReturnThis(),
      into: jest.fn().mockReturnThis(),
      values: jest.fn(() => ({
        orIgnore: () => ({ execute: async () => ({}) }),
      })),
    })),
  };
  const dataSource = {
    transaction: jest.fn(async (fn: any) => fn(manager)),
    query: jest.fn().mockResolvedValue([]),
  };
  const revalidation = { revalidateTags: jest.fn() };
  const redis = {
    keys: jest.fn().mockResolvedValue([]),
    del: jest.fn(),
    pipeline: jest.fn(() => ({ del: jest.fn(), exec: jest.fn() })),
  };

  /**
   * Index of the last statement carrying `id` as a bare parameter, or -1. The
   * entity DELETE passes it inside an array (`ANY($1::uuid[])`) and is found
   * by `indexOfStatement` instead, which is what lets the two be compared.
   */
  const lastTouching = (id: string) =>
    calls.reduce(
      (last, c, i) => ((c.params ?? []).includes(id) ? i : last),
      -1,
    );

  const indexOfStatement = (verb: string, table: string) =>
    calls.findIndex((c) =>
      new RegExp(`^\\s*${verb}\\s+${table}\\b`, "i").test(c.sql),
    );

  /** Dependency tables a statement carrying `id` reached. */
  const tablesTouched = (id: string) =>
    calls
      .filter((c) => (c.params ?? []).includes(id))
      .map((c) => /(?:FROM|UPDATE)\s+(\w+)/i.exec(c.sql)?.[1])
      .filter((t): t is string => Boolean(t));

  beforeEach(async () => {
    jest.clearAllMocks();
    calls.length = 0;
    ridesCollide = false;

    manager.findOne.mockImplementation(
      async (_e: unknown, opts: any) =>
        ({
          id: opts.where.id,
          name: opts.where.id === WINNER_PARK ? "Efteling" : "Efteling (ghost)",
          continentSlug: "europe",
          countrySlug: "netherlands",
          citySlug: "kaatsheuvel",
          slug: "efteling",
        }) as Park,
    );

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkMergeService,
        { provide: getRepositoryToken(Park), useValue: {} },
        { provide: getRepositoryToken(ScheduleEntry), useValue: {} },
        { provide: getRepositoryToken(ExternalEntityMapping), useValue: {} },
        { provide: DataSource, useValue: dataSource },
        { provide: REDIS_CLIENT, useValue: redis },
        { provide: RevalidationService, useValue: revalidation },
      ],
    }).compile();

    service = module.get(ParkMergeService);
  });

  it("drains every SHOW_DEPENDENCIES table before the losing show is deleted", async () => {
    await service.mergeParks(WINNER_PARK, LOSER_PARK);

    const touched = tablesTouched(LOSER_SHOW);
    for (const dep of SHOW_DEPENDENCIES) {
      expect(touched).toContain(dep.table);
    }

    // The DELETE is what the CASCADEs hang off, so every one of those
    // statements has to have run before it.
    const showDelete = indexOfStatement("DELETE FROM", "shows");
    expect(showDelete).toBeGreaterThan(-1);
    expect(lastTouching(LOSER_SHOW)).toBeGreaterThan(-1);
    expect(lastTouching(LOSER_SHOW)).toBeLessThan(showDelete);
    expect(calls[showDelete].params).toEqual([[LOSER_SHOW]]);
  });

  it("drains every RESTAURANT_DEPENDENCIES table before the losing restaurant is deleted", async () => {
    await service.mergeParks(WINNER_PARK, LOSER_PARK);

    const touched = tablesTouched(LOSER_RESTAURANT);
    for (const dep of RESTAURANT_DEPENDENCIES) {
      expect(touched).toContain(dep.table);
    }

    const restaurantDelete = indexOfStatement("DELETE FROM", "restaurants");
    expect(restaurantDelete).toBeGreaterThan(-1);
    expect(lastTouching(LOSER_RESTAURANT)).toBeGreaterThan(-1);
    expect(lastTouching(LOSER_RESTAURANT)).toBeLessThan(restaurantDelete);
    expect(calls[restaurantDelete].params).toEqual([[LOSER_RESTAURANT]]);
  });

  it("still reparents the show that did not collide, by id", async () => {
    const result = await service.mergeParks(WINNER_PARK, LOSER_PARK);

    const move = calls.find((c) =>
      /^\s*UPDATE shows SET "parkId"/i.test(c.sql),
    );
    expect(move?.params).toEqual([WINNER_PARK, [MOVING_SHOW]]);
    expect(result.migratedShows).toBe(2);
    expect(result.migratedRestaurants).toBe(1);
  });

  it("lifts the TimescaleDB decompression cap once, before anything moves, and never re-caps it", async () => {
    // `queue_data`, `show_live_data`, `restaurant_live_data` and
    // `park_occupancy` are all hypertables, and the docstring of
    // `applyMergeDependencies` puts the cap on its caller. The counter is per
    // transaction, so a reset to 100000 after one of those moves re-caps a
    // transaction that has already spent the budget, and the next hypertable
    // — `park_occupancy` in step 4, which runs whether or not anything
    // collided — aborts the whole merge.
    await service.mergeParks(WINNER_PARK, LOSER_PARK);

    const lifted = calls
      .map((c, i) => ({ i, sql: c.sql }))
      .filter(({ sql }) =>
        /max_tuples_decompressed_per_dml_transaction = 0\b/.test(sql),
      );
    expect(lifted).toHaveLength(1);
    expect(lifted[0].sql).toMatch(/^\s*SET LOCAL\b/);

    const liveDataMove = calls.findIndex(
      (c) =>
        /show_live_data/.test(c.sql) && (c.params ?? []).includes(LOSER_SHOW),
    );
    expect(liveDataMove).toBeGreaterThan(-1);
    expect(lifted[0].i).toBeLessThan(liveDataMove);

    expect(
      calls.filter((c) =>
        /max_tuples_decompressed_per_dml_transaction = \d/.test(c.sql),
      ),
    ).toHaveLength(1);
  });

  it("does not stamp last_merged_at when no ride collided — it is a column on attractions", async () => {
    await service.mergeParks(WINNER_PARK, LOSER_PARK);

    expect(
      calls.filter((c) => /UPDATE attractions SET last_merged_at/i.test(c.sql)),
    ).toHaveLength(0);
  });

  it("still stamps last_merged_at when a ride does collide", async () => {
    // The statement moved into a nested branch when the three entity types
    // were folded into one path; without this the show fixture above would be
    // just as green with the stamp dropped altogether.
    ridesCollide = true;

    await service.mergeParks(WINNER_PARK, LOSER_PARK);

    const stamp = calls.find((c) =>
      /UPDATE attractions SET last_merged_at/i.test(c.sql),
    );
    expect(stamp?.params).toEqual([WINNER_RIDE]);

    // And the ride's own dependency list ran, on the same path.
    const touched = tablesTouched(LOSER_RIDE);
    for (const dep of ATTRACTION_DEPENDENCIES) {
      expect(touched).toContain(dep.table);
    }
  });
});
