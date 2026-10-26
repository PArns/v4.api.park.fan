import { ChildrenMetadataProcessor } from "./children-metadata.processor";
import { parkColumnsFollowingAttraction } from "../../parks/utils/merge-dependencies";

/**
 * A listed child whose row lives in another park of the same resort is moved
 * there by the children sync (PAR-743).
 *
 * Measured 2026-10-25: 0 of 11,864 wiki entities listed under two parks, 74
 * listed children whose row lies in a sister park (49 attractions, 17 shows,
 * 8 restaurants, 7 resorts). The move is decided after the whole run, against
 * the OTHER park's `/children` from that run, and never crosses a resort.
 */
describe("ChildrenMetadataProcessor — moving a row to the park that lists it", () => {
  const attractionRepo = {
    find: jest.fn(),
    update: jest.fn(),
    save: jest.fn(),
  };
  const showRepo = { findOne: jest.fn(), find: jest.fn(), update: jest.fn() };
  const tx = { query: jest.fn() };
  const manager = {
    query: jest.fn(),
    transaction: jest.fn(async (cb: (m: typeof tx) => unknown) => cb(tx)),
  };
  const mappingRepository = {
    findOne: jest.fn().mockResolvedValue(null),
    save: jest.fn(),
    manager,
  };
  const themeParksMapper = { mapAttraction: jest.fn(), mapShow: jest.fn() };
  const redis = { del: jest.fn(), keys: jest.fn().mockResolvedValue([]) };
  const revalidation = {
    revalidateTags: jest.fn().mockResolvedValue(undefined),
  };

  const resort = "dest-kc";
  const worldsOfFun = {
    id: "park-wof",
    name: "Worlds of Fun",
    destinationId: resort,
  };
  const oceansOfFun = {
    id: "park-oof",
    name: "Oceans of Fun",
    destinationId: resort,
  };
  const elsewhere = {
    id: "park-x",
    name: "Cedar Point",
    destinationId: "dest-cp",
  };
  const parks = [worldsOfFun, oceansOfFun, elsewhere];
  const wikiId = "aaaaaaaa-1111-4111-8111-111111111111";
  const row = { id: "row-patriot-plunge", slug: "patriot-plunge" };

  let processor: ChildrenMetadataProcessor;
  let warned: string[];
  let logged: string[];

  beforeEach(() => {
    jest.clearAllMocks();
    warned = [];
    logged = [];
    attractionRepo.find.mockResolvedValue([]);
    attractionRepo.save.mockImplementation(async (r) => ({
      ...r,
      id: "row-new",
    }));
    manager.query.mockResolvedValue([]);
    tx.query.mockImplementation(async (sql: string) => {
      if (sql.includes("FOR UPDATE")) return [{ slug: row.slug }];
      return [];
    });
    processor = new ChildrenMetadataProcessor(
      { getRepository: () => attractionRepo } as any,
      { unretire: jest.fn() } as any,
      { getRepository: () => showRepo } as any,
      { getRepository: () => showRepo } as any,
      {} as any,
      {} as any,
      themeParksMapper as any,
      {} as any,
      mappingRepository as any,
      {} as any,
      redis as any,
      revalidation as any,
    );
    jest
      .spyOn((processor as any).logger, "warn")
      .mockImplementation((m: unknown) => warned.push(String(m)));
    jest
      .spyOn((processor as any).logger, "log")
      .mockImplementation((m: unknown) => logged.push(String(m)));
  });

  const attractionMove = {
    table: "attractions" as const,
    rowId: row.id,
    externalId: wikiId,
    name: "Patriot Plunge",
    fromParkId: worldsOfFun.id,
    toParkId: oceansOfFun.id,
  };

  const apply = (moves: unknown[], listing: Record<string, string[]>) =>
    (processor as any).applyParkMoves(
      moves,
      new Map(Object.entries(listing).map(([k, v]) => [k, new Set(v)])),
      parks,
    );

  const sqlCalls = () =>
    tx.query.mock.calls.map(([sql, params]) => ({ sql, params }));

  it("hands a sister park's row to the move instead of inserting", async () => {
    themeParksMapper.mapAttraction.mockReturnValue({
      externalId: wikiId,
      name: "Patriot Plunge",
      parkId: oceansOfFun.id,
    });
    manager.query.mockResolvedValue([{ id: row.id, parkId: worldsOfFun.id }]);
    const pendingMoves: unknown[] = [];

    await (processor as any).syncAttraction({}, oceansOfFun.id, {
      claimed: new Set(),
      listedExternalIds: new Set([wikiId]),
      listedRowIds: new Set(),
      pendingMoves,
    });

    expect(attractionRepo.save).not.toHaveBeenCalled();
    expect(pendingMoves).toEqual([attractionMove]);
  });

  it("moves an attraction within the resort, with an alias for the old path", async () => {
    await apply([attractionMove], {
      [worldsOfFun.id]: ["something-else"],
      [oceansOfFun.id]: [wikiId],
    });

    const calls = sqlCalls();
    const update = calls.find((c) => c.sql.includes("UPDATE attractions"));
    expect(update?.params).toEqual([row.id, oceansOfFun.id, row.slug]);
    expect(update?.sql).toContain("last_listed_at = now()");
    const alias = calls.find((c) => c.sql.includes("attraction_slug_aliases"));
    expect(alias?.params).toEqual([row.id, row.slug]);
    // The denormalised park columns follow, e.g. the curated ride profile.
    expect(
      calls.some(
        (c) =>
          c.sql.includes("UPDATE attraction_ride_profiles") &&
          c.params[1] === oceansOfFun.id,
      ),
    ).toBe(true);
    expect(logged.some((l) => l.includes("🚚") && l.includes(row.id))).toBe(
      true,
    );
    expect(redis.del).toHaveBeenCalled();
  });

  it("carries plain park columns, but no jsonb ride list and no hypertable", () => {
    const tables = parkColumnsFollowingAttraction().map((f) => f.table);
    expect(tables).toEqual(
      expect.arrayContaining([
        "attraction_ride_profiles",
        "headliner_attractions",
        "schedule_entries",
      ]),
    );
    expect(tables).not.toContain("park_seasons");
    expect(tables).not.toContain("queue_data_aggregates");
  });

  it("never moves a row across a resort boundary", async () => {
    await apply([{ ...attractionMove, fromParkId: elsewhere.id }], {
      [elsewhere.id]: [],
      [oceansOfFun.id]: [wikiId],
    });

    expect(manager.transaction).not.toHaveBeenCalled();
  });

  it("leaves the row where it is while its own park still lists the id", async () => {
    await apply([attractionMove], {
      [worldsOfFun.id]: [wikiId],
      [oceansOfFun.id]: [wikiId],
    });

    expect(manager.transaction).not.toHaveBeenCalled();
    expect(warned.join("\n")).toContain("lists it too");
  });

  it("leaves the row where it is when its own park's listing is unknown", async () => {
    // That park's fetch failed this run: absence from a failed fetch is not
    // evidence (rules §1).
    await apply([attractionMove], { [oceansOfFun.id]: [wikiId] });

    expect(manager.transaction).not.toHaveBeenCalled();
  });

  it("takes a free slug when the target park already has it", async () => {
    // `Rulantica Vibes` exists in Rulantica already (measured 2026-10-25).
    tx.query.mockImplementation(async (sql: string) => {
      if (sql.includes("FOR UPDATE")) return [{ slug: "rulantica-vibes" }];
      if (sql.startsWith("SELECT slug")) return [{ slug: "rulantica-vibes" }];
      return [];
    });

    await apply(
      [
        {
          table: "shows",
          rowId: "show-vibes",
          externalId: wikiId,
          name: "Rulantica Vibes",
          fromParkId: worldsOfFun.id,
          toParkId: oceansOfFun.id,
        },
      ],
      { [worldsOfFun.id]: [], [oceansOfFun.id]: [wikiId] },
    );

    const update = sqlCalls().find((c) => c.sql.includes("UPDATE shows"));
    expect(update?.params).toEqual([
      "show-vibes",
      oceansOfFun.id,
      "rulantica-vibes-2",
    ]);
    // Shows have no alias table.
    expect(sqlCalls().some((c) => c.sql.includes("slug_aliases"))).toBe(false);
  });

  it("queues a show found in a sister park for the move", async () => {
    themeParksMapper.mapShow.mockReturnValue({
      externalId: wikiId,
      name: "Rulantica Vibes",
      parkId: oceansOfFun.id,
    });
    showRepo.findOne.mockResolvedValue({
      id: "show-vibes",
      parkId: worldsOfFun.id,
      retiredReason: null,
    });
    const pendingMoves: unknown[] = [];

    await (processor as any).syncShow({}, oceansOfFun.id, pendingMoves);

    expect(pendingMoves).toEqual([
      expect.objectContaining({
        table: "shows",
        rowId: "show-vibes",
        fromParkId: worldsOfFun.id,
        toParkId: oceansOfFun.id,
      }),
    ]);
  });
});
