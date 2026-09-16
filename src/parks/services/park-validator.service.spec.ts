import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParkValidatorService } from "./park-validator.service";
import { Park } from "../entities/park.entity";
import { QueueTimesClient } from "../../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";
import { determineMergeWinner } from "../utils/park-merge.util";

/**
 * findDuplicates() is the only automatic guard against the same physical
 * park existing twice. Two real duplicate pairs sat in production undetected
 * for seven months, so these tests pin the exact rows that slipped through.
 *
 * Both pairs come from one cause: a park known to two upstream sources gets
 * two rows, and the second row carries a *wrong* geocode. The old detector
 * required same-city OR <1km proximity, so the bad geo protected the ghost.
 * It also skipped any pair that shared an external entity ID — which is the
 * single strongest duplicate signal, and which both real pairs had.
 *
 * The third pair, Wet'n'Wild, is the opposite shape: every physical fact
 * agrees and the NAMES are what disagree, so it needed a branch that does not
 * ask the name first. Its five catalogue-real negative cases are the pairs
 * that came closest to it when the branch was measured against all 213 parks:
 * a resort's three parks on one geocode, a row filed 110 km from the park it
 * names, and a theme park 0.1174 km from its own water park — the last being
 * the only one that pins the radius.
 */
describe("ParkValidatorService.findDuplicates", () => {
  let service: ParkValidatorService;

  const parkRepository = { find: jest.fn(), count: jest.fn() };

  const park = (p: Partial<Park>): Park => ({ ...p }) as Park;

  /** Universal Studios Hollywood — real production rows. */
  const ushLosAngeles = park({
    id: "06acfaad-ea27-4c38-bc35-716e65842495",
    name: "Universal Studios Hollywood",
    city: "Los Angeles",
    latitude: 34.137261,
    longitude: -118.355516,
    wikiEntityId: null,
    queueTimesEntityId: "qt-park-66",
    wartezeitenEntityId: null,
  });
  const ushBullCreek = park({
    id: "ef48df35-7100-437e-885b-b5cb7f8ec39a",
    name: "Universal Studios Hollywood",
    city: "Bull Creek",
    // Placeholder geocode in central Florida for a park in California.
    latitude: 28.0,
    longitude: -81.0,
    wikiEntityId: "bc4005c5-8c7e-41d7-b349-cdddf1796427",
    queueTimesEntityId: "qt-park-66",
    wartezeitenEntityId: null,
  });

  /** Universal Islands of Adventure — real production rows. */
  const ioaOrlando = park({
    id: "a1594244-0325-46fa-b0ce-2a9ab106f433",
    name: "Universal Islands of Adventure",
    city: "Orlando",
    latitude: 28.471778,
    longitude: -81.470832,
    wikiEntityId: "267615cc-8943-4c2a-ae2c-5da728ca591f",
    queueTimesEntityId: "qt-park-64",
    wartezeitenEntityId: "universalislandsofadventure",
  });
  const ioaTampa = park({
    id: "9500e2b7-f400-45de-b7d9-9a5be161c14b",
    name: "Universal Islands of Adventure",
    city: "Tampa",
    latitude: 28.0417444,
    longitude: -82.4131981,
    wikiEntityId: null,
    queueTimesEntityId: "qt-park-97",
    wartezeitenEntityId: "universalislandsofadventure",
  });

  /** Two genuinely different parks that share a name. Must NOT be flagged. */
  const disneylandParis = park({
    id: "8355a0b7-26e9-4a90-af47-246ec143e99a",
    name: "Disneyland Park",
    city: "Paris",
    latitude: 48.870205,
    longitude: 2.779913,
    wikiEntityId: "dlp-wiki",
    queueTimesEntityId: "qt-park-4",
    wartezeitenEntityId: "disneylandpark",
  });
  const disneylandAnaheim = park({
    id: "9a906f3b-0bb2-45b6-b23c-879d0961f1a5",
    name: "Disneyland Park",
    city: "Anaheim",
    latitude: 33.8095545,
    longitude: -117.918953,
    wikiEntityId: "dlr-wiki",
    queueTimesEntityId: "qt-park-16",
    wartezeitenEntityId: null,
  });

  /**
   * Wet'n'Wild, Oxenford — real production rows, and the pair the
   * `sharedPoint` branch exists for. Same coordinates to seven decimals, same
   * city, same thirteen slides, one row per upstream source; the names score
   * 0.6923, which is under every other branch's threshold.
   */
  const wetnwildWiki = park({
    id: "0319acc9-06d5-4f41-9e9f-45ec6a8f3587",
    name: "Wet'n'Wild",
    city: "Oxenford",
    latitude: -27.9149499,
    longitude: 153.3167716,
    wikiEntityId: "ee018a72-0a0a-4a0a-8a0a-0a0a0a0a0a0a",
    queueTimesEntityId: null,
    wartezeitenEntityId: null,
  });
  const wetnwildQueueTimes = park({
    id: "eca6de1a-b9ea-427a-ad3e-0f7c5f9b1bd4",
    name: "Wet 'n' Wild Gold Coast",
    city: "Oxenford",
    latitude: -27.9149499,
    longitude: 153.3167716,
    wikiEntityId: null,
    queueTimesEntityId: "qt-park-146",
    wartezeitenEntityId: null,
  });

  /**
   * PortAventura World's three parks, all on one resort geocode (0.0000 km
   * apart in production). Two of them are listed separately by Queue-Times,
   * which is the source saying it knows two parks here. Must NOT be flagged.
   */
  const portAventuraPark = park({
    id: "pa-1",
    name: "PortAventura Park",
    city: "Vila-seca",
    latitude: 41.0986786,
    longitude: 1.151773,
    wikiEntityId: "pa-wiki",
    queueTimesEntityId: "qt-park-19",
    wartezeitenEntityId: null,
  });
  const ferrariLand = park({
    id: "pa-2",
    name: "Ferrari Land",
    city: "Vila-seca",
    latitude: 41.0986786,
    longitude: 1.151773,
    wikiEntityId: "fl-wiki",
    queueTimesEntityId: "qt-park-277",
    wartezeitenEntityId: null,
  });
  /** The third, which Queue-Times does not list — so the sources ARE disjoint
   * and only the name keeps it out. */
  const caribeAquaticPark = park({
    id: "pa-3",
    name: "Caribe Aquatic Park",
    city: "Vila-seca",
    latitude: 41.0986786,
    longitude: 1.151773,
    wikiEntityId: null,
    queueTimesEntityId: null,
    wartezeitenEntityId: "caribeaquaticpark",
  });

  /**
   * Two real parks 110 km apart, of which the Rockford row carries a Gurnee
   * geocode: 0.0424 km in production, name 0.6122, disjoint sources. The
   * closest thing in the catalogue to a false positive. Must NOT be flagged.
   */
  const hurricaneHarborChicago = park({
    id: "hh-1",
    name: "Hurricane Harbor Chicago",
    city: "Gurnee",
    latitude: 42.3706,
    longitude: -87.9361,
    wikiEntityId: "hh-chicago-wiki",
    queueTimesEntityId: null,
    wartezeitenEntityId: null,
  });
  const hurricaneHarborRockford = park({
    id: "hh-2",
    name: "Six Flags Hurricane Harbor, Rockford",
    city: "Gurnee",
    latitude: 42.370244,
    longitude: -87.935916,
    wikiEntityId: null,
    queueTimesEntityId: "qt-park-297",
    wartezeitenEntityId: null,
  });

  /**
   * A theme park and its own water park, 0.1174 km apart on the catalogue's
   * real coordinates and 0.6923 on names — the same score as the Wet'n'Wild
   * pair, which is the whole point. Everything `sharedPoint` asks for is
   * satisfied here EXCEPT the radius, and the sources are deliberately set
   * disjoint so that nothing else can do the refusing.
   *
   * This is what pins `SHARED_POINT_KM`. It is the nearest pair in the
   * catalogue that the radius has to separate, so widening the radius past
   * 0.1174 km merges a theme park into its water park.
   */
  const boonieBearsAdventure = park({
    id: "bb-1",
    name: "Boonie Bears Adventure Park Linhai",
    city: "Tai Zhou Shi",
    latitude: 28.8602,
    longitude: 121.195,
    wikiEntityId: "bb-adventure-wiki",
    queueTimesEntityId: null,
    wartezeitenEntityId: null,
  });
  const boonieBearsWater = park({
    id: "bb-2",
    name: "Boonie Bears Water Park Linhai",
    city: "Tai Zhou Shi",
    latitude: 28.8601,
    longitude: 121.1962,
    wikiEntityId: null,
    queueTimesEntityId: "qt-park-bb-water",
    wartezeitenEntityId: null,
  });

  /** Sibling parks of one chain, ~0.4 km apart. Must NOT be flagged. */
  const fantawildPark = park({
    id: "fw-1",
    name: "Fantawild Park Xuzhou",
    city: "Xu Zhou Shi",
    latitude: 34.2,
    longitude: 117.2,
    wikiEntityId: null,
    queueTimesEntityId: "qt-park-500",
    wartezeitenEntityId: null,
  });
  const fantawildWaterPark = park({
    id: "fw-2",
    name: "Fantawild Water Park Xuzhou",
    city: "Xu Zhou Shi",
    latitude: 34.2032,
    longitude: 117.2015,
    wikiEntityId: null,
    queueTimesEntityId: "qt-park-501",
    wartezeitenEntityId: null,
  });

  const idsOf = (pair: { park1: { id: string }; park2: { id: string } }) =>
    [pair.park1.id, pair.park2.id].sort();

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkValidatorService,
        { provide: getRepositoryToken(Park), useValue: parkRepository },
        { provide: QueueTimesClient, useValue: {} },
        { provide: WartezeitenClient, useValue: {} },
      ],
    }).compile();

    service = module.get(ParkValidatorService);
  });

  it("detects the USH pair despite a 3600 km geocode error", async () => {
    parkRepository.find.mockResolvedValue([ushLosAngeles, ushBullCreek]);

    const duplicates = await service.findDuplicates();

    expect(duplicates).toHaveLength(1);
    expect(idsOf(duplicates[0])).toEqual(
      [ushLosAngeles.id, ushBullCreek.id].sort(),
    );
  });

  it("detects the IOA pair despite different cities", async () => {
    parkRepository.find.mockResolvedValue([ioaOrlando, ioaTampa]);

    const duplicates = await service.findDuplicates();

    expect(duplicates).toHaveLength(1);
    expect(idsOf(duplicates[0])).toEqual([ioaOrlando.id, ioaTampa.id].sort());
  });

  it("reports a shared queue-times ID as the reason, not as grounds to skip", async () => {
    parkRepository.find.mockResolvedValue([ushLosAngeles, ushBullCreek]);

    const [pair] = await service.findDuplicates();

    expect(pair.sharedEntityIds.queueTimes).toBe(true);
    expect(pair.reason).toContain("shared queue-times ID");
  });

  it("reports sharedEntityIds only for IDs that are actually equal", async () => {
    // Both parks carry a wiki ID, but they are different IDs.
    parkRepository.find.mockResolvedValue([ioaOrlando, ioaTampa]);

    const [pair] = await service.findDuplicates();

    expect(pair.sharedEntityIds.wartezeiten).toBe(true);
    expect(pair.sharedEntityIds.queueTimes).toBe(false);
  });

  it("detects the Wet'n'Wild pair on coordinates alone, at 0.6923 on names", async () => {
    parkRepository.find.mockResolvedValue([wetnwildWiki, wetnwildQueueTimes]);

    const duplicates = await service.findDuplicates();

    expect(duplicates).toHaveLength(1);
    expect(idsOf(duplicates[0])).toEqual(
      [wetnwildWiki.id, wetnwildQueueTimes.id].sort(),
    );
    expect(duplicates[0].reason).toContain(
      "same coordinates, one park per source",
    );
    // The point of the branch: no existing threshold would have taken it.
    expect(duplicates[0].score).toBeLessThan(0.85);
  });

  /**
   * `safe` is what `autoDetect` acts on, so these three pin which evidence
   * buys an unattended deletion. The two real production duplicates do; the
   * pair that rests on geometry does not.
   */
  it("marks the two pairs with a shared upstream id safe to merge unattended", async () => {
    parkRepository.find.mockResolvedValue([
      ushLosAngeles,
      ushBullCreek,
      ioaOrlando,
      ioaTampa,
    ]);

    const duplicates = await service.findDuplicates();

    expect(duplicates).toHaveLength(2);
    for (const pair of duplicates) {
      expect(pair.safe).toBe(true);
      expect(pair.reviewReason).toBeNull();
    }
  });

  it("never marks a sharedPoint pair safe, because it cannot carry a shared id", async () => {
    parkRepository.find.mockResolvedValue([wetnwildWiki, wetnwildQueueTimes]);

    const [pair] = await service.findDuplicates();

    // Not a special case for this branch: `sharedPoint` requires
    // `sourcesDisjoint`, and a shared id value means both rows carry that
    // source, so the two can never hold at once.
    expect(pair.sharedEntityIds).toEqual({
      wiki: false,
      queueTimes: false,
      wartezeiten: false,
    });
    expect(pair.safe).toBe(false);
    expect(pair.reviewReason).toContain("no upstream source holds one id");
  });

  it("refuses a shared id whose names do not agree — the second venue at one address", async () => {
    // A water park beside the theme park it is named after, close enough on
    // the name to be detected (0.8621, same city) and carrying the theme
    // park's wartezeiten id, which is what `source-id-inheritance` hands down
    // a resort. That combination is the dangerous one the issue names —
    // Legoland Windsor against its water park is the same shape — and the id
    // on its own would have deleted it.
    const waterPark = park({
      id: "ioa-water",
      name: "Universal Islands of Adventure Water Park",
      city: "Orlando",
      latitude: 28.4705,
      longitude: -81.4702,
      wikiEntityId: null,
      queueTimesEntityId: null,
      wartezeitenEntityId: ioaOrlando.wartezeitenEntityId,
    });
    parkRepository.find.mockResolvedValue([ioaOrlando, waterPark]);

    const [pair] = await service.findDuplicates();

    expect(pair).toBeDefined();
    expect(pair.sharedEntityIds.wartezeiten).toBe(true);
    expect(pair.score).toBeGreaterThanOrEqual(0.85);
    expect(pair.score).toBeLessThan(0.95);
    expect(pair.safe).toBe(false);
    expect(pair.reviewReason).toContain("names score");
  });

  it("still needs the name to say something — a suffix, not a different park", async () => {
    // Same two rows, but the Queue-Times row is renamed to a park that merely
    // shares the address. Geography and sources are untouched.
    parkRepository.find.mockResolvedValue([
      wetnwildWiki,
      park({ ...wetnwildQueueTimes, name: "Paradise Country" }),
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("pins the name floor above 0.6061, which is what the raise was for", async () => {
    // `Wet 'n' Wild Las Vegas` against the Gold Coast row is 0.6061 — a real
    // park of the same brand, and the figure the floor was raised past. It
    // shares this pair's point and its disjoint sources, so the NAME is the
    // only thing refusing it.
    //
    // Reach, measured, stated like the radius case below: red at a floor of
    // 0.60, green from 0.61 up. It pins the floor above 0.6061 and not at
    // 0.65 exactly — the catalogue offers nothing between those two figures
    // to pin it with.
    parkRepository.find.mockResolvedValue([
      park({ ...wetnwildWiki, name: "Wet 'n' Wild Las Vegas" }),
      wetnwildQueueTimes,
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("pins the floor from below too — 0.5625 on one point is still refused", async () => {
    // Wet'n'Wild Sydney, another park of the same brand, at 0.5625. The Las
    // Vegas case above dominates this one — anything that makes this red makes
    // that red first — so it pins nothing on its own today. It is kept as the
    // lower anchor of the brand-sibling range. (With no case in this range at
    // all, the suite stayed green with the floor dropped as far as 0.25, which
    // is why the first of the two was written.)
    parkRepository.find.mockResolvedValue([
      park({ ...wetnwildWiki, name: "Wet 'n' Wild Sydney" }),
      wetnwildQueueTimes,
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("does not treat two failed geocodes as one point", async () => {
    // 0,0 is Null Island: rows whose geocoding failed, not neighbours. They
    // are 0.0000 km apart and would otherwise clear SHARED_POINT_KM on no
    // location information at all.
    parkRepository.find.mockResolvedValue([
      park({ ...wetnwildWiki, latitude: 0, longitude: 0 }),
      park({ ...wetnwildQueueTimes, latitude: 0, longitude: 0 }),
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("still finds a ghost pair on Null Island through the name-led branches", async () => {
    // The case above refuses two failed geocodes the GEOMETRY would have
    // joined. This one is the other half of that claim, and the docstring of
    // `usableCoordinate` makes it: refusing Null Island must not take real
    // ghost detection with it. Same city, 1.0 on names, both unlocated — the
    // `sameCity && nameSimilarity >= 0.85` branch does not ask about geometry
    // and must still fire. Hoisting the Null Island refusal into a `continue`
    // over the whole pair loop would turn this red.
    parkRepository.find.mockResolvedValue([
      park({
        id: "ghost-1",
        name: "Phantom Park",
        city: "Springfield",
        latitude: 0,
        longitude: 0,
        wikiEntityId: "ghost-wiki",
        queueTimesEntityId: null,
        wartezeitenEntityId: null,
      }),
      park({
        id: "ghost-2",
        name: "Phantom Park",
        city: "Springfield",
        latitude: 0,
        longitude: 0,
        wikiEntityId: null,
        queueTimesEntityId: "qt-park-ghost",
        wartezeitenEntityId: null,
      }),
    ]);

    const duplicates = await service.findDuplicates();

    expect(duplicates).toHaveLength(1);
    expect(duplicates[0].reason).toContain("same city");
  });

  it("compares a park on the prime meridian instead of calling it unlocated", async () => {
    // Longitude 0 is a real position; `p.longitude && …` read it as a missing
    // one and dropped the pair out of every geographic test. No catalogue park
    // sits there today, which is exactly why this needs a case of its own.
    parkRepository.find.mockResolvedValue([
      park({ ...wetnwildWiki, latitude: 51.4779, longitude: 0 }),
      park({ ...wetnwildQueueTimes, latitude: 51.4779, longitude: 0 }),
    ]);

    expect(await service.findDuplicates()).toHaveLength(1);
  });

  it("reads the coordinates Postgres actually returns for a decimal column", async () => {
    // TypeORM hands `decimal` back as a string. Everything above feeds
    // numbers, so without this the branch is only ever tested in a shape
    // production does not use.
    parkRepository.find.mockResolvedValue([
      park({
        ...wetnwildWiki,
        latitude: "-27.9149499" as unknown as number,
        longitude: "153.3167716" as unknown as number,
      }),
      park({
        ...wetnwildQueueTimes,
        latitude: "-27.9149499" as unknown as number,
        longitude: "153.3167716" as unknown as number,
      }),
    ]);

    const duplicates = await service.findDuplicates();

    expect(duplicates).toHaveLength(1);
    expect(duplicates[0].reason).toContain(
      "same coordinates, one park per source",
    );
  });

  it("does not flag PortAventura World's three parks on one resort geocode", async () => {
    parkRepository.find.mockResolvedValue([
      portAventuraPark,
      ferrariLand,
      caribeAquaticPark,
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("does not flag two parks that one source lists separately", async () => {
    // Identical coordinates AND a name score over the floor. Only the
    // Queue-Times IDs on both rows keep them apart, which is that source
    // saying it knows two parks here.
    parkRepository.find.mockResolvedValue([
      park({ ...wetnwildWiki, queueTimesEntityId: "qt-park-999" }),
      wetnwildQueueTimes,
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("pins the radius: a park and its own water park 0.1174 km apart stay two parks", async () => {
    // 0.6923 on names — the target pair's own score, so the floor cannot
    // refuse this one — disjoint sources, same city. Only SHARED_POINT_KM
    // stands between them, and it is the only case in the suite that says so.
    //
    // Before the floor moved to 0.65 the Rockford pair did this job by
    // accident, because 0.6122 sat above a floor of 0.6. It no longer does,
    // and widening the radius stopped being caught by anything.
    //
    // How far this reaches, measured: red at 0.2 km and at 1.0 km, still
    // green at 0.05. That is not a hole in the case, it is the catalogue —
    // above the 0.65 floor nothing sits between the target pair's 0.0000 km
    // and this pair's 0.1174 km, so widening to 0.05 produces no false
    // positive to catch. A fixture placed in that gap would pin the digit
    // rather than the risk.
    parkRepository.find.mockResolvedValue([
      boonieBearsAdventure,
      boonieBearsWater,
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("does not flag the Rockford row that carries a Gurnee geocode", async () => {
    // Two real parks 110 km apart: 0.0424 km apart on the geocode the Rockford
    // row wrongly carries, 0.6122 on names, disjoint sources.
    //
    // Since the floor moved to 0.65 this pair fails BOTH conditions, so no
    // single-constant change makes it red — that is the point of the raise
    // rather than a gap in the case. It is kept because it is the real pair
    // the radius was chosen against, and because a fix that reintroduced the
    // 0.6 floor would have to get past it.
    parkRepository.find.mockResolvedValue([
      hurricaneHarborChicago,
      hurricaneHarborRockford,
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("does not flag two rows on one point when neither names a source", async () => {
    parkRepository.find.mockResolvedValue([
      park({ ...wetnwildWiki, wikiEntityId: null }),
      park({ ...wetnwildQueueTimes, queueTimesEntityId: null }),
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("refuses a shared wiki ID on one point, not just a shared queue-times ID", async () => {
    // `sourcesDisjoint` has one clause per source, and only the queue-times
    // clause was covered — by "does not flag two parks that one source lists
    // separately", not by the PortAventura fixture, whose names sit far under
    // the floor and which therefore pins none of the three clauses. Drop
    // `!(p1.wikiEntityId && p2.wikiEntityId)` and nothing else in the suite
    // notices — yet that is the clause that has to hold when ThemeParks.wiki
    // itself lists two rows here, which is the source this pair's winner comes
    // from.
    parkRepository.find.mockResolvedValue([
      wetnwildWiki,
      park({ ...wetnwildQueueTimes, wikiEntityId: "another-wiki-id" }),
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("refuses a shared wartezeiten ID on one point", async () => {
    // The third clause, for the same reason as the second.
    parkRepository.find.mockResolvedValue([
      park({ ...wetnwildWiki, wartezeitenEntityId: "wz-a" }),
      park({ ...wetnwildQueueTimes, wartezeitenEntityId: "wz-b" }),
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("needs a source on EACH side, not a source on either", async () => {
    // Only the wiki row is claimed by an upstream; the other names no source
    // at all. "Two sources agree on this point" is then not what happened, and
    // the disjointness test is satisfied vacuously. Turning the branch's
    // `namesASource(p1) && namesASource(p2)` into `||` leaves the case above
    // green and only this one red.
    parkRepository.find.mockResolvedValue([
      wetnwildWiki,
      park({
        ...wetnwildQueueTimes,
        wikiEntityId: null,
        queueTimesEntityId: null,
        wartezeitenEntityId: null,
      }),
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("does not flag two rows on one point when one row has no coordinates", async () => {
    parkRepository.find.mockResolvedValue([
      park({
        ...wetnwildWiki,
        // Nullable columns, typed non-null on the entity: this is what
        // Postgres hands back for a park that was never geocoded.
        latitude: null as unknown as number,
        longitude: null as unknown as number,
      }),
      wetnwildQueueTimes,
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("does not flag two real parks that share a name", async () => {
    parkRepository.find.mockResolvedValue([disneylandParis, disneylandAnaheim]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("does not flag sibling chain parks that sit next to each other", async () => {
    parkRepository.find.mockResolvedValue([fantawildPark, fantawildWaterPark]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("resolves a winner for the pair through the function the endpoint uses", async () => {
    // This calls `determineMergeWinner` directly — the function
    // `GET /v1/admin/duplicate-parks` maps each pair through, and the one the
    // merge uses. It does NOT exercise the controller, so a swapped argument
    // order or a changed mapping step in `listDuplicateParks` would leave this
    // green. The row ThemeParks.wiki knows wins: it is the only one of the two
    // carrying a wiki ID.
    const verdict = determineMergeWinner(wetnwildWiki, wetnwildQueueTimes);

    expect(verdict).toEqual({
      winnerId: wetnwildWiki.id,
      loserId: wetnwildQueueTimes.id,
    });
  });

  it("finds all three real pairs and no false positives in one pass", async () => {
    parkRepository.find.mockResolvedValue([
      ushLosAngeles,
      ushBullCreek,
      ioaOrlando,
      ioaTampa,
      wetnwildWiki,
      wetnwildQueueTimes,
      disneylandParis,
      disneylandAnaheim,
      fantawildPark,
      fantawildWaterPark,
      portAventuraPark,
      ferrariLand,
      caribeAquaticPark,
      hurricaneHarborChicago,
      hurricaneHarborRockford,
    ]);

    const duplicates = await service.findDuplicates();

    expect(duplicates.map(idsOf).sort()).toEqual(
      [
        [ushLosAngeles.id, ushBullCreek.id].sort(),
        [ioaOrlando.id, ioaTampa.id].sort(),
        [wetnwildWiki.id, wetnwildQueueTimes.id].sort(),
      ].sort(),
    );
  });
});
