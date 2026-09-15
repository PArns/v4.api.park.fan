import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ParkValidatorService } from "./park-validator.service";
import { Park } from "../entities/park.entity";
import { QueueTimesClient } from "../../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";

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
 * ask the name first. Its four negative cases are the pairs that came closest
 * to it when the branch was measured against all 213 parks — a resort's three
 * parks on one geocode, and a row filed 110 km from the park it names.
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

  it("still needs the name to say something — a suffix, not a different park", async () => {
    // Same two rows, but the Queue-Times row is renamed to a park that merely
    // shares the address. Geography and sources are untouched.
    parkRepository.find.mockResolvedValue([
      wetnwildWiki,
      park({ ...wetnwildQueueTimes, name: "Paradise Country" }),
    ]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("pins the name floor near 0.6, not merely somewhere under 0.6923", async () => {
    // Wet'n'Wild Sydney is a different park of the same brand and scores
    // 0.5625 against the Gold Coast row — the closest realistic miss. Without
    // a case in this range the suite stays green with the floor dropped to
    // 0.25, and the constant's whole claim is that it sits just under the
    // pair it catches.
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

  it("does not flag the Rockford row that carries a Gurnee geocode", async () => {
    // 0.0424 km apart, 0.6122 on names, disjoint sources — everything the
    // Wet'n'Wild pair has except one point. Two real parks 110 km apart.
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
