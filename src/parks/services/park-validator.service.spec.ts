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

  it("does not flag two real parks that share a name", async () => {
    parkRepository.find.mockResolvedValue([disneylandParis, disneylandAnaheim]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("does not flag sibling chain parks that sit next to each other", async () => {
    parkRepository.find.mockResolvedValue([fantawildPark, fantawildWaterPark]);

    expect(await service.findDuplicates()).toEqual([]);
  });

  it("finds both real pairs and no false positives in one pass", async () => {
    parkRepository.find.mockResolvedValue([
      ushLosAngeles,
      ushBullCreek,
      ioaOrlando,
      ioaTampa,
      disneylandParis,
      disneylandAnaheim,
      fantawildPark,
      fantawildWaterPark,
    ]);

    const duplicates = await service.findDuplicates();

    expect(duplicates.map(idsOf).sort()).toEqual(
      [
        [ushLosAngeles.id, ushBullCreek.id].sort(),
        [ioaOrlando.id, ioaTampa.id].sort(),
      ].sort(),
    );
  });
});
