import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { AttractionsService } from "./attractions.service";
import { Attraction } from "./entities/attraction.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { QueueTimesClient } from "../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../external-apis/wartezeiten/wartezeiten.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { ParksService } from "../parks/parks.service";
import { Park } from "../parks/entities/park.entity";

/**
 * Regression cover for the duplicate-attraction defect.
 *
 * Blackpool Pleasure Beach is synced from both Queue-Times and
 * ThemeParks.wiki. The Queue-Times rows were created first
 * (externalId "qt-ride-12979"); when the wiki sync ran months later it
 * looked rides up by externalId only, found nothing, and created a second
 * row for every single ride — "alice-in-wonderland-2" and 28 siblings.
 * The park detail endpoint then deduplicated at read time while the sitemap
 * did not, so Google was handed ~400 URLs that 404.
 */
describe("AttractionsService — cross-source duplicate prevention", () => {
  let service: AttractionsService;

  const attractionRepository = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    update: jest.fn(),
  };
  const themeParksClient = {
    getEntityChildren: jest.fn(),
    getEntity: jest.fn(),
  };
  const themeParksMapper = { mapAttraction: jest.fn() };
  const queueTimesClient = {
    getParks: jest.fn().mockResolvedValue([]),
    getParkQueueTimes: jest.fn().mockResolvedValue({ lands: [], rides: [] }),
  };
  const parksService = { ensureParksLoaded: jest.fn() };

  const blackpool = {
    id: "park-blackpool",
    name: "Blackpool Pleasure Beach",
    externalId: "wiki-park-uuid",
  } as Park;

  /** The row Queue-Times created in December. */
  const existingQueueTimesRow = {
    id: "row-alice",
    externalId: "qt-ride-12979",
    slug: "alice-in-wonderland",
    name: "Alice in Wonderland",
    queueTimesEntityId: "12979",
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AttractionsService,
        {
          provide: getRepositoryToken(Attraction),
          useValue: attractionRepository,
        },
        { provide: ThemeParksClient, useValue: themeParksClient },
        { provide: QueueTimesClient, useValue: queueTimesClient },
        {
          provide: WartezeitenClient,
          useValue: {
            getParks: jest.fn().mockResolvedValue([]),
            getWaitTimes: jest.fn().mockResolvedValue([]),
            getOpeningTimes: jest.fn().mockResolvedValue([]),
          },
        },
        { provide: ThemeParksMapper, useValue: themeParksMapper },
        { provide: ParksService, useValue: parksService },
      ],
    }).compile();

    service = module.get(AttractionsService);
  });

  it("updates the existing Queue-Times row instead of creating an '-2' duplicate", async () => {
    parksService.ensureParksLoaded.mockResolvedValue([blackpool]);
    themeParksClient.getEntityChildren.mockResolvedValue({
      children: [{ entityType: "ATTRACTION", id: "wiki-alice-uuid" }],
    });
    // The wiki reports the same ride under its own ID — and carries the geo
    // data the Queue-Times row lacks.
    themeParksMapper.mapAttraction.mockReturnValue({
      externalId: "59f971c2-f3fa-4ca4-9b92-6c3a097d5b61",
      name: "Alice in Wonderland",
      parkId: blackpool.id,
      latitude: 53.7906,
      longitude: -3.0553,
    });
    attractionRepository.find.mockResolvedValue([existingQueueTimesRow]);

    await service.syncAttractions();

    expect(attractionRepository.save).not.toHaveBeenCalled();
    expect(attractionRepository.update).toHaveBeenCalledWith(
      "row-alice",
      expect.objectContaining({
        name: "Alice in Wonderland",
        latitude: 53.7906,
        longitude: -3.0553,
      }),
    );
  });

  it("still creates a row for a ride the park does not have yet", async () => {
    parksService.ensureParksLoaded.mockResolvedValue([blackpool]);
    themeParksClient.getEntityChildren.mockResolvedValue({
      children: [{ entityType: "ATTRACTION", id: "wiki-valhalla-uuid" }],
    });
    themeParksMapper.mapAttraction.mockReturnValue({
      externalId: "wiki-valhalla-uuid",
      name: "Valhalla",
      parkId: blackpool.id,
    });
    attractionRepository.find.mockResolvedValue([existingQueueTimesRow]);

    await service.syncAttractions();

    expect(attractionRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({ name: "Valhalla", slug: "valhalla" }),
    );
  });

  it("does not collapse a park's five identically named facilities onto one row", async () => {
    // Wet'n'Wild genuinely has five rows called "Restroom". A name-based
    // fallback that ignores what it already matched would fold all five
    // incoming rows onto the first existing one and destroy four of them.
    parksService.ensureParksLoaded.mockResolvedValue([blackpool]);
    themeParksClient.getEntityChildren.mockResolvedValue({
      children: [
        { entityType: "ATTRACTION", id: "wiki-restroom-1" },
        { entityType: "ATTRACTION", id: "wiki-restroom-2" },
      ],
    });
    themeParksMapper.mapAttraction
      .mockReturnValueOnce({
        externalId: "wiki-restroom-1",
        name: "Restroom",
        parkId: blackpool.id,
      })
      .mockReturnValueOnce({
        externalId: "wiki-restroom-2",
        name: "Restroom",
        parkId: blackpool.id,
      });
    attractionRepository.find.mockResolvedValue([
      {
        id: "row-restroom-a",
        externalId: "qt-ride-1",
        slug: "restroom",
        name: "Restroom",
        queueTimesEntityId: "1",
      },
    ]);

    await service.syncAttractions();

    // First incoming row claims the existing one; the second must become its
    // own row rather than overwriting the first again.
    expect(attractionRepository.update).toHaveBeenCalledTimes(1);
    expect(attractionRepository.save).toHaveBeenCalledWith(
      expect.objectContaining({ name: "Restroom", slug: "restroom-2" }),
    );
  });

  /**
   * A park is read from exactly one source. The Queue-Times and Wartezeiten
   * branches sit outside the shared ThemeParks.wiki template and claim their
   * park before it: without that, a `qt-` park would fall through to the wiki
   * client, which has no idea what a Queue-Times ID is.
   */
  it("sends Queue-Times and Wartezeiten parks to their own sync, never to the wiki", async () => {
    const syncFromQueueTimes = jest
      .spyOn(
        service as unknown as {
          syncFromQueueTimes: (park: Park, qtId: number) => Promise<void>;
        },
        "syncFromQueueTimes",
      )
      .mockResolvedValue(undefined);
    const syncFromWartezeiten = jest
      .spyOn(
        service as unknown as {
          syncFromWartezeiten: (park: Park, wzId: string) => Promise<void>;
        },
        "syncFromWartezeiten",
      )
      .mockResolvedValue(undefined);

    parksService.ensureParksLoaded.mockResolvedValue([
      { id: "park-qt", externalId: "qt-56" } as Park,
      { id: "park-wz", externalId: "wz-phantasialand" } as Park,
      // A "qt-" ID with nothing numeric in it: still a Queue-Times park, so it
      // is dropped rather than handed to the wiki, and it counts for nothing.
      { id: "park-qt-broken", externalId: "qt-abc" } as Park,
    ]);

    const syncedCount = await service.syncAttractions();

    expect(themeParksClient.getEntityChildren).not.toHaveBeenCalled();
    expect(syncFromQueueTimes).toHaveBeenCalledTimes(1);
    expect(syncFromQueueTimes).toHaveBeenCalledWith(
      expect.objectContaining({ id: "park-qt" }),
      56,
    );
    expect(syncFromWartezeiten).toHaveBeenCalledTimes(1);
    expect(syncFromWartezeiten).toHaveBeenCalledWith(
      expect.objectContaining({ id: "park-wz" }),
      "phantasialand",
    );
    expect(syncedCount).toBe(2);
  });
});
