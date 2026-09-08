import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { ShowsService } from "./shows.service";
import { Show } from "./entities/show.entity";
import { ShowLiveData } from "./entities/show-live-data.entity";
import { ShowSchedulePattern } from "./entities/show-schedule-pattern.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { ParksService } from "../parks/parks.service";
import {
  EntityLiveResponse,
  EntityType,
  LiveStatus,
} from "../external-apis/themeparks/themeparks.types";
import { generateSlug } from "../common/utils/slug.util";

/**
 * Chainable query-builder stub covering both call shapes shows.service.ts
 * uses on the ShowLiveData repository: the DISTINCT ON "latest per show"
 * path (applyLatestPerEntity) and the plain "today's OPERATING rows" path.
 */
function makeShowLiveDataQueryBuilder(rows: unknown[] = []) {
  const qb: Record<string, jest.Mock> = {};
  const chain = [
    "innerJoinAndSelect",
    "leftJoinAndSelect",
    "innerJoin",
    "where",
    "andWhere",
    "distinctOn",
    "orderBy",
    "addOrderBy",
  ];
  for (const method of chain) {
    qb[method] = jest.fn().mockReturnValue(qb);
  }
  qb.getMany = jest.fn().mockResolvedValue(rows);
  return qb;
}

describe("ShowsService", () => {
  let service: ShowsService;

  const mockShowRepository = {
    findOne: jest.fn(),
    find: jest.fn(),
    update: jest.fn().mockResolvedValue(undefined),
    save: jest.fn().mockResolvedValue(undefined),
  };

  const mockShowLiveDataRepository = {
    findOne: jest.fn(),
    create: jest.fn((x) => x),
    save: jest.fn().mockResolvedValue(undefined),
    createQueryBuilder: jest.fn(() => makeShowLiveDataQueryBuilder()),
  };

  const mockShowSchedulePatternRepository = {
    manager: { transaction: jest.fn(), query: jest.fn() },
  };

  const mockThemeParksClient = {
    getEntityChildren: jest.fn(),
  };

  const mockThemeParksMapper = {
    mapShow: jest.fn(),
  };

  const mockParksService = {
    ensureParksLoaded: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ShowsService,
        { provide: getRepositoryToken(Show), useValue: mockShowRepository },
        {
          provide: getRepositoryToken(ShowLiveData),
          useValue: mockShowLiveDataRepository,
        },
        {
          provide: getRepositoryToken(ShowSchedulePattern),
          useValue: mockShowSchedulePatternRepository,
        },
        { provide: ThemeParksClient, useValue: mockThemeParksClient },
        { provide: ThemeParksMapper, useValue: mockThemeParksMapper },
        { provide: ParksService, useValue: mockParksService },
      ],
    }).compile();

    service = module.get<ShowsService>(ShowsService);

    jest.clearAllMocks();
  });

  it("should be defined", () => {
    expect(service).toBeDefined();
  });

  describe("shouldSaveShowLiveData (via saveShowLiveData)", () => {
    const baseLiveData: EntityLiveResponse = {
      id: "ext-show-1",
      name: "Test Show",
      entityType: EntityType.SHOW,
      status: LiveStatus.CLOSED,
      lastUpdated: new Date().toISOString(),
    };

    beforeEach(() => {
      // The show exists — every "should save" path reaches the create/save call.
      mockShowRepository.findOne.mockResolvedValue({ id: "show-1" });
    });

    it("saves when no previous live data exists", async () => {
      mockShowLiveDataRepository.findOne.mockResolvedValue(null);

      const result = await service.saveShowLiveData("show-1", baseLiveData);

      expect(result).toBe(1);
      expect(mockShowLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("saves when the status changed", async () => {
      mockShowLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.CLOSED,
        showtimes: null,
        operatingHours: null,
        timestamp: new Date(),
        show: { park: { timezone: "UTC" } },
      });

      const result = await service.saveShowLiveData("show-1", {
        ...baseLiveData,
        status: LiveStatus.OPERATING,
        // Empty showtimes keeps this test isolated from the
        // stale-showtime-projection branch, which only runs when there is
        // at least one entry.
        showtimes: [],
      });

      expect(result).toBe(1);
      expect(mockShowLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("saves when the showtimes array changed", async () => {
      mockShowLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.CLOSED,
        showtimes: [
          {
            type: "Performance",
            startTime: "2025-01-01T10:00:00Z",
            endTime: "2025-01-01T10:30:00Z",
          },
        ],
        operatingHours: null,
        timestamp: new Date(),
        show: { park: { timezone: "UTC" } },
      });

      const result = await service.saveShowLiveData("show-1", {
        ...baseLiveData,
        status: LiveStatus.CLOSED,
        showtimes: [
          {
            type: "Performance",
            startTime: "2025-01-01T11:00:00Z",
            endTime: "2025-01-01T11:30:00Z",
          },
        ],
      });

      expect(result).toBe(1);
      expect(mockShowLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("saves when the operating hours changed", async () => {
      mockShowLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.CLOSED,
        showtimes: null,
        operatingHours: [
          { type: "OPERATING", startTime: "09:00", endTime: "18:00" },
        ],
        timestamp: new Date(),
        show: { park: { timezone: "UTC" } },
      });

      const result = await service.saveShowLiveData("show-1", {
        ...baseLiveData,
        status: LiveStatus.CLOSED,
        operatingHours: [
          { type: "OPERATING", startTime: "09:00", endTime: "20:00" },
        ],
      });

      expect(result).toBe(1);
      expect(mockShowLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("saves on day rollover even when nothing else changed", async () => {
      mockShowLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.CLOSED,
        showtimes: null,
        operatingHours: null,
        // 25h guarantees a different UTC calendar date than "now" regardless
        // of what time the test happens to run at.
        timestamp: new Date(Date.now() - 25 * 60 * 60 * 1000),
        show: { park: { timezone: "UTC" } },
      });

      const result = await service.saveShowLiveData("show-1", {
        ...baseLiveData,
        status: LiveStatus.CLOSED,
      });

      expect(result).toBe(1);
      expect(mockShowLiveDataRepository.save).toHaveBeenCalledTimes(1);
    });

    it("skips saving when nothing changed", async () => {
      mockShowLiveDataRepository.findOne.mockResolvedValue({
        status: LiveStatus.CLOSED,
        showtimes: null,
        operatingHours: null,
        timestamp: new Date(),
        show: { park: { timezone: "UTC" } },
      });

      const result = await service.saveShowLiveData("show-1", {
        ...baseLiveData,
        status: LiveStatus.CLOSED,
      });

      expect(result).toBe(0);
      expect(mockShowLiveDataRepository.save).not.toHaveBeenCalled();
    });
  });

  describe("findBatchCurrentStatusByShows", () => {
    it("returns null for a show whose OPERATING reading is older than 48h", async () => {
      mockShowLiveDataRepository.createQueryBuilder.mockReturnValue(
        makeShowLiveDataQueryBuilder([
          {
            showId: "show-1",
            status: LiveStatus.OPERATING,
            showtimes: [
              { type: "Performance", startTime: "2020-01-01T10:00:00Z" },
            ],
            lastUpdated: new Date(Date.now() - 49 * 60 * 60 * 1000),
            show: { park: { timezone: "UTC" } },
          },
        ]),
      );

      const result = await service.findBatchCurrentStatusByShows(["show-1"]);

      expect(result.get("show-1")).toBeNull();
    });

    it("keeps a fresh OPERATING reading and projects its showtimes to today", async () => {
      mockShowLiveDataRepository.createQueryBuilder.mockReturnValue(
        makeShowLiveDataQueryBuilder([
          {
            showId: "show-1",
            status: LiveStatus.OPERATING,
            showtimes: [
              { type: "Performance", startTime: "2020-01-01T10:00:00Z" },
            ],
            lastUpdated: new Date(),
            show: { park: { timezone: "UTC" } },
          },
        ]),
      );

      const result = await service.findBatchCurrentStatusByShows(["show-1"]);
      const entry = result.get("show-1");

      expect(entry).not.toBeNull();
      const todayUtc = new Date().toISOString().substring(0, 10);
      expect(entry!.showtimes?.[0].startTime.substring(0, 10)).toBe(todayUtc);
      // Only the date moved — the wall-clock time-of-day is preserved.
      expect(entry!.showtimes?.[0].startTime).toContain("10:00:00");
    });

    it("returns null for showIds it never queried a row for", async () => {
      mockShowLiveDataRepository.createQueryBuilder.mockReturnValue(
        makeShowLiveDataQueryBuilder([]),
      );

      const result = await service.findBatchCurrentStatusByShows([
        "show-without-data",
      ]);

      expect(result.get("show-without-data")).toBeNull();
    });
  });

  describe("findTodayOperatingDataByPark", () => {
    it("keeps only the row that falls on today in the park's timezone", async () => {
      const todayUtc = new Date();
      const yesterdayUtc = new Date(Date.now() - 24 * 60 * 60 * 1000);

      mockShowLiveDataRepository.createQueryBuilder.mockReturnValue(
        makeShowLiveDataQueryBuilder([
          // Newest first, as the real query orders it.
          { showId: "show-today", timestamp: todayUtc },
          { showId: "show-yesterday", timestamp: yesterdayUtc },
        ]),
      );

      const result = await service.findTodayOperatingDataByPark(
        "park-1",
        "UTC",
      );

      expect(result.has("show-today")).toBe(true);
      expect(result.has("show-yesterday")).toBe(false);
    });

    it("returns an empty map when nothing operated today", async () => {
      mockShowLiveDataRepository.createQueryBuilder.mockReturnValue(
        makeShowLiveDataQueryBuilder([]),
      );

      const result = await service.findTodayOperatingDataByPark(
        "park-1",
        "UTC",
      );

      expect(result.size).toBe(0);
    });
  });

  describe("syncShows", () => {
    it("upserts shows, keeps slugs unique, and only syncs ThemeParks.wiki parks", async () => {
      mockParksService.ensureParksLoaded.mockResolvedValue([
        { id: "park-1", externalId: "tp_1" },
        // Queue-Times-only park: never reaches the ThemeParks.wiki client.
        { id: "park-2", externalId: "qt-2" },
      ]);

      mockThemeParksClient.getEntityChildren.mockResolvedValue({
        children: [
          { id: "show-ext-1", name: "Existing Show", entityType: "SHOW" },
          { id: "show-ext-new-a", name: "Duplicate Show", entityType: "SHOW" },
          { id: "show-ext-new-b", name: "Duplicate Show", entityType: "SHOW" },
          // Not a show — must be filtered out before any mapping happens.
          { id: "attraction-ext", name: "Some Ride", entityType: "ATTRACTION" },
        ],
      });

      mockShowRepository.find.mockResolvedValue([
        {
          id: "existing-show-id",
          externalId: "show-ext-1",
          slug: "existing-show",
        },
      ]);

      mockThemeParksMapper.mapShow.mockImplementation(
        (apiData: { id: string; name: string }, parkId: string) => ({
          externalId: apiData.id,
          name: apiData.name,
          slug: generateSlug(apiData.name),
          parkId,
        }),
      );

      const syncedCount = await service.syncShows();

      // Wiki-only filter: the Queue-Times park is never asked for children.
      expect(mockThemeParksClient.getEntityChildren).toHaveBeenCalledTimes(1);
      expect(mockThemeParksClient.getEntityChildren).toHaveBeenCalledWith(
        "tp_1",
      );

      // Non-SHOW children never reach the mapper.
      expect(mockThemeParksMapper.mapShow).toHaveBeenCalledTimes(3);

      // Update path for the existing show.
      expect(mockShowRepository.update).toHaveBeenCalledWith(
        "existing-show-id",
        { name: "Existing Show" },
      );

      // Insert path: two shows sharing a name get distinct slugs.
      expect(mockShowRepository.save).toHaveBeenCalledWith([
        expect.objectContaining({
          externalId: "show-ext-new-a",
          slug: "duplicate-show",
        }),
        expect.objectContaining({
          externalId: "show-ext-new-b",
          slug: "duplicate-show-2",
        }),
      ]);

      expect(syncedCount).toBe(3);
    });
  });
});
