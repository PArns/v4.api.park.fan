import { ParkMetadataProcessor } from "./park-metadata.processor";
import { ThemeParksRateLimitError } from "../../external-apis/themeparks/themeparks.errors";
import { CALENDAR_WARMUP_PARK_PAUSE_MS } from "../services/cache-warmup.service";
import { BEST_DAYS_WINDOW } from "../../parks/services/best-days.service";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";

/**
 * The bulk schedule sync's three outcomes per park, which used to be two.
 *
 * A throttled fetch came back as an empty list, so `saveScheduleData` wrote
 * nothing, the run logged `📅 Fetched total 0 schedule entries`, and the number
 * at the end counted it beside the parks whose source genuinely publishes
 * nothing. On 2026-09-23 that hid 87 of 200 parks and La Ronde's opening hours
 * stood untouched for 17 days (PAR-480). Nothing in the suite noticed, because
 * the confusion lived on the caller's side of the client.
 */
describe("ParkMetadataProcessor — sync-schedules-only", () => {
  const park = (id: string, name: string) => ({
    id,
    name,
    dataSources: "themeparks-wiki,queue-times",
    wikiEntityId: `wiki-${id}`,
    continentSlug: "europe",
    countrySlug: "germany",
    citySlug: "bruehl",
    slug: name.toLowerCase(),
    timezone: "America/Los_Angeles",
  });
  // The park's today, not the server's: the snapshot's window starts there.
  const today = () => getCurrentDateInTimezone("America/Los_Angeles");

  const FULL = park("p-full", "Full");
  const EMPTY = park("p-empty", "Empty");
  const THROTTLED = park("p-throttled", "Throttled");

  let parksService: any;
  let themeParksClient: any;
  let revalidationService: any;
  let bestDaysService: any;
  let parkRepository: any;
  let sleep: jest.SpyInstance;
  let warnings: string[];
  let processor: ParkMetadataProcessor;

  beforeEach(() => {
    warnings = [];

    parksService = {
      // Two future days on the park the source went quiet about, so the warning
      // can say what the run kept rather than only that it got nothing.
      countFutureScheduleEntriesByPark: jest.fn().mockResolvedValue(
        new Map([
          [EMPTY.id, 2],
          [THROTTLED.id, 7],
        ]),
      ),
      saveScheduleData: jest
        .fn()
        .mockImplementation((_id: string, rows: unknown[]) => rows.length),
      fillScheduleGaps: jest.fn().mockResolvedValue(0),
      recordScheduleVersions: jest.fn().mockResolvedValue([]),
      invalidateCalendarMonthCache: jest.fn().mockResolvedValue(undefined),
    };

    themeParksClient = {
      getScheduleExtended: jest
        .fn()
        .mockImplementation((externalId: string) => {
          if (externalId === EMPTY.wikiEntityId)
            return { schedule: [], coveredMonths: [] };
          if (externalId === THROTTLED.wikiEntityId) {
            return Promise.reject(
              new ThemeParksRateLimitError("cooling down 58s", 58),
            );
          }
          return {
            schedule: [{ date: "2026-10-11" }, { date: "2026-10-12" }],
            coveredMonths: ["2026-10"],
          };
        }),
    };

    revalidationService = {
      revalidateTags: jest.fn().mockResolvedValue(undefined),
    };
    bestDaysService = {
      precomputeForPark: jest.fn((p: { slug: string }) =>
        Promise.resolve(p.slug),
      ),
    };
    parkRepository = {
      find: jest.fn().mockResolvedValue([FULL, EMPTY, THROTTLED]),
    };

    processor = new ParkMetadataProcessor(
      parksService,
      {} as any,
      themeParksClient,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      {} as any,
      revalidationService,
      { findOne: jest.fn() } as any,
      parkRepository,
      {} as any,
      {} as any,
      {} as any,
      bestDaysService,
    );

    const logger: any = (processor as any).logger;
    jest.spyOn(logger, "warn").mockImplementation((...args: unknown[]) => {
      warnings.push(String(args[0]));
    });
    jest.spyOn(logger, "log").mockImplementation(() => undefined);
    jest.spyOn(logger, "error").mockImplementation(() => undefined);
    jest.spyOn(logger, "debug").mockImplementation(() => undefined);
    jest.spyOn(logger, "verbose").mockImplementation(() => undefined);
    sleep = jest.spyOn(processor as any, "sleep").mockResolvedValue(undefined);
  });

  afterEach(() => jest.restoreAllMocks());

  it("counts a throttled park apart from one whose source published nothing", async () => {
    await processor.handleSyncSchedulesOnly({} as any);

    const summary = warnings.join("\n");
    expect(summary).toMatch(
      /Empty: the source published no opening hours .* we keep the 2 future entries/,
    );
    expect(summary).toMatch(
      /Throttled: schedule fetch throttled, not synced this run .* the 7 future entries already stored stay as they are/,
    );
  });

  it("writes nothing for a throttled park, so what it holds survives the run", async () => {
    await processor.handleSyncSchedulesOnly({} as any);

    const written = parksService.saveScheduleData.mock.calls.map(
      (c: unknown[]) => c[0],
    );
    expect(written).toEqual([FULL.id]);
  });

  it("hands the answered months on, so a withdrawn day can be retracted", async () => {
    // Without the third argument `saveScheduleData` retracts nothing at all, so
    // the fix would be dead code behind a client that reports coverage correctly
    // (PAR-538).
    await processor.handleSyncSchedulesOnly({} as any);

    const [, rows, coveredMonths] = parksService.saveScheduleData.mock.calls[0];
    expect(rows).toHaveLength(2);
    expect(coveredMonths).toEqual(["2026-10"]);
  });

  it("still runs the calendar maintenance pass for every park it asked about", async () => {
    await processor.handleSyncSchedulesOnly({} as any);

    // Gap filling is not a consequence of the fetch — "today" moves whether or
    // not the source answered, so skipping it for the quiet and the throttled
    // parks would freeze their UNKNOWN window.
    const filled = parksService.fillScheduleGaps.mock.calls.map(
      (c: unknown[]) => c[0],
    );
    expect(filled.sort()).toEqual([EMPTY.id, FULL.id, THROTTLED.id].sort());
    expect(parksService.invalidateCalendarMonthCache).toHaveBeenCalledTimes(3);
  });

  it("logs each park's schedule versions after its gap fill, not in between (PAR-1061)", async () => {
    await processor.handleSyncSchedulesOnly({} as any);

    const recorded = parksService.recordScheduleVersions.mock.calls;
    expect(Object.fromEntries(recorded)).toEqual({
      [EMPTY.id]: "themeparks",
      [FULL.id]: "themeparks",
      [THROTTLED.id]: "gap_fill",
    });
    // Last writer first: every record call follows that park's gap fill.
    for (const [parkId] of recorded) {
      const fill = parksService.fillScheduleGaps.mock.calls.findIndex(
        (c: unknown[]) => c[0] === parkId,
      );
      expect(
        parksService.fillScheduleGaps.mock.invocationCallOrder[fill],
      ).toBeLessThan(
        parksService.recordScheduleVersions.mock.invocationCallOrder[
          recorded.findIndex((c: unknown[]) => c[0] === parkId)
        ],
      );
    }
  });

  it("revalidates each park's page and calendar once, in one call", async () => {
    await processor.handleSyncSchedulesOnly({} as any);

    expect(revalidationService.revalidateTags).toHaveBeenCalledTimes(1);
    const [tags] = revalidationService.revalidateTags.mock.calls[0];
    expect(tags).toHaveLength(6);
    expect(tags).toContain("park:europe/germany/bruehl/full");
    expect(tags).toContain("park-calendar:europe/germany/bruehl/full");
  });

  it("rebuilds and rings the best-days snapshot of a park whose schedule changed, and of no other", async () => {
    // Full's sync wrote a changed day; the throttled park's gap fill did too,
    // but its snapshot fails to rebuild and keeps the old one.
    parksService.recordScheduleVersions.mockImplementation((id: string) =>
      Promise.resolve(id === EMPTY.id ? [] : [addIsoDays(today(), 10)]),
    );
    bestDaysService.precomputeForPark.mockImplementation(
      (p: { id: string; slug: string }) =>
        Promise.resolve(p.id === THROTTLED.id ? null : p.slug),
    );

    await processor.handleSyncSchedulesOnly({} as any);

    expect(
      bestDaysService.precomputeForPark.mock.calls.map(
        ([p]: [{ id: string }]) => p.id,
      ),
    ).toEqual([FULL.id, THROTTLED.id]);
    // After the months it is built from were evicted.
    expect(
      bestDaysService.precomputeForPark.mock.invocationCallOrder[0],
    ).toBeGreaterThan(
      Math.max(
        ...parksService.invalidateCalendarMonthCache.mock.invocationCallOrder,
      ),
    );
    expect(revalidationService.revalidateTags).toHaveBeenCalledTimes(1);
    const [tags] = revalidationService.revalidateTags.mock.calls[0];
    expect(tags.filter((t: string) => t.startsWith("best-days:"))).toEqual([
      "best-days:full",
    ]);
    expect(tags).toContain("park-calendar:europe/germany/bruehl/full");
  });

  it("rebuilds only a park with a changed day inside the snapshot's window", async () => {
    // The gap fill's daily placeholder sits half a year out, past the window,
    // and a corrected past day is no longer in it either; the window's last
    // day still is.
    const lastDay = addIsoDays(today(), BEST_DAYS_WINDOW);
    const changed: Record<string, string[]> = {
      [FULL.id]: [addIsoDays(today(), 182), lastDay],
      [EMPTY.id]: [addIsoDays(today(), -1)],
      [THROTTLED.id]: [addIsoDays(lastDay, 1), addIsoDays(today(), 182)],
    };
    parksService.recordScheduleVersions.mockImplementation((id: string) =>
      Promise.resolve(changed[id]),
    );

    await processor.handleSyncSchedulesOnly({} as any);

    expect(
      bestDaysService.precomputeForPark.mock.calls.map(
        ([p]: [{ id: string }]) => p.id,
      ),
    ).toEqual([FULL.id]);
    const [tags] = revalidationService.revalidateTags.mock.calls[0];
    expect(tags.filter((t: string) => t.startsWith("best-days:"))).toEqual([
      "best-days:full",
    ]);
    // The others' calendars are still rung: their months were evicted.
    expect(tags).toContain("park-calendar:europe/germany/bruehl/throttled");
    expect(sleep).not.toHaveBeenCalled();
  });

  it("keeps the calendar warmup's pause between two rebuilds", async () => {
    parksService.recordScheduleVersions.mockImplementation((id: string) =>
      Promise.resolve(id === EMPTY.id ? [] : [today()]),
    );

    await processor.handleSyncSchedulesOnly({} as any);

    expect(sleep).toHaveBeenCalledTimes(1);
    expect(sleep).toHaveBeenCalledWith(CALENDAR_WARMUP_PARK_PAUSE_MS);
    const [first, second] =
      bestDaysService.precomputeForPark.mock.invocationCallOrder;
    const paused = sleep.mock.invocationCallOrder[0];
    expect(paused).toBeGreaterThan(first);
    expect(paused).toBeLessThan(second);
  });

  it("skips a park without the Wiki source without asking the client", async () => {
    parkRepository.find.mockResolvedValue([
      { ...FULL, dataSources: "queue-times" },
    ]);

    await processor.handleSyncSchedulesOnly({} as any);

    expect(themeParksClient.getScheduleExtended).not.toHaveBeenCalled();
    expect(revalidationService.revalidateTags).not.toHaveBeenCalled();
  });
});
