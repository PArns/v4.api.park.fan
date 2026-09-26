import { ParkMetadataProcessor } from "./park-metadata.processor";
import { ThemeParksRateLimitError } from "../../external-apis/themeparks/themeparks.errors";

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
  });

  const FULL = park("p-full", "Full");
  const EMPTY = park("p-empty", "Empty");
  const THROTTLED = park("p-throttled", "Throttled");

  let parksService: any;
  let themeParksClient: any;
  let revalidationService: any;
  let parkRepository: any;
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
      invalidateCalendarMonthCache: jest.fn().mockResolvedValue(undefined),
    };

    themeParksClient = {
      getScheduleExtended: jest
        .fn()
        .mockImplementation((externalId: string) => {
          if (externalId === EMPTY.wikiEntityId) return { schedule: [] };
          if (externalId === THROTTLED.wikiEntityId) {
            return Promise.reject(
              new ThemeParksRateLimitError("cooling down 58s", 58),
            );
          }
          return { schedule: [{ date: "2026-10-11" }, { date: "2026-10-12" }] };
        }),
    };

    revalidationService = {
      revalidateTags: jest.fn().mockResolvedValue(undefined),
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
    );

    const logger: any = (processor as any).logger;
    jest.spyOn(logger, "warn").mockImplementation((...args: unknown[]) => {
      warnings.push(String(args[0]));
    });
    jest.spyOn(logger, "log").mockImplementation(() => undefined);
    jest.spyOn(logger, "error").mockImplementation(() => undefined);
    jest.spyOn(logger, "debug").mockImplementation(() => undefined);
    jest.spyOn(logger, "verbose").mockImplementation(() => undefined);
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

  it("revalidates each park's frontend tag once, in one call", async () => {
    await processor.handleSyncSchedulesOnly({} as any);

    expect(revalidationService.revalidateTags).toHaveBeenCalledTimes(1);
    const [tags] = revalidationService.revalidateTags.mock.calls[0];
    expect(tags).toHaveLength(3);
    expect(tags).toContain("park:europe/germany/bruehl/full");
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
