import { HolidaysProcessor } from "./holidays.processor";

describe("HolidaysProcessor", () => {
  let processor: HolidaysProcessor;

  const holidaysService = {
    saveHolidaysFromApi: jest.fn(),
    saveLongWeekendsFromApi: jest.fn(),
    saveSchoolHolidaysFromApi: jest.fn(),
    synthesizeProjectedSummerHolidays: jest.fn(),
    saveRawHolidays: jest.fn(),
    deleteOldHolidays: jest.fn(),
    logSchoolCoverageGaps: jest.fn(),
    replaceHolidaysBySource: jest.fn(),
  };
  const nagerDateClient = {
    getHolidaysForYears: jest.fn(),
    getLongWeekends: jest.fn(),
  };
  const openHolidaysClient = { getSchoolHolidays: jest.fn() };
  const parksService = {
    getSyncCountryCodes: jest.fn(),
    getSyncRegionCodes: jest.fn(),
  };
  const parkMetadataQueue = { add: jest.fn() };
  const neisClient = {
    enabled: false,
    getElementarySchools: jest.fn(),
    getSchedule: jest.fn(),
  };
  const kasiClient = { enabled: false, getPublicHolidays: jest.fn() };

  const run = () => processor.handleSyncHolidays({} as any);

  beforeEach(() => {
    jest.resetAllMocks();
    jest.useFakeTimers().setSystemTime(new Date("2026-06-15T12:00:00Z"));
    holidaysService.saveHolidaysFromApi.mockResolvedValue(0);
    holidaysService.saveLongWeekendsFromApi.mockResolvedValue(0);
    holidaysService.saveSchoolHolidaysFromApi.mockResolvedValue(0);
    holidaysService.synthesizeProjectedSummerHolidays.mockResolvedValue(0);
    holidaysService.saveRawHolidays.mockResolvedValue(undefined);
    holidaysService.deleteOldHolidays.mockResolvedValue(0);
    holidaysService.logSchoolCoverageGaps.mockResolvedValue([]);
    nagerDateClient.getHolidaysForYears.mockResolvedValue([]);
    nagerDateClient.getLongWeekends.mockResolvedValue([]);
    openHolidaysClient.getSchoolHolidays.mockResolvedValue([]);
    parkMetadataQueue.add.mockResolvedValue(undefined);
    holidaysService.replaceHolidaysBySource.mockImplementation(
      async (_p: string, rows: unknown[]) => ({
        saved: rows.length,
        removed: 0,
      }),
    );
    neisClient.enabled = false;
    kasiClient.enabled = false;

    processor = new HolidaysProcessor(
      holidaysService as any,
      nagerDateClient as any,
      openHolidaysClient as any,
      parksService as any,
      parkMetadataQueue as any,
      neisClient as any,
      kasiClient as any,
    );
    for (const m of ["log", "warn", "error"] as const) {
      jest.spyOn((processor as any).logger, m).mockImplementation();
    }
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("reports the synced countries' school-holiday coverage, and survives its failure (PAR-1049)", async () => {
    parksService.getSyncCountryCodes.mockResolvedValue(["ES", "SE"]);
    holidaysService.logSchoolCoverageGaps.mockRejectedValue(new Error("db"));

    await run();

    expect(holidaysService.logSchoolCoverageGaps).toHaveBeenCalledWith(
      ["ES", "SE"],
      "2026-06-15",
    );
    expect(parkMetadataQueue.add).toHaveBeenCalled();
  });

  it("does nothing when no country is relevant", async () => {
    parksService.getSyncCountryCodes.mockResolvedValue([]);

    await run();

    expect(nagerDateClient.getHolidaysForYears).not.toHaveBeenCalled();
    expect(holidaysService.saveRawHolidays).not.toHaveBeenCalled();
    expect(parkMetadataQueue.add).not.toHaveBeenCalled();
  });

  it("fetches and saves public holidays per country for last year to two years ahead", async () => {
    const de = [{ date: "2026-12-25" }];
    const us = [{ date: "2026-07-04" }];
    parksService.getSyncCountryCodes.mockResolvedValue(["DE", "US"]);
    nagerDateClient.getHolidaysForYears.mockImplementation((c: string) =>
      Promise.resolve(c === "DE" ? de : us),
    );

    await run();

    expect(nagerDateClient.getHolidaysForYears).toHaveBeenCalledWith(
      "DE",
      2025,
      2028,
    );
    expect(nagerDateClient.getHolidaysForYears).toHaveBeenCalledWith(
      "US",
      2025,
      2028,
    );
    expect(holidaysService.saveHolidaysFromApi).toHaveBeenCalledWith(de, "DE");
    expect(holidaysService.saveHolidaysFromApi).toHaveBeenCalledWith(us, "US");
  });

  it("fetches long weekends and school holidays for every year of the window", async () => {
    parksService.getSyncCountryCodes.mockResolvedValue(["DE"]);

    await run();

    const years = [2025, 2026, 2027, 2028];
    expect(nagerDateClient.getLongWeekends.mock.calls).toEqual(
      years.map((y) => [y, "DE"]),
    );
    expect(openHolidaysClient.getSchoolHolidays.mock.calls).toEqual(
      years.map((y) => ["DE", `${y}-01-01`, `${y}-12-31`]),
    );
    expect(
      holidaysService.synthesizeProjectedSummerHolidays,
    ).toHaveBeenCalledWith("DE");
  });

  it("keeps going with the next country when Nager.Date fails for one", async () => {
    parksService.getSyncCountryCodes.mockResolvedValue(["DE", "FR"]);
    nagerDateClient.getHolidaysForYears.mockImplementation((c: string) =>
      c === "DE" ? Promise.reject(new Error("down")) : Promise.resolve([]),
    );

    await run();

    expect(holidaysService.saveHolidaysFromApi).toHaveBeenCalledTimes(1);
    expect(holidaysService.saveHolidaysFromApi).toHaveBeenCalledWith([], "FR");
    // the failed country still gets its school holidays
    expect(openHolidaysClient.getSchoolHolidays).toHaveBeenCalledWith(
      "DE",
      "2026-01-01",
      "2026-12-31",
    );
    expect(parkMetadataQueue.add).toHaveBeenCalled();
  });

  it("keeps going with the next country when OpenHolidays fails for one", async () => {
    parksService.getSyncCountryCodes.mockResolvedValue(["DE", "FR"]);
    openHolidaysClient.getSchoolHolidays.mockImplementation((c: string) =>
      c === "DE" ? Promise.reject(new Error("down")) : Promise.resolve([]),
    );

    await run();

    expect(holidaysService.saveSchoolHolidaysFromApi).toHaveBeenCalledWith(
      [],
      "FR",
    );
    expect(parkMetadataQueue.add).toHaveBeenCalled();
  });

  it("survives a failing summer-holiday synthesis", async () => {
    parksService.getSyncCountryCodes.mockResolvedValue(["IT", "DE"]);
    holidaysService.synthesizeProjectedSummerHolidays.mockRejectedValueOnce(
      new Error("db"),
    );

    await run();

    expect(
      holidaysService.synthesizeProjectedSummerHolidays,
    ).toHaveBeenCalledTimes(2);
    expect(parkMetadataQueue.add).toHaveBeenCalled();
  });

  it("adds Easter Sunday for the Easter countries only, four years each", async () => {
    parksService.getSyncCountryCodes.mockResolvedValue(["DE", "US", "NL"]);

    await run();

    expect(holidaysService.saveRawHolidays).toHaveBeenCalledTimes(1);
    const rows = holidaysService.saveRawHolidays.mock.calls[0][0];
    expect(rows).toHaveLength(8);
    expect(new Set(rows.map((r: any) => r.country))).toEqual(
      new Set(["DE", "NL"]),
    );
    const de2026 = rows.find(
      (r: any) => r.country === "DE" && r.date.getUTCFullYear() === 2026,
    );
    expect(de2026).toMatchObject({
      externalId: "computed:DE:2026-04-05:easter-sunday",
      name: "Easter Sunday",
      localName: "Ostersonntag",
      holidayType: "public",
      isNationwide: true,
      region: undefined,
    });
    const nl2026 = rows.find(
      (r: any) => r.country === "NL" && r.date.getUTCFullYear() === 2026,
    );
    expect(nl2026.localName).toBe("Eerste Paasdag");
  });

  it.each([
    ["AT", "Ostersonntag"],
    ["CH", "Ostersonntag"],
    ["FR", "Dimanche de Pâques"],
    ["BE", "Easter Sunday"],
  ])("localizes Easter Sunday for %s as %s", async (country, localName) => {
    parksService.getSyncCountryCodes.mockResolvedValue([country]);

    await run();

    const rows = holidaysService.saveRawHolidays.mock.calls[0][0];
    expect(rows[0].localName).toBe(localName);
  });

  it("skips the Easter write when no country observes it", async () => {
    parksService.getSyncCountryCodes.mockResolvedValue(["US", "JP"]);

    await run();

    expect(holidaysService.saveRawHolidays).not.toHaveBeenCalled();
  });

  it.each([
    [2025, "2025-04-20"],
    [2026, "2026-04-05"],
    [2027, "2027-03-28"],
    [2028, "2028-04-16"],
  ])("computes Easter Sunday %i as %s", async (_year, expected) => {
    parksService.getSyncCountryCodes.mockResolvedValue(["DE"]);

    await run();

    const dates = holidaysService.saveRawHolidays.mock.calls[0][0].map(
      (r: any) => r.date.toISOString().slice(0, 10),
    );
    expect(dates).toContain(expected);
  });

  it("deletes holidays older than three years and queues gap filling", async () => {
    parksService.getSyncCountryCodes.mockResolvedValue(["US"]);
    holidaysService.deleteOldHolidays.mockResolvedValue(12);

    await run();

    const cutoff: Date = holidaysService.deleteOldHolidays.mock.calls[0][0];
    expect(cutoff.getFullYear()).toBe(2023);
    expect(parkMetadataQueue.add).toHaveBeenCalledWith(
      "fill-all-gaps",
      {},
      { priority: 5 },
    );
  });

  describe("curated and Korean sources (PAR-1065)", () => {
    const sources = () =>
      holidaysService.replaceHolidaysBySource.mock.calls.map((c) => c[0]);

    it("writes a country's curated file, and only for countries that have one", async () => {
      parksService.getSyncCountryCodes.mockResolvedValue(["AU", "DE", "CN"]);

      await run();

      expect(sources()).toEqual(["curated:AU:", "curated:CN:"]);
      const cn = holidaysService.replaceHolidaysBySource.mock.calls[1][1];
      expect(cn.some((r: any) => r.holidayType === "working_day")).toBe(true);
    });

    it("skips Korea's API sources without their keys", async () => {
      parksService.getSyncCountryCodes.mockResolvedValue(["KR"]);

      await run();

      expect(sources()).toEqual([]);
      expect(neisClient.getSchedule).not.toHaveBeenCalled();
      expect(kasiClient.getPublicHolidays).not.toHaveBeenCalled();
    });

    it("imports KASI holidays and NEIS majority vacations for the parks' regions", async () => {
      neisClient.enabled = true;
      kasiClient.enabled = true;
      parksService.getSyncCountryCodes.mockResolvedValue(["KR"]);
      parksService.getSyncRegionCodes.mockResolvedValue(["11", "Mars"]);
      kasiClient.getPublicHolidays.mockImplementation(async (y: number) =>
        y === 2026 ? [{ date: "2026-09-25", name: "추석" }] : [],
      );
      neisClient.getElementarySchools.mockResolvedValue(["a", "b"]);
      const vac = (d: string) => ({
        AA_YMD: d,
        EVENT_NM: "여름방학",
        SBTR_DD_SC_NM: "휴업일",
      });
      neisClient.getSchedule.mockImplementation(
        async (_o: string, school: string) =>
          school === "a"
            ? [vac("20260720"), vac("20260721")]
            : [vac("20260721")],
      );

      await run();

      expect(kasiClient.getPublicHolidays.mock.calls).toEqual([
        [2025],
        [2026],
        [2027],
        [2028],
      ]);
      expect(neisClient.getElementarySchools).toHaveBeenCalledWith("B10", 15);
      expect(neisClient.getSchedule).toHaveBeenCalledWith(
        "B10",
        "a",
        "2025-01-01",
        "2028-12-31",
      );
      const [
        [kasiPrefix, kasiRows, kasiFrom],
        [neisPrefix, neisRows, neisFrom],
      ] = holidaysService.replaceHolidaysBySource.mock.calls;
      expect([kasiPrefix, kasiFrom]).toEqual(["kasi:KR:", "2025-01-01"]);
      expect(kasiRows).toHaveLength(1);
      expect([neisPrefix, neisFrom]).toEqual(["neis:KR:", "2025-01-01"]);
      // 07-20: one of two schools — not a majority. 07-21: both.
      expect(neisRows.map((r: any) => r.externalId)).toEqual([
        "neis:KR:KR-11:2026-07-21",
      ]);
      expect((processor as any).logger.warn).toHaveBeenCalledWith(
        expect.stringContaining("Mars"),
      );
    });

    it("keeps the last good NEIS import when a fetch fails", async () => {
      neisClient.enabled = true;
      parksService.getSyncCountryCodes.mockResolvedValue(["KR"]);
      parksService.getSyncRegionCodes.mockResolvedValue(["41"]);
      neisClient.getElementarySchools.mockRejectedValue(new Error("quota"));

      await run();

      expect(sources()).toEqual([]);
      expect(parkMetadataQueue.add).toHaveBeenCalled();
    });
  });

  it("rethrows when the country lookup fails", async () => {
    parksService.getSyncCountryCodes.mockRejectedValue(new Error("db down"));

    await expect(run()).rejects.toThrow("db down");
    expect(parkMetadataQueue.add).not.toHaveBeenCalled();
  });
});
