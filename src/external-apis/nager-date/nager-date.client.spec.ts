import axios from "axios";
import { NagerDateClient } from "./nager-date.client";
import { HolidayType, NagerPublicHoliday } from "./nager-date.types";

jest.mock("axios");
const mockedAxios = axios as jest.Mocked<typeof axios>;

const holiday = (
  over: Partial<NagerPublicHoliday> = {},
): NagerPublicHoliday => ({
  date: "2026-12-25",
  localName: "Weihnachtstag",
  name: "Christmas Day",
  countryCode: "DE",
  fixed: true,
  global: true,
  counties: null,
  launchYear: null,
  types: [HolidayType.PUBLIC],
  ...over,
});

describe("NagerDateClient", () => {
  let client: NagerDateClient;
  let http: { get: jest.Mock };

  beforeEach(() => {
    http = { get: jest.fn() };
    mockedAxios.create = jest.fn().mockReturnValue(http);
    client = new NagerDateClient();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("points the axios instance at the v3 API with a 20 s timeout", () => {
    expect(mockedAxios.create).toHaveBeenCalledWith(
      expect.objectContaining({
        baseURL: "https://date.nager.at/api/v3",
        timeout: 20000,
      }),
    );
  });

  describe("getPublicHolidays", () => {
    it("requests /PublicHolidays/{year}/{country} and returns the body", async () => {
      const body = [holiday()];
      http.get.mockResolvedValue({ data: body });

      await expect(client.getPublicHolidays(2026, "DE")).resolves.toBe(body);
      expect(http.get).toHaveBeenCalledWith("/PublicHolidays/2026/DE");
    });

    it("returns an empty list unchanged", async () => {
      http.get.mockResolvedValue({ data: [] });
      await expect(client.getPublicHolidays(2026, "DE")).resolves.toEqual([]);
    });

    it("keeps the counties of a regional holiday", async () => {
      const regional = holiday({
        name: "Corpus Christi",
        global: false,
        counties: ["DE-BW", "DE-BY"],
      });
      http.get.mockResolvedValue({ data: [regional] });

      const [result] = await client.getPublicHolidays(2026, "DE");
      expect(result.global).toBe(false);
      expect(result.counties).toEqual(["DE-BW", "DE-BY"]);
    });

    it("wraps an HTTP error and names the cause", async () => {
      http.get.mockRejectedValue(new Error("Request failed with status 404"));
      jest.spyOn((client as any).logger, "error").mockImplementation();

      await expect(client.getPublicHolidays(2026, "XX")).rejects.toThrow(
        "Nager.Date API error: Request failed with status 404",
      );
    });

    it("stringifies a non-Error rejection", async () => {
      http.get.mockRejectedValue("boom");
      jest.spyOn((client as any).logger, "error").mockImplementation();

      await expect(client.getPublicHolidays(2026, "DE")).rejects.toThrow(
        "Nager.Date API error: boom",
      );
    });
  });

  describe("getAvailableCountries", () => {
    it("returns the country list", async () => {
      const body = [{ countryCode: "DE", name: "Germany" }];
      http.get.mockResolvedValue({ data: body });

      await expect(client.getAvailableCountries()).resolves.toBe(body);
      expect(http.get).toHaveBeenCalledWith("/AvailableCountries");
    });

    it("wraps an HTTP error", async () => {
      http.get.mockRejectedValue(new Error("timeout of 20000ms exceeded"));
      jest.spyOn((client as any).logger, "error").mockImplementation();

      await expect(client.getAvailableCountries()).rejects.toThrow(
        "Nager.Date API error: timeout of 20000ms exceeded",
      );
    });
  });

  describe("getLongWeekends", () => {
    it("returns the body on success", async () => {
      const body = [{ startDate: "2026-12-24", endDate: "2026-12-27" }];
      http.get.mockResolvedValue({ data: body });

      await expect(client.getLongWeekends(2026, "DE")).resolves.toBe(body);
      expect(http.get).toHaveBeenCalledWith("/LongWeekend/2026/DE");
    });

    it("returns an empty list instead of throwing on an HTTP error", async () => {
      http.get.mockRejectedValue(new Error("503"));
      jest.spyOn((client as any).logger, "warn").mockImplementation();

      await expect(client.getLongWeekends(2026, "DE")).resolves.toEqual([]);
    });
  });

  describe("getHolidaysForYears", () => {
    beforeEach(() => {
      jest.spyOn(client as any, "sleep").mockResolvedValue(undefined);
    });

    it("concatenates every year from start to end inclusive", async () => {
      http.get.mockImplementation((url: string) =>
        Promise.resolve({
          data: [holiday({ date: `${url.split("/")[2]}-01-01` })],
        }),
      );

      const result = await client.getHolidaysForYears("DE", 2025, 2027);

      expect(http.get.mock.calls.map((c) => c[0])).toEqual([
        "/PublicHolidays/2025/DE",
        "/PublicHolidays/2026/DE",
        "/PublicHolidays/2027/DE",
      ]);
      expect(result.map((h) => h.date)).toEqual([
        "2025-01-01",
        "2026-01-01",
        "2027-01-01",
      ]);
    });

    it("skips a failing year and keeps the others", async () => {
      jest.spyOn((client as any).logger, "error").mockImplementation();
      jest.spyOn((client as any).logger, "warn").mockImplementation();
      http.get
        .mockResolvedValueOnce({ data: [holiday({ date: "2025-01-01" })] })
        .mockRejectedValueOnce(new Error("500"))
        .mockResolvedValueOnce({ data: [holiday({ date: "2027-01-01" })] });

      const result = await client.getHolidaysForYears("DE", 2025, 2027);

      expect(result.map((h) => h.date)).toEqual(["2025-01-01", "2027-01-01"]);
    });

    it("returns an empty list when the range is empty", async () => {
      await expect(
        client.getHolidaysForYears("DE", 2027, 2026),
      ).resolves.toEqual([]);
      expect(http.get).not.toHaveBeenCalled();
    });
  });

  it("sleep resolves after the given delay", async () => {
    jest.useFakeTimers();
    const done = jest.fn();
    (client as any).sleep(100).then(done);

    await jest.advanceTimersByTimeAsync(99);
    expect(done).not.toHaveBeenCalled();
    await jest.advanceTimersByTimeAsync(1);
    expect(done).toHaveBeenCalled();
    jest.useRealTimers();
  });
});
