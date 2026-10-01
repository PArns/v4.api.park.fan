import { OpenMeteoClient, toDailyWeather } from "./open-meteo.client";

/**
 * The sixteenth day of a 16-day forecast.
 *
 * Open-Meteo lists every date the request asked for in `daily.time`, but its
 * value arrays run out at the model's horizon. Mapped by index that produced a
 * `weather_data` row with every column null — and the calendar downstream read
 * those nulls as 0, putting "0°–0°" on 22 parks' tiles on 2026-08-28.
 */
describe("toDailyWeather", () => {
  it("keeps the days the model actually forecast", () => {
    const days = toDailyWeather({
      time: ["2026-09-10", "2026-09-11"],
      temperature_2m_max: [22, 20.4],
      temperature_2m_min: [15.7, 14],
      weathercode: [51, 53],
    });

    expect(days).toHaveLength(2);
    expect(days[0]).toMatchObject({ date: "2026-09-10", temperatureMax: 22 });
  });

  it("drops the trailing day the arrays have no numbers for", () => {
    const days = toDailyWeather({
      time: ["2026-09-11", "2026-09-12"],
      temperature_2m_max: [20.4, null],
      temperature_2m_min: [14, null],
      weathercode: [53, null],
    });

    expect(days.map((day) => day.date)).toEqual(["2026-09-11"]);
  });

  it("drops a day whose arrays simply end short", () => {
    const days = toDailyWeather({
      time: ["2026-09-11", "2026-09-12"],
      temperature_2m_max: [20.4],
      temperature_2m_min: [14],
    });

    expect(days.map((day) => day.date)).toEqual(["2026-09-11"]);
  });

  it("keeps a day that has a temperature but nothing else", () => {
    // Half a reading is still a reading; only "no temperature at all" is not.
    const days = toDailyWeather({
      time: ["2026-09-12"],
      temperature_2m_max: [18],
      temperature_2m_min: [null],
    });

    expect(days).toHaveLength(1);
    expect(days[0]!.temperatureMin).toBeNull();
  });

  it("keeps a genuine 0 °C day", () => {
    const days = toDailyWeather({
      time: ["2026-12-24"],
      temperature_2m_max: [0],
      temperature_2m_min: [0],
      weathercode: [71],
    });

    expect(days).toHaveLength(1);
  });
});

describe("OpenMeteoClient climate normals", () => {
  const redis = { get: jest.fn(), set: jest.fn(), mget: jest.fn() };
  const get = jest.fn();
  let client: OpenMeteoClient;

  const archive = {
    daily: {
      time: ["2020-07-11", "2021-07-11"],
      temperature_2m_max: [20, 30],
      temperature_2m_min: [10, 14],
    },
  };

  beforeEach(() => {
    jest.clearAllMocks();
    redis.get.mockResolvedValue(null);
    redis.set.mockResolvedValue("OK");
    get.mockResolvedValue({ data: archive });
    client = new OpenMeteoClient({} as never, redis as never);
    (client as unknown as { client: { get: jest.Mock } }).client = { get };
  });

  it("asks the archive once, with rounded coordinates, and caches 30 days", async () => {
    const normals = await client.fetchClimateNormals(50.8049, 6.8751);
    expect(normals["07-11"].temperatureMax).toBe(25);
    expect(get).toHaveBeenCalledTimes(1);
    expect(get.mock.calls[0][0]).toBe(
      "https://archive-api.open-meteo.com/v1/archive",
    );
    expect(get.mock.calls[0][1].params).toMatchObject({
      latitude: 50.8,
      longitude: 6.88,
      start_date: "2015-01-01",
      end_date: "2024-12-31",
    });
    expect(redis.set).toHaveBeenCalledWith(
      "weather:climate:2015-2024:50.8:6.88",
      expect.any(String),
      "EX",
      30 * 24 * 60 * 60,
    );
  });

  it("blocks the archive for 15 minutes on a failure, on its own key", async () => {
    get.mockRejectedValue(new Error("429"));
    await expect(client.fetchClimateNormals(50.8, 6.87)).rejects.toThrow();
    expect(redis.set).toHaveBeenCalledWith(
      "ratelimit:openmeteo-archive:blocked",
      "true",
      "EX",
      900,
    );
    // The forecast's shared keys stay untouched.
    const keys = redis.set.mock.calls.map((c) => c[0]);
    expect(keys).not.toContain("ratelimit:openmeteo:blocked");
    expect(keys).not.toContain("ratelimit:openmeteo:circuit");
  });

  it("does not call the archive while it is blocked", async () => {
    redis.get.mockImplementation((k: string) =>
      Promise.resolve(
        k === "ratelimit:openmeteo-archive:blocked" ? "true" : null,
      ),
    );
    await expect(client.fetchClimateNormals(50.8, 6.87)).rejects.toThrow(
      /blocked/,
    );
    expect(get).not.toHaveBeenCalled();
  });

  it("does not cache an empty result", async () => {
    get.mockResolvedValue({ data: { daily: { time: [] } } });
    await expect(client.fetchClimateNormals(50.8, 6.87)).rejects.toThrow();
    expect(redis.set.mock.calls.map((c) => c[0])).not.toContain(
      "weather:climate:2015-2024:50.8:6.87",
    );
  });

  it("peek never calls the archive", async () => {
    redis.get.mockResolvedValue(JSON.stringify({ "07-11": {} }));
    await expect(client.peekClimateNormals(50.8, 6.87)).resolves.toEqual({
      "07-11": {},
    });
    expect(get).not.toHaveBeenCalled();
  });
});
