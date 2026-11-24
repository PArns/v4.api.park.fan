import {
  cachedParkWeatherNow,
  tooColdToGetWet,
  WET_RIDE_MIN_FEELS_LIKE_C,
  wetInTheCold,
} from "./wet-ride-weather.util";

describe("tooColdToGetWet", () => {
  it("is cold below the felt temperature", () => {
    expect(
      tooColdToGetWet({ temperature: 2.2, apparentTemperature: -2.1 }),
    ).toBe(true);
  });

  it("is not at the line", () => {
    expect(
      tooColdToGetWet({
        temperature: WET_RIDE_MIN_FEELS_LIKE_C,
        apparentTemperature: WET_RIDE_MIN_FEELS_LIKE_C,
      }),
    ).toBe(false);
  });

  it("reads the felt temperature before the air", () => {
    expect(tooColdToGetWet({ temperature: 16, apparentTemperature: 12 })).toBe(
      true,
    );
    expect(tooColdToGetWet({ temperature: 13, apparentTemperature: 16 })).toBe(
      false,
    );
  });

  it("falls back to the air where the felt temperature is missing", () => {
    expect(tooColdToGetWet({ temperature: 8, apparentTemperature: null })).toBe(
      true,
    );
  });

  it("is not cold without a reading", () => {
    expect(tooColdToGetWet(null)).toBe(false);
    expect(tooColdToGetWet(undefined)).toBe(false);
  });
});

describe("wetInTheCold", () => {
  it("marks a wet ride in the cold, and nothing else", () => {
    expect(wetInTheCold(true, true)).toEqual({ tooColdToGetWet: true });
    expect(wetInTheCold(true, false)).toEqual({});
    expect(wetInTheCold(false, true)).toEqual({});
    expect(wetInTheCold(null, true)).toEqual({});
  });
});

describe("cachedParkWeatherNow", () => {
  const now = { temperature: 2.2, apparentTemperature: -2.1 };

  it("reads weather.now from the park's cached payload", async () => {
    const redis = {
      get: jest.fn().mockResolvedValue(JSON.stringify({ weather: { now } })),
    };
    await expect(cachedParkWeatherNow(redis, "p1")).resolves.toEqual(now);
    expect(redis.get).toHaveBeenCalledWith(expect.stringContaining("p1"));
  });

  it("is null on a miss, a broken entry or a failing cache", async () => {
    for (const get of [
      jest.fn().mockResolvedValue(null),
      jest.fn().mockResolvedValue("{"),
      jest.fn().mockRejectedValue(new Error("down")),
    ]) {
      await expect(cachedParkWeatherNow({ get }, "p1")).resolves.toBeNull();
    }
  });
});
