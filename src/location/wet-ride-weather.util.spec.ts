import {
  tooColdToGetWet,
  WET_RIDE_MIN_FEELS_LIKE_C,
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
