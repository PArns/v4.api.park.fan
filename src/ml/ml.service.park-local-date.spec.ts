import { MLService } from "./ml.service";
import { parkLocalHourToUtcIso } from "../parks/weather.service";

/**
 * PAR-818: CatBoost predictions carry UTC instants, schedules and TFT rows are
 * keyed on the park-local day, and Open-Meteo answers park-local wall-clock
 * hours without an offset. These pin the two conversions at the boundary.
 */
describe("MLService.localDateOf", () => {
  it("reads an instant on the park's clock, not the UTC date", () => {
    // 20:00 in Los Angeles (PDT) is 03:00 UTC of the next day.
    expect(
      MLService.localDateOf("2026-07-05T03:00:00+00:00", "America/Los_Angeles"),
    ).toBe("2026-07-04");
    // 08:00 in Tokyo is 23:00 UTC of the previous day.
    expect(
      MLService.localDateOf("2026-07-03T23:00:00.000Z", "Asia/Tokyo"),
    ).toBe("2026-07-04");
  });

  it("takes an offset-less TFT day at its word", () => {
    expect(
      MLService.localDateOf("2026-07-04T12:00:00", "America/Los_Angeles"),
    ).toBe("2026-07-04");
  });
});

describe("parkLocalHourToUtcIso", () => {
  it("turns an Open-Meteo local hour into the UTC instant it names", () => {
    expect(parkLocalHourToUtcIso("2026-07-04T14:00", "Europe/Berlin")).toBe(
      "2026-07-04T12:00:00.000Z",
    );
    expect(parkLocalHourToUtcIso("2026-07-04T14:00", "Asia/Tokyo")).toBe(
      "2026-07-04T05:00:00.000Z",
    );
    expect(
      parkLocalHourToUtcIso("2026-01-15T16:00", "America/Los_Angeles"),
    ).toBe("2026-01-16T00:00:00.000Z");
  });

  it("leaves a string that already is an instant on the same instant", () => {
    expect(parkLocalHourToUtcIso("2026-07-04T12:00:00Z", "Asia/Tokyo")).toBe(
      "2026-07-04T12:00:00.000Z",
    );
    expect(
      parkLocalHourToUtcIso("2026-07-04T14:00:00+02:00", "Asia/Tokyo"),
    ).toBe("2026-07-04T12:00:00.000Z");
  });
});
