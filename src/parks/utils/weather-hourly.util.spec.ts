import { buildParkHourly, HOURLY_DAYS } from "./weather-hourly.util";
import type {
  HourlyDetail,
  HourlyDetailResponse,
} from "../../external-apis/weather/open-meteo.client";

/** UTC hours from `fromIso` for `count` hours, temperature = index. */
function raw(
  fromIso: string,
  count: number,
  extra: Partial<HourlyDetailResponse> = {},
): HourlyDetailResponse {
  const start = Date.parse(fromIso);
  const hours: HourlyDetail[] = Array.from({ length: count }, (_, i) => ({
    time: new Date(start + i * 3_600_000).toISOString().slice(0, 16) + "Z",
    temperature: i,
    apparentTemperature: i - 2,
    precipitation: 0,
    precipitationProbability: 10,
    weatherCode: 3,
    windSpeed: 12,
    windGusts: 30,
    windDirection: 250,
    isDay: true,
  }));
  return {
    fetchedAt: "2026-11-25T09:10:00.000Z",
    hours,
    sunrises: [],
    sunsets: [],
    ...extra,
  };
}

const TTL = 2 * 60 * 60;

describe("buildParkHourly", () => {
  it("cuts a European park's day midnight to midnight on its clock, today first", () => {
    const r = buildParkHourly(
      raw("2026-11-24T00:00:00Z", 17 * 24),
      "Europe/Berlin",
      Date.parse("2026-11-25T09:30:00Z"),
      TTL,
    );
    const today = r.days[0];
    expect(today.date).toBe("2026-11-25");
    expect(today.hours).toHaveLength(24);
    expect(today.hours[0].time).toBe("2026-11-25T00:00+01:00");
    expect(today.hours[23].time).toBe("2026-11-25T23:00+01:00");
    // 2026-11-24T23:00Z is index 23 → the first local hour of the 25th.
    expect(today.hours[0].temperatureC).toBe(23);
    expect(today.hours[0]).toMatchObject({
      apparentTemperatureC: 21,
      precipitationMm: 0,
      precipitationProbability: 10,
      weatherCode: 3,
      windSpeedKmh: 12,
      windGustsKmh: 30,
      windDirectionDeg: 250,
      isDay: true,
    });
  });

  it("caps the days at today + 14 and drops the hours outside them", () => {
    const r = buildParkHourly(
      raw("2026-11-24T00:00:00Z", 17 * 24),
      "Europe/Berlin",
      Date.parse("2026-11-25T09:30:00Z"),
      TTL,
    );
    expect(HOURLY_DAYS).toBe(15);
    expect(r.days).toHaveLength(15);
    expect(r.days[14].date).toBe("2026-12-09");
    expect(r.days.every((d) => d.hours.length === 24)).toBe(true);
  });

  it("keeps a park far east of UTC whole: Tokyo's today starts the UTC day before", () => {
    // 01:00 UTC on the 25th is 10:00 in Tokyo; its midnight was 15:00 UTC on the 24th.
    const r = buildParkHourly(
      raw("2026-11-24T00:00:00Z", 17 * 24),
      "Asia/Tokyo",
      Date.parse("2026-11-25T01:00:00Z"),
      TTL,
    );
    expect(r.days[0].date).toBe("2026-11-25");
    expect(r.days[0].hours).toHaveLength(24);
    expect(r.days[0].hours[0].time).toBe("2026-11-25T00:00+09:00");
    expect(r.days[0].hours[0].temperatureC).toBe(15);
  });

  it("keeps a park far west of UTC whole: Honolulu's today runs into the next UTC day", () => {
    const r = buildParkHourly(
      raw("2026-11-24T00:00:00Z", 18 * 24),
      "Pacific/Honolulu",
      Date.parse("2026-11-26T08:00:00Z"), // 22:00 on the 25th
      TTL,
    );
    expect(r.days[0].date).toBe("2026-11-25");
    expect(r.days[0].hours.map((h) => h.time.slice(11))).toEqual(
      Array.from(
        { length: 24 },
        (_, h) => `${String(h).padStart(2, "0")}:00-10:00`,
      ),
    );
  });

  it("serves 23 hours on a spring-forward day and 25 on a fall-back day", () => {
    const spring = buildParkHourly(
      raw("2026-03-28T00:00:00Z", 17 * 24),
      "Europe/Berlin",
      Date.parse("2026-03-29T08:00:00Z"),
      TTL,
    );
    expect(spring.days[0].date).toBe("2026-03-29");
    expect(spring.days[0].hours).toHaveLength(23);
    expect(spring.days[0].hours.map((h) => h.time.slice(11, 16))).not.toContain(
      "02:00",
    );

    const fall = buildParkHourly(
      raw("2026-10-24T00:00:00Z", 17 * 24),
      "Europe/Berlin",
      Date.parse("2026-10-25T08:00:00Z"),
      TTL,
    );
    expect(fall.days[0].date).toBe("2026-10-25");
    expect(fall.days[0].hours).toHaveLength(25);
    expect(fall.days[0].hours.map((h) => h.time.slice(11))).toEqual(
      expect.arrayContaining(["02:00+02:00", "02:00+01:00"]),
    );
  });

  it("files each sunrise and sunset under the park-local day it happens on", () => {
    // In GMT, Tokyo's sunrise (06:20 local) falls on the UTC day before.
    const r = buildParkHourly(
      raw("2026-11-24T00:00:00Z", 17 * 24, {
        sunrises: ["2026-11-24T21:20Z", "2026-11-25T21:21Z"],
        sunsets: ["2026-11-25T07:30Z", "2026-11-26T07:30Z"],
      }),
      "Asia/Tokyo",
      Date.parse("2026-11-25T01:00:00Z"),
      TTL,
    );
    expect(r.days[0].sunrise).toBe("2026-11-25T06:20+09:00");
    expect(r.days[0].sunset).toBe("2026-11-25T16:30+09:00");
    expect(r.days[1].sunrise).toBe("2026-11-26T06:21+09:00");
    expect(r.days[2].sunrise).toBeNull();
  });

  it("asks again when the cache runs out, or at the park's midnight if sooner", () => {
    const morning = buildParkHourly(
      raw("2026-11-24T00:00:00Z", 17 * 24),
      "Europe/Berlin",
      Date.parse("2026-11-25T09:30:00Z"),
      TTL,
    );
    expect(morning.generatedAt).toBe("2026-11-25T09:10:00.000Z");
    expect(morning.nextUpdateAt).toBe("2026-11-25T11:10:00.000Z");

    const lateEvening = buildParkHourly(
      {
        ...raw("2026-11-24T00:00:00Z", 17 * 24),
        fetchedAt: "2026-11-25T22:00:00.000Z",
      },
      "Europe/Berlin",
      Date.parse("2026-11-25T22:30:00Z"),
      TTL,
    );
    expect(lateEvening.nextUpdateAt).toBe("2026-11-25T23:00:00.000Z");
  });

  it("never points nextUpdateAt into the past", () => {
    const r = buildParkHourly(
      {
        ...raw("2026-11-24T00:00:00Z", 17 * 24),
        fetchedAt: "2026-11-25T01:00:00.000Z",
      },
      "Europe/Berlin",
      Date.parse("2026-11-25T09:30:00Z"),
      TTL,
    );
    expect(r.nextUpdateAt).toBe("2026-11-25T09:31:00.000Z");
  });

  it("leaves out days the upstream answer does not reach or cuts short", () => {
    const r = buildParkHourly(
      raw("2026-11-24T00:00:00Z", 3 * 24),
      "Europe/Berlin",
      Date.parse("2026-11-25T09:30:00Z"),
      TTL,
    );
    expect(r.days.map((d) => d.date)).toEqual(["2026-11-25", "2026-11-26"]);
    expect(r.days[1].hours).toHaveLength(24); // the 27th's lone 00:00 is dropped
  });
});
