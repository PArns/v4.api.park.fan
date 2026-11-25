import { addDays } from "date-fns";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import type { HourlyDetailResponse } from "../../external-apis/weather/open-meteo.client";

/** The park's today plus the 14 days of a forecast strip; nothing past it is served. */
export const HOURLY_DAYS = 15;

/** One hour on the park's clock: °C, mm, %, km/h, degrees the wind comes FROM. */
export interface ParkHour {
  /** The hour's start on the park's clock, with its offset (`2026-11-25T14:00+01:00`). */
  time: string;
  temperatureC: number | null;
  apparentTemperatureC: number | null;
  precipitationMm: number | null;
  precipitationProbability: number | null;
  weatherCode: number | null;
  windSpeedKmh: number | null;
  windGustsKmh: number | null;
  windDirectionDeg: number | null;
  isDay: boolean | null;
}

export interface ParkHourlyDay {
  /** Park-local date, `YYYY-MM-DD`. */
  date: string;
  sunrise: string | null;
  sunset: string | null;
  /** 24 hours, 23 on a spring-forward day, 25 on a fall-back day. */
  hours: ParkHour[];
}

export interface ParkHourly {
  generatedAt: string;
  nextUpdateAt: string;
  days: ParkHourlyDay[];
}

const onParkClock = (iso: string, tz: string) =>
  formatInTimeZone(new Date(iso), tz, "yyyy-MM-dd'T'HH:mmXXX");

/**
 * Cuts one coordinate's cached UTC hours into the park's days, today first, at
 * read time — so the same cache entry rolls over at each park's own midnight.
 * `nextUpdateAt` is when the cache entry runs out, or the park's next midnight
 * if that comes first (today changes then), and never less than a minute away.
 */
export function buildParkHourly(
  raw: HourlyDetailResponse,
  timezone: string,
  nowMs: number,
  ttlSeconds: number,
): ParkHourly {
  const today = formatInTimeZone(nowMs, timezone, "yyyy-MM-dd");
  // Noon steps 24 h at a time: a DST shift moves it an hour, never off its date.
  const noon = fromZonedTime(`${today}T12:00:00`, timezone);
  const days = new Map<string, ParkHourlyDay>();
  for (let i = 0; i < HOURLY_DAYS; i++) {
    const date = formatInTimeZone(addDays(noon, i), timezone, "yyyy-MM-dd");
    days.set(date, { date, sunrise: null, sunset: null, hours: [] });
  }

  for (const h of raw.hours) {
    const time = onParkClock(h.time, timezone);
    days.get(time.slice(0, 10))?.hours.push({
      time,
      temperatureC: h.temperature,
      apparentTemperatureC: h.apparentTemperature,
      precipitationMm: h.precipitation,
      precipitationProbability: h.precipitationProbability,
      weatherCode: h.weatherCode,
      windSpeedKmh: h.windSpeed,
      windGustsKmh: h.windGusts,
      windDirectionDeg: h.windDirection,
      isDay: h.isDay,
    });
  }
  // A GMT day holds one park day's sunset and the next one's sunrise, so each
  // event is filed under the park-local date it happens on.
  for (const [list, key] of [
    [raw.sunrises, "sunrise"],
    [raw.sunsets, "sunset"],
  ] as const) {
    for (const iso of list) {
      const time = onParkClock(iso, timezone);
      const day = days.get(time.slice(0, 10));
      if (day && day[key] == null) day[key] = time;
    }
  }

  const tomorrow = formatInTimeZone(addDays(noon, 1), timezone, "yyyy-MM-dd");
  const midnightMs = fromZonedTime(`${tomorrow}T00:00:00`, timezone).getTime();
  const expiresMs = Date.parse(raw.fetchedAt) + ttlSeconds * 1000;
  const nextMs = Math.max(nowMs + 60_000, Math.min(expiresMs, midnightMs));

  return {
    generatedAt: raw.fetchedAt,
    nextUpdateAt: new Date(nextMs).toISOString(),
    // A day the upstream window cuts short is no curve; 23 is a spring-forward day.
    days: [...days.values()].filter((d) => d.hours.length >= 23),
  };
}
