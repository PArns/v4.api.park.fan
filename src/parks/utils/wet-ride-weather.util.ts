import type { Redis } from "ioredis";
import { CacheKeys } from "../../common/cache/cache-keys";
import type { ParkWithAttractionsDto } from "../dto/park-with-attractions.dto";

/**
 * Below this felt temperature (°C) at the park, a ride that gets its riders
 * wet is no tip: its queue is short because nobody wants to sit wet in the
 * cold, and nobody crosses the park for it (PAR-1420).
 */
export const WET_RIDE_MIN_FEELS_LIKE_C = 15;

/** The park payload's reading now (`weather.now`), as far as the rule reads it. */
export type WeatherNowReading = Pick<
  NonNullable<NonNullable<ParkWithAttractionsDto["weather"]>["now"]>,
  "temperature" | "apparentTemperature"
>;

/**
 * Whether a park is too cold right now for a ride that gets its riders wet:
 * its felt temperature (`apparentTemperature`, the air temperature where that
 * is missing) is below {@link WET_RIDE_MIN_FEELS_LIKE_C}. Without a reading it
 * is not, so a missing reading never marks a ride.
 */
export function tooColdToGetWet(
  now: WeatherNowReading | null | undefined,
): boolean {
  if (!now) return false;
  const feelsLike = now.apparentTemperature ?? now.temperature;
  return typeof feelsLike === "number" && feelsLike < WET_RIDE_MIN_FEELS_LIKE_C;
}

/**
 * Whether a planned day is too cold for a ride that gets its riders wet: its
 * warmest hour (`tempMax` of the day's forecast, or of the date's historical
 * average past the forecast's reach) stays below
 * {@link WET_RIDE_MIN_FEELS_LIKE_C}. A day has no felt temperature, only its
 * range, so the warmest hour stands in: a day that never gets above the line
 * is cold all day. Without a range it is not, so a missing reading never marks
 * a ride (PAR-1447).
 */
export function dayTooColdToGetWet(
  weather: { tempMax?: number | null } | null | undefined,
): boolean {
  const warmest = weather?.tempMax;
  return typeof warmest === "number" && warmest < WET_RIDE_MIN_FEELS_LIKE_C;
}

/** Swagger's words for `tooColdToGetWet` on a planned day's rides (`/plan/day`). */
export const DAY_TOO_COLD_TO_GET_WET_DESCRIPTION =
  "Present and true only while the ride gets its riders wet (`mayGetWet`) and the planned day's " +
  `warmest hour stays below ${WET_RIDE_MIN_FEELS_LIKE_C} °C (the day's tempMax; past the forecast's ` +
  "reach, the date's historical average). The ride stays what it is, a headliner included; the " +
  "key says why it is no tip on that day.";

/** Swagger's words for `tooColdToGetWet`, the same on every payload that carries it. */
export const TOO_COLD_TO_GET_WET_DESCRIPTION =
  "Present and true only while the ride gets its riders wet (`mayGetWet`) and the park feels " +
  `below ${WET_RIDE_MIN_FEELS_LIKE_C} °C now (weather.now.apparentTemperature, else temperature). ` +
  "The ride stays what it is, a headliner included; the key says why it is no tip right now.";

/**
 * `{ tooColdToGetWet: true }` for a ride that gets its riders wet while its
 * park is too cold for it ({@link tooColdToGetWet}), else nothing: one key a
 * ride carries only when it holds. A ride whose `mayGetWet` is unknown (null)
 * is not marked. The ride stays what it is otherwise, a headliner included;
 * the key says why it is no tip right now.
 */
export function wetInTheCold(
  mayGetWet: boolean | null | undefined,
  cold: boolean,
): { tooColdToGetWet?: true } {
  return cold && mayGetWet === true ? { tooColdToGetWet: true } : {};
}

/**
 * The park's weather reading now, from its prewarmed `park:integrated`
 * payload: no query of its own. `null` on a miss or an unreadable entry,
 * which {@link tooColdToGetWet} reads as no reading.
 */
export async function cachedParkWeatherNow(
  redis: Pick<Redis, "get">,
  parkId: string,
): Promise<WeatherNowReading | null> {
  const raw = await redis
    .get(CacheKeys.parkIntegrated(parkId))
    .catch(() => null);
  if (!raw) return null;
  try {
    return (JSON.parse(raw) as ParkWithAttractionsDto).weather?.now ?? null;
  } catch {
    return null;
  }
}
