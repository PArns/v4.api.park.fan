import type { ParkWithAttractionsDto } from "../parks/dto/park-with-attractions.dto";

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
 * is not, so a missing reading never takes a ride away.
 */
export function tooColdToGetWet(
  now: WeatherNowReading | null | undefined,
): boolean {
  if (!now) return false;
  const feelsLike = now.apparentTemperature ?? now.temperature;
  return typeof feelsLike === "number" && feelsLike < WET_RIDE_MIN_FEELS_LIKE_C;
}
