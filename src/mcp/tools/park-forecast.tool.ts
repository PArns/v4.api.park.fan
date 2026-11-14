import { z } from "zod";
import { formatInTimeZone } from "date-fns-tz";
import { defineMcpTool } from "../mcp-tool";
import { isAmbiguous, resolvePark } from "./park-ref";
import { parkPageUrl } from "./links";
import { CalendarService } from "../../parks/services/calendar.service";
import { MlOverviewService } from "../../admin/ml-overview.service";
import { parseDateRange } from "../../common/utils/date-parsing.util";
import type { CalendarDay } from "../../parks/dto/integrated-calendar.dto";
import { PlanDayService } from "../../parks/services/plan-day.service";
import {
  worksPeriodCovering,
  type WorksPeriod,
} from "../../attractions/utils/curated-out-of-service.util";
import { ParkSeasonService } from "../../parks/services/park-season.service";
import type { ParkSeason } from "../../parks/entities/park-season.entity";
import { seasonsOn } from "../../trip-regions/trip-calendar.util";
import { seasonDayView } from "./trip-season.inputs";

const LOCAL_ISO = "yyyy-MM-dd'T'HH:mmxxx";
const HEADLINERS = 5;

/** An hour of the day's opening, in the park's clock with its offset. */
function localTime(value: string | undefined, timezone: string) {
  if (!value) return null;
  const at = new Date(value);
  return Number.isNaN(at.getTime())
    ? null
    : formatInTimeZone(at, timezone, LOCAL_ISO);
}

/** The rides a park has curated maintenance or works windows for, with them. */
type MaintenanceRides = ReadonlyArray<{
  name: string;
  windows: readonly WorksPeriod[];
}>;

/**
 * One calendar day for a tool. With `maintenance`, the rides a window shuts
 * that day, by name; a closed day lists none, since nothing runs anyway. With
 * `seasons`, the curated seasons running that day (`seasonsOn`): a cancelled
 * one never, one with single `dates` only on those.
 */
export function compactDay(
  day: CalendarDay,
  timezone: string,
  maintenance: MaintenanceRides = [],
  seasons: readonly ParkSeason[] = [],
) {
  const weather = day.weather;
  const inMaintenance =
    day.status === "CLOSED"
      ? []
      : maintenance
          .filter((ride) => worksPeriodCovering(ride.windows, day.date))
          .map((ride) => ride.name);
  const daySeasons = seasonsOn(seasons, day.date).map(seasonDayView);
  return {
    date: day.date,
    status: day.status,
    opens: localTime(day.hours?.openingTime, timezone),
    closes: localTime(day.hours?.closingTime, timezone),
    hoursInferred: day.hours?.isInferred || undefined,
    crowdLevel: day.crowdLevel,
    // The day-peak the level is rated on; absent for a closed day.
    expectedPeakWaitMinutes: day.headlinerForecast?.avgWait,
    forecastIsActual: day.headlinerForecast?.actual || undefined,
    weather: weather && {
      basis: weather.basis,
      condition: weather.condition,
      // The WMO code, as get_park_weather names it.
      weatherCode: weather.icon ?? null,
      tempMin: weather.tempMin,
      tempMax: weather.tempMax,
      precipitationMm: weather.precipitationMm ?? weather.rainChance,
    },
    holiday: day.isHoliday || undefined,
    schoolVacation: day.isSchoolVacation || undefined,
    bridgeDay: day.isBridgeDay || undefined,
    events: day.events?.length
      ? day.events.map((event) => `${event.name} (${event.type})`)
      : undefined,
    ridesInMaintenance: inMaintenance.length > 0 ? inMaintenance : undefined,
    seasons: daySeasons.length > 0 ? daySeasons : undefined,
  };
}

export const parkForecastTool = defineMcpTool({
  name: "park_forecast",
  title: "Park forecast",
  description:
    "What park.fan shows a visitor about the days ahead for one park: per day in the park's " +
    "timezone the opening hours (or closed), the crowd level as the calendar rates it (the day's " +
    "peak against a typical day's peak), the expected peak wait of the headliners, weather " +
    "(`basis: climate_normal` is the long-run average, not a forecast) and holidays, school " +
    "breaks and bridge days, the rides a maintenance or works window shuts that day " +
    "(ridesInMaintenance, by name), the park's curated seasons running that day (seasons: kind, " +
    "name, status where not confirmed, separateTicket); today's hourly forecast (expected level and wait per hour) and " +
    "today's headliner waits; and which models serve which horizon. Days after " +
    "`scheduleCoverage.to` have no published hours: their status is inferred, read it as " +
    "unknown. For the live state use get_park; for how wrong the forecasts are use ml_accuracy. " +
    "Pass the park id from find_parks, or its slug when that is unambiguous.",
  input: {
    park: z
      .string()
      .min(2)
      .max(120)
      .describe(
        'Park id (uuid) from find_parks, or a slug like "phantasialand"',
      ),
    days: z
      .number()
      .int()
      .min(1)
      .max(60)
      .default(14)
      .describe("Days ahead, today included"),
  },
  async handler({ park: reference, days }, ctx) {
    const found = await resolvePark(ctx, reference);
    if (isAmbiguous(found)) return found;
    const park = found;

    const { fromDate, toDate } = parseDateRange(undefined, undefined, {
      timezone: park.timezone,
      defaultToDaysAhead: days - 1,
    });
    // `today` carries the hourly curve; the calendar caches months per variant.
    const [calendar, overview, maintenance, seasons] = await Promise.all([
      ctx
        .get(CalendarService)
        .buildCalendarResponse(park, fromDate, toDate, "today"),
      ctx
        .get(MlOverviewService)
        .getOverview()
        .catch(() => null),
      // A failed lookup costs the days their maintenance list, never the days.
      Promise.resolve()
        .then(() => ctx.get(PlanDayService).maintenanceWindows(park))
        .catch(() => []),
      // Likewise their seasons: the public seasons route's query, over the same days.
      Promise.resolve()
        .then(() =>
          ctx
            .get(ParkSeasonService)
            .forParkInRange(
              park.id,
              formatInTimeZone(fromDate, park.timezone, "yyyy-MM-dd"),
              formatInTimeZone(toDate, park.timezone, "yyyy-MM-dd"),
            ),
        )
        .catch((): ParkSeason[] => []),
    ]);

    const today = calendar.days.find((day) => day.isToday);
    return {
      park: {
        id: park.id,
        name: park.name,
        timezone: park.timezone,
        pageUrl: parkPageUrl(park),
      },
      scheduleCoverage: calendar.meta.scheduleCoverage,
      days: calendar.days.map((day) =>
        compactDay(day, park.timezone, maintenance, seasons),
      ),
      todayHourly: today?.hourly?.length
        ? today.hourly.map((hour) => ({
            hour: hour.hour,
            crowdLevel: hour.crowdLevel,
            waitMinutes: hour.predictedWaitTime,
          }))
        : null,
      todayHeadliners: today?.headlinerForecast
        ? today.headlinerForecast.rides.slice(0, HEADLINERS).map((ride) => ({
            id: ride.attractionId,
            name: ride.name,
            waitMinutes: ride.waitTime,
            uncertaintyMinutes: ride.uncertaintyMinutes ?? undefined,
          }))
        : null,
      // Straight from the ML overview, so a switch flipped in production shows here.
      servingModels: overview
        ? overview.components
            .filter((component) => component.role === "served")
            .map(({ id, name, delivers, status }) => ({
              id,
              name,
              delivers,
              status,
            }))
        : null,
    };
  },
});
