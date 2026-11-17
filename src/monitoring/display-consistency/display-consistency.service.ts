import { Inject, Injectable, Logger, type Type } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { safeJsonParse } from "../../common/utils/json.util";
import { isCanonicalUuid } from "../../common/utils/uuid-param.util";
import { parseDateRange } from "../../common/utils/date-parsing.util";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../common/utils/date.util";
import { round5 } from "../../trip-regions/trip-calendar.util";
import type { Park } from "../../parks/entities/park.entity";
import { ParksService } from "../../parks/parks.service";
import { CalendarService } from "../../parks/services/calendar.service";
import { BestDaysService } from "../../parks/services/best-days.service";
import { PlanDayService } from "../../parks/services/plan-day.service";
import { cachedPlanDay } from "../../parks/services/plan-day-cache";
import { ParkIntegrationService } from "../../parks/services/park-integration.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { TripRegionService } from "../../trip-regions/trip-region.service";
import { regionOfPark } from "../../trip-regions/regions";
import {
  ASSISTANT_TOOLS,
  type AssistantToolContext,
} from "../../assistant/tools/assistant-tools";
import { PageViewsService } from "../../page-views/page-views.service";
import { parksWithRunningSeason } from "../../ml/utils/season-level.query";
import type { AdminPrincipal } from "../../admin/auth/admin-principal";
import { resolvePark, type AmbiguousPark } from "../../mcp/tools/park-ref";
import type { CalendarDay } from "../../parks/dto/integrated-calendar.dto";
import {
  compareSurfaces,
  type DisplayDifference,
  type DisplayMeasure,
  type DisplaySurface,
  type RelayCopy,
  type SurfaceValue,
} from "./display-consistency.rules";
import { readRelay, relayUrls } from "./display-consistency.relays";

/** Today, tomorrow and the seven days after. */
export const DISPLAY_CONSISTENCY_DEFAULT_DAYS = 9;
export const DISPLAY_CONSISTENCY_MAX_DAYS = 14;

/** A run's results outlive the next run by a day, so a failed night still leaves one. */
const RESULT_TTL_SECONDS = 2 * 24 * 60 * 60;
const KEY_PREFIX = "display-consistency:v1";
const SUMMARY_KEY = `${KEY_PREFIX}:summary`;
const parkKey = (parkId: string) => `${KEY_PREFIX}:park:${parkId}`;

/** The most read park pages of the last week the daily run adds to the parks in a season. */
const MOST_VIEWED_PARKS = 25;
const MOST_VIEWED_DAYS = 7;

/** The days of one park whose `/plan/day` and `get_park_day` are built at once. */
const DAY_CONCURRENCY = 3;

/** A surface that could not be read; its pairs are left out of the comparison. */
export interface SurfaceFailure {
  surface: DisplaySurface;
  error: string;
}

/** One day as every surface showed it: crowd levels and headliner waits by surface. */
export interface DisplayDayView {
  date: string;
  crowdLevel: Record<string, string | number>;
  headlinerWait: Record<string, string | number>;
}

export interface DisplayConsistencyResult {
  park: { id: string; name: string; slug: string; timezone: string };
  /** When the surfaces were read, all of them within this check. */
  checkedAt: string;
  days: DisplayDayView[];
  /** Pairs of known values compared. */
  compared: number;
  mismatches: DisplayDifference[];
  differences: DisplayDifference[];
  failures: SurfaceFailure[];
}

/** What the daily run found, per park; the details are each park's own entry. */
export interface DisplayConsistencySummary {
  checkedAt: string;
  days: number;
  mismatches: number;
  /** Surfaces that could not be read, over every park; a park whose check threw counts once. */
  failures: number;
  parks: Array<{
    id: string;
    name: string;
    mismatches: number;
    failures: number;
    /** Why the park's check threw; it then has no result of its own. */
    error?: string;
  }>;
}

/** The last run's summary and the result of each park it flagged, as the admin and MCP show it. */
export interface DisplayConsistencyLastRun {
  summary: DisplayConsistencySummary;
  /** The parks with a mismatch or a failure, in the summary's order. */
  parks: DisplayConsistencyResult[];
}

const message = (error: unknown) =>
  error instanceof Error ? error.message : String(error);

/** A day's values grouped by measure, keyed `surface` or `surface:key`; ride waits stay out. */
function dayViews(
  dates: readonly string[],
  values: readonly SurfaceValue[],
): DisplayDayView[] {
  const columns: Partial<
    Record<DisplayMeasure, "crowdLevel" | "headlinerWait">
  > = {
    crowd_level: "crowdLevel",
    headliner_wait: "headlinerWait",
  };
  return dates.map((date) => {
    const view: DisplayDayView = { date, crowdLevel: {}, headlinerWait: {} };
    for (const v of values) {
      const column = columns[v.measure];
      if (v.date !== date || !column || v.value === null) continue;
      const name = v.key ? `${v.surface}:${v.key}` : v.surface;
      view[column][name] = v.value;
    }
    return view;
  });
}

/** The fields of a calendar day the check reads, as the API or a relay of it serves them. */
type CalendarDayFields = Partial<
  Pick<CalendarDay, "date" | "crowdLevel" | "headlinerForecast">
>;

/**
 * A calendar day's crowd level and, on a forecast day, its headliner wait. A
 * relay's body is read the same way, so a field of another type counts as absent.
 */
function calendarDayValues(
  day: CalendarDayFields,
  surface: "calendar" | "calendar_relay",
  copy?: RelayCopy,
): SurfaceValue[] {
  if (typeof day.date !== "string") return [];
  const at = { date: day.date, surface, ...(copy && { copy }) };
  const forecast = day.headlinerForecast;
  return [
    {
      ...at,
      measure: "crowd_level",
      value: typeof day.crowdLevel === "string" ? day.crowdLevel : null,
    },
    ...(forecast && !forecast.actual && typeof forecast.avgWait === "number"
      ? [{ ...at, measure: "headliner_wait" as const, value: forecast.avgWait }]
      : []),
  ];
}

/** The calendar relay's months as the grid and its day dialog read them, the checked days only. */
async function calendarRelayValues(
  urls: readonly string[],
  page: string,
  dates: readonly string[],
): Promise<SurfaceValue[]> {
  const months = await Promise.all(urls.map((url) => readRelay(url, page)));
  return months.flatMap(({ body, copy }) =>
    (body.days as CalendarDayFields[])
      .filter((day) => dates.includes(day.date as string))
      .flatMap((day) => calendarDayValues(day, "calendar_relay", copy)),
  );
}

/** The best-days relay's snapshot as „Prognose heute" and the best-days section read it. */
async function bestDaysRelayValues(
  url: string,
  page: string,
  dates: readonly string[],
): Promise<SurfaceValue[]> {
  const { body, copy } = await readRelay(url, page);
  const asOf = body.meta?.computedAt;
  return (body.days as CalendarDayFields[])
    .filter((day) => dates.includes(day.date as string))
    .map((day) => ({
      date: day.date as string,
      surface: "best_days_relay" as const,
      measure: "crowd_level" as const,
      value: typeof day.crowdLevel === "string" ? day.crowdLevel : null,
      ...(typeof asOf === "string" && { asOf }),
      copy,
    }));
}

/**
 * Reads one park-day as every surface would show it right now, each through the
 * method its endpoint or tool calls, and through park.fan's own relays as a
 * visitor's browser asks for them, and compares the pairs that must agree
 * (`display-consistency.rules.ts`). Reads only: the caches it touches are the
 * ones those endpoints and relays fill on a miss.
 * docs/analytics/display-consistency.md.
 */
@Injectable()
export class DisplayConsistencyService {
  private readonly logger = new Logger(DisplayConsistencyService.name);

  constructor(
    private readonly moduleRef: ModuleRef,
    private readonly dataSource: DataSource,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  private get<T>(token: Type<T>): T {
    return this.moduleRef.get(token, { strict: false });
  }

  /** A park by id, slug or name, as the admin's MCP tools resolve one. */
  findPark(
    reference: string,
    admin: AdminPrincipal,
  ): Promise<Park | AmbiguousPark> {
    return resolvePark({ admin, get: (token) => this.get(token) }, reference);
  }

  /** Every surface's values for `days` park-local days from today, compared. */
  async check(
    park: Park,
    days: number = DISPLAY_CONSISTENCY_DEFAULT_DAYS,
  ): Promise<DisplayConsistencyResult> {
    const today = getCurrentDateInTimezone(park.timezone);
    const dates = Array.from({ length: days }, (_, i) => addIsoDays(today, i));
    const checkedAt = new Date().toISOString();
    const values: SurfaceValue[] = [];
    const failures: SurfaceFailure[] = [];
    const read = (
      surfaces: DisplaySurface[],
      load: () => Promise<SurfaceValue[]>,
    ) =>
      load().then(
        (found) => {
          values.push(...found);
        },
        (error: unknown) => {
          for (const surface of surfaces)
            failures.push({ surface, error: message(error) });
        },
      );

    const relays = relayUrls(park, dates);
    await Promise.all([
      read(["calendar", "calendar_so_far"], () =>
        this.calendarValues(park, dates),
      ),
      read(["best_days"], () => this.bestDaysValues(park, dates)),
      read(["trip_region", "trip_region_last_year"], () =>
        this.tripRegionValues(park, dates),
      ),
      read(["park_live", "park_lists_live"], () =>
        this.liveValues(park, today),
      ),
      read(["parky_live", "parky_today_expected"], () =>
        this.parkyLiveValues(park, today),
      ),
      read(["parky_get_days"], () => this.parkyDaysValues(park, dates)),
      read(["plan_day", "parky_get_park_day"], () =>
        this.dayPlanValues(park, dates),
      ),
      ...(relays
        ? [
            read(["calendar_relay"], () =>
              calendarRelayValues(relays.calendar, relays.page, dates),
            ),
            read(["best_days_relay"], () =>
              bestDaysRelayValues(relays.bestDays, relays.page, dates),
            ),
          ]
        : []),
    ]);

    const comparison = compareSurfaces(values);
    return {
      park: {
        id: park.id,
        name: park.name,
        slug: park.slug,
        timezone: park.timezone,
      },
      checkedAt,
      days: dayViews(dates, values),
      ...comparison,
      failures,
    };
  }

  /**
   * The daily run: the parks in a running season and the most read park pages,
   * one after the other, each park's result and the run's summary kept for two days.
   */
  async runDaily(): Promise<DisplayConsistencySummary> {
    const parkIds = await this.parksToCheck();
    const checkedAt = new Date().toISOString();
    const parks = this.get(ParksService);
    const summary: DisplayConsistencySummary = {
      checkedAt,
      days: DISPLAY_CONSISTENCY_DEFAULT_DAYS,
      mismatches: 0,
      failures: 0,
      parks: [],
    };
    for (const id of parkIds) {
      const park = await parks.findById(id).catch(() => null);
      if (!park || park.retiredAt) continue;
      try {
        const result = await this.check(park);
        await this.redis.set(
          parkKey(park.id),
          JSON.stringify(result),
          "EX",
          RESULT_TTL_SECONDS,
        );
        summary.parks.push({
          id: park.id,
          name: park.name,
          mismatches: result.mismatches.length,
          failures: result.failures.length,
        });
        summary.mismatches += result.mismatches.length;
        summary.failures += result.failures.length;
      } catch (error) {
        summary.parks.push({
          id: park.id,
          name: park.name,
          mismatches: 0,
          failures: 1,
          error: message(error),
        });
        summary.failures++;
      }
    }
    summary.parks.sort(
      (a, b) => b.mismatches - a.mismatches || b.failures - a.failures,
    );
    await this.redis.set(
      SUMMARY_KEY,
      JSON.stringify(summary),
      "EX",
      RESULT_TTL_SECONDS,
    );
    return summary;
  }

  /** The last daily run's summary, or null when none ran in the last two days. */
  async latestSummary(): Promise<DisplayConsistencySummary | null> {
    return safeJsonParse<DisplayConsistencySummary>(
      await this.redis.get(SUMMARY_KEY),
    );
  }

  /** One park's result from the last daily run, or null. */
  async latestForPark(
    parkId: string,
  ): Promise<DisplayConsistencyResult | null> {
    return safeJsonParse<DisplayConsistencyResult>(
      await this.redis.get(parkKey(parkId)),
    );
  }

  /** The last run with the result of each park it flagged, or null when none is kept. */
  async latestRun(): Promise<DisplayConsistencyLastRun | null> {
    const summary = await this.latestSummary();
    if (!summary) return null;
    const flagged = summary.parks.filter(
      (park) => park.mismatches > 0 || park.failures > 0,
    );
    const parks = await Promise.all(
      flagged.map((park) => this.latestForPark(park.id)),
    );
    return { summary, parks: parks.filter((park) => park !== null) };
  }

  /** The parks in a running season first, then the most read park pages of the week. */
  async parksToCheck(): Promise<string[]> {
    const [seasonal, viewed] = await Promise.all([
      parksWithRunningSeason(this.dataSource).catch((error: unknown) => {
        this.logger.warn(`Running seasons unreadable: ${message(error)}`);
        return [] as string[];
      }),
      this.get(PageViewsService)
        .adminOverview(MOST_VIEWED_DAYS, MOST_VIEWED_PARKS)
        .then(
          (overview) =>
            overview.kinds
              .find((kind) => kind.kind === "park")
              ?.top.map((entry) => entry.key) ?? [],
        )
        .catch((error: unknown) => {
          this.logger.warn(`Most read parks unreadable: ${message(error)}`);
          return [] as string[];
        }),
    ]);
    return [...new Set([...seasonal, ...viewed])].filter(isCanonicalUuid);
  }

  /** `/calendar` with `includeHourly=none`, what the grid, the day dialog and the month summary read. */
  private async calendarValues(
    park: Park,
    dates: readonly string[],
  ): Promise<SurfaceValue[]> {
    const { fromDate, toDate } = parseDateRange(
      dates[0],
      dates[dates.length - 1],
      { timezone: park.timezone },
    );
    const calendar = await this.get(CalendarService).buildCalendarResponse(
      park,
      fromDate,
      toDate,
      "none",
    );
    const out: SurfaceValue[] = [];
    for (const day of calendar.days) {
      if (!dates.includes(day.date)) continue;
      const date = day.date;
      out.push(...calendarDayValues(day, "calendar"), {
        date,
        surface: "calendar_so_far",
        measure: "crowd_level",
        value: day.todayCrowdLevel ?? null,
      });
      const forecast = day.headlinerForecast;
      if (!forecast || forecast.actual) continue;
      for (const ride of forecast.rides) {
        out.push({
          date,
          surface: "calendar",
          measure: "ride_wait",
          key: ride.attractionId,
          value: ride.waitTime,
        });
      }
    }
    return out;
  }

  /** `/best-days`, the snapshot the month calendar paints from. */
  private async bestDaysValues(
    park: Park,
    dates: readonly string[],
  ): Promise<SurfaceValue[]> {
    const best = await this.get(BestDaysService).getBestDays(
      park,
      dates[0],
      dates[dates.length - 1],
    );
    return best.days.map((day) => ({
      date: day.date,
      surface: "best_days" as const,
      measure: "crowd_level" as const,
      value: day.crowdLevel,
      ...(best.meta.computedAt && { asOf: best.meta.computedAt }),
    }));
  }

  /** The region calendar's day of this park, when the park is in a trip region. */
  private async tripRegionValues(
    park: Park,
    dates: readonly string[],
  ): Promise<SurfaceValue[]> {
    const region = regionOfPark(park);
    if (!region) return [];
    const calendar = await this.get(TripRegionService).calendar(
      region.slug,
      dates[0],
      dates[dates.length - 1],
    );
    const out: SurfaceValue[] = [];
    for (const day of calendar.days) {
      const cell = day.parks[park.slug];
      if (!cell || !dates.includes(day.date)) continue;
      const asOf = calendar.meta.computedAt;
      out.push({
        date: day.date,
        surface:
          cell.basis === "last_year" ? "trip_region_last_year" : "trip_region",
        measure: "crowd_level",
        value: cell.crowdLevel,
        asOf,
      });
      if (cell.forecastWait !== undefined) {
        out.push({
          date: day.date,
          surface: "trip_region",
          measure: "headliner_wait",
          value: cell.forecastWait,
          asOf,
        });
      }
    }
    return out;
  }

  /**
   * The live level as the park payload and the park lists carry it, while the
   * payload says the park is open: a closed park has no live level to show.
   */
  private async liveValues(park: Park, today: string): Promise<SurfaceValue[]> {
    if (!Array.isArray(park.attractions)) {
      await this.get(ParksService).loadParkRelations(park);
    }
    const [payload, lists] = await Promise.all([
      this.get(ParkIntegrationService).buildIntegratedResponse(
        park,
        false,
        false,
      ),
      this.get(AnalyticsService).getBatchParkOccupancy([park.id]),
    ]);
    if (payload.status !== "OPERATING") return [];
    const statistics = payload.analytics?.statistics;
    const occupancy = lists.get(park.id);
    return [
      {
        date: today,
        surface: "park_live",
        measure: "crowd_level",
        value: statistics?.crowdLevel ?? null,
        ...(statistics?.timestamp && { asOf: String(statistics.timestamp) }),
      },
      {
        date: today,
        surface: "park_lists_live",
        measure: "crowd_level",
        value: occupancy?.crowdLevel ?? null,
        ...(occupancy?.updatedAt && { asOf: occupancy.updatedAt }),
      },
    ];
  }

  /** Parky's `get_park_live`, which the park card returns too. */
  private async parkyLiveValues(
    park: Park,
    today: string,
  ): Promise<SurfaceValue[]> {
    const live = await this.parkyTool("get_park_live", { park: park.id });
    const crowd = live.crowd as { level?: string | null } | undefined;
    const expected = live.todayExpected as
      { crowdLevel?: string; expectedPeakWaitMinutes?: number } | undefined;
    return [
      {
        date: today,
        surface: "parky_live",
        measure: "crowd_level",
        value: crowd?.level ?? null,
      },
      {
        date: today,
        surface: "parky_today_expected",
        measure: "crowd_level",
        value: expected?.crowdLevel ?? null,
      },
      {
        date: today,
        surface: "parky_today_expected",
        measure: "headliner_wait",
        value: expected?.expectedPeakWaitMinutes ?? null,
      },
    ];
  }

  /** Parky's `get_days` over the whole window. */
  private async parkyDaysValues(
    park: Park,
    dates: readonly string[],
  ): Promise<SurfaceValue[]> {
    const answer = await this.parkyTool("get_days", {
      park: park.id,
      from: dates[0],
      to: dates[dates.length - 1],
    });
    const rows = (answer.days ?? []) as Array<{
      date: string;
      crowdLevel?: string;
      expectedPeakWaitMinutes?: number;
    }>;
    return rows.flatMap((row) => [
      {
        date: row.date,
        surface: "parky_get_days" as const,
        measure: "crowd_level" as const,
        value: row.crowdLevel ?? null,
      },
      {
        date: row.date,
        surface: "parky_get_days" as const,
        measure: "headliner_wait" as const,
        value: row.expectedPeakWaitMinutes ?? null,
      },
    ]);
  }

  /**
   * `/plan/day` through its cache, as the planner reads it, and Parky's
   * `get_park_day`, which builds the day itself; a few days at a time.
   */
  private async dayPlanValues(
    park: Park,
    dates: readonly string[],
  ): Promise<SurfaceValue[]> {
    const planDays = this.get(PlanDayService);
    const out: SurfaceValue[] = [];
    for (let i = 0; i < dates.length; i += DAY_CONCURRENCY) {
      const batch = await Promise.all(
        dates.slice(i, i + DAY_CONCURRENCY).map(async (date) => {
          const [plan, parkyDay] = await Promise.all([
            cachedPlanDay(this.redis, planDays, park, date),
            this.parkyTool("get_park_day", { park: park.id, date }),
          ]);
          const headliners = plan.rides.filter((ride) => ride.isHeadliner);
          const values: SurfaceValue[] = [
            {
              date,
              surface: "plan_day",
              measure: "crowd_level",
              value: plan.context.crowdLevel ?? null,
            },
            {
              date,
              surface: "parky_get_park_day",
              measure: "crowd_level",
              value: (parkyDay.crowdLevel as string | null | undefined) ?? null,
            },
            ...headliners.map((ride) => ({
              date,
              surface: "plan_day" as const,
              measure: "ride_wait" as const,
              key: ride.attractionId,
              value: ride.dayPeak,
            })),
          ];
          if (headliners.length > 0) {
            values.push({
              date,
              surface: "plan_day",
              measure: "headliner_wait",
              value: round5(
                headliners.reduce((sum, ride) => sum + ride.dayPeak, 0) /
                  headliners.length,
              ),
            });
          }
          return values;
        }),
      );
      out.push(...batch.flat());
    }
    return out;
  }

  /** One of Parky's chat tools, run as the chat runs it, with its answer before it is worded. */
  private async parkyTool(
    name: string,
    input: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const tool = ASSISTANT_TOOLS.find((t) => t.name === name);
    if (!tool) throw new Error(`Parky has no tool ${name}`);
    const ctx: AssistantToolContext = {
      get: (token) => this.get(token),
      locale: "en",
    };
    const answer = await tool.run(tool.input.parse(input), ctx);
    if (answer && typeof answer === "object" && "ambiguous" in answer) {
      throw new Error(`${name} could not resolve the park`);
    }
    return (answer ?? {}) as Record<string, unknown>;
  }
}
