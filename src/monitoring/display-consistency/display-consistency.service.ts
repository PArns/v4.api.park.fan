import { Inject, Injectable, Logger, type Type } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { DataSource } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { safeJsonParse } from "../../common/utils/json.util";
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
import { ParkyTipService, dayVerdict } from "../../assistant/parky-tip.service";
import {
  ASSISTANT_TOOLS,
  type AssistantToolContext,
} from "../../assistant/tools/assistant-tools";
import { PageViewsService } from "../../page-views/page-views.service";
import { parksWithRunningSeason } from "../../ml/utils/season-level.query";
import type { AdminPrincipal } from "../../admin/auth/admin-principal";
import { resolvePark, type AmbiguousPark } from "../../mcp/tools/park-ref";
import {
  compareSurfaces,
  type DisplayDifference,
  type DisplayMeasure,
  type DisplaySurface,
  type SurfaceValue,
} from "./display-consistency.rules";

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

/** One day as every surface showed it: crowd levels, headliner waits and stamps by surface. */
export interface DisplayDayView {
  date: string;
  crowdLevel: Record<string, string | number>;
  headlinerWait: Record<string, string | number>;
  verdict: Record<string, string | number>;
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
  parks: Array<{
    id: string;
    name: string;
    mismatches: number;
    failures: number;
  }>;
}

const message = (error: unknown) =>
  error instanceof Error ? error.message : String(error);

/** A day's values grouped by measure, keyed `surface` or `surface:key`; ride waits stay out. */
function dayViews(
  dates: readonly string[],
  values: readonly SurfaceValue[],
): DisplayDayView[] {
  const columns: Partial<Record<DisplayMeasure, keyof DisplayDayView>> = {
    crowd_level: "crowdLevel",
    headliner_wait: "headlinerWait",
    verdict: "verdict",
  };
  return dates.map((date) => {
    const view: DisplayDayView = {
      date,
      crowdLevel: {},
      headlinerWait: {},
      verdict: {},
    };
    for (const v of values) {
      const column = columns[v.measure];
      if (v.date !== date || !column || v.value === null) continue;
      const name = v.key ? `${v.surface}:${v.key}` : v.surface;
      (view[column] as Record<string, string | number>)[name] = v.value;
    }
    return view;
  });
}

/**
 * Reads one park-day as every surface would show it right now, each through the
 * method its endpoint or tool calls, and compares the pairs that must agree
 * (`display-consistency.rules.ts`). Reads only: the caches it touches are the
 * ones those endpoints fill on a miss. docs/analytics/display-consistency.md.
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

    await Promise.all([
      read(["calendar", "calendar_predicted", "calendar_so_far"], () =>
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
      read(["parky_tip", "parky_tip_served"], () =>
        this.tipValues(park, dates),
      ),
      read(["plan_day", "parky_get_park_day"], () =>
        this.dayPlanValues(park, dates),
      ),
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
      } catch (error) {
        this.logger.warn(
          `Display consistency failed for ${park.slug}: ${message(error)}`,
        );
      }
    }
    summary.parks.sort((a, b) => b.mismatches - a.mismatches);
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
    return [...new Set([...seasonal, ...viewed])];
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
      out.push(
        {
          date,
          surface: "calendar",
          measure: "crowd_level",
          value: day.crowdLevel,
        },
        {
          date,
          surface: "calendar_predicted",
          measure: "crowd_level",
          value: day.predictedCrowdLevel ?? null,
        },
        {
          date,
          surface: "calendar_so_far",
          measure: "crowd_level",
          value: day.todayCrowdLevel ?? null,
        },
        {
          date,
          surface: "calendar",
          measure: "verdict",
          value: dayVerdict(day),
        },
      );
      const forecast = day.headlinerForecast;
      if (!forecast || forecast.actual) continue;
      out.push({
        date,
        surface: "calendar",
        measure: "headliner_wait",
        value: forecast.avgWait,
      });
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

  /** The stamp a tip would be written with now, and the stamps of the tips served now. */
  private async tipValues(
    park: Park,
    dates: readonly string[],
  ): Promise<SurfaceValue[]> {
    const tips = this.get(ParkyTipService);
    const perDay = await Promise.all(
      dates.map(async (date) => {
        const [{ verdict }, served] = await Promise.all([
          tips.readDay(park, date),
          tips.servedVerdicts(park, date),
        ]);
        return [
          {
            date,
            surface: "parky_tip" as const,
            measure: "verdict" as const,
            value: verdict,
          },
          ...Object.entries(served).map(([locale, stamp]) => ({
            date,
            surface: "parky_tip_served" as const,
            measure: "verdict" as const,
            key: locale,
            value: stamp,
          })),
        ];
      }),
    );
    return perDay.flat();
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
