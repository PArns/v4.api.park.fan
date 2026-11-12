import {
  BadRequestException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  ServiceUnavailableException,
} from "@nestjs/common";
import type { Redis } from "ioredis";
import { availableParallelism } from "os";
import { CacheKeys } from "../common/cache/cache-keys";
import { cachedJson } from "../common/cache/redis-json-cache";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  getCurrentDateInTimezone,
  getCurrentTimeInTimezone,
  isRealIsoDate,
} from "../common/utils/date.util";
import type { Park } from "../parks/entities/park.entity";
import { ParksService } from "../parks/parks.service";
import {
  cachedPlanDay,
  PLAN_DAY_CACHE_SECONDS,
} from "../parks/services/plan-day-cache";
import { PlanDayService } from "../parks/services/plan-day.service";
import type { DayOptimizerRequest } from "./day-optimizer/engine";
import {
  compiledWorkerFile,
  DayOptimizerBusyError,
  DayOptimizerPool,
  DayOptimizerTimeoutError,
} from "./day-optimizer/pool";
import type {
  DayClock,
  OptimizerDay,
  PlannerEntry,
} from "./day-optimizer/types";
import type {
  PlanOptimizeRequestDto,
  PlanOptimizeResponseDto,
  PlannerEntryDto,
} from "./dto/plan-optimize.dto";

/**
 * Where a planned date sits against the park's clock. The phase is the server's, from the park's
 * own zone; the minute is the caller's where it sent one for today, so the plan starts where the
 * visitor's now line is, and the server's park-local minute otherwise.
 */
export function dayClockFor(
  timezone: string,
  date: string,
  nowMinute?: number,
): DayClock {
  const today = getCurrentDateInTimezone(timezone);
  if (date < today) return { phase: "past" };
  if (date > today) return { phase: "future" };
  if (nowMinute !== undefined) return { phase: "today", nowMinute };
  const now = getCurrentTimeInTimezone(timezone);
  return { phase: "today", nowMinute: now.getHours() * 60 + now.getMinutes() };
}

/** An entry as the engine reads it: the planner's fields and nothing the transport added. */
function plainEntry(entry: PlannerEntryDto): PlannerEntry {
  return {
    id: entry.id,
    startMinute: entry.startMinute,
    ...(entry.attractionSlug !== undefined && {
      attractionSlug: entry.attractionSlug,
    }),
    ...(entry.attractionName !== undefined && {
      attractionName: entry.attractionName,
    }),
    ...(entry.custom && {
      custom: {
        label: entry.custom.label,
        icon: entry.custom.icon,
        durationMinutes: entry.custom.durationMinutes,
      },
    }),
    ...(entry.showSlug !== undefined && { showSlug: entry.showSlug }),
    ...(entry.done !== undefined && { done: entry.done }),
    ...(entry.actualWait !== undefined && { actualWait: entry.actualWait }),
  };
}

/**
 * The single-day trip planner's optimizer, behind `POST …/plan/optimize`: loads the day the planner
 * draws (`/plan/day` from its own cache, plus the rides a works window shuts), takes the clock from
 * the park's zone, and hands the search to {@link DayOptimizerPool}. The browser never uploads a
 * forecast; it sends its plan and gets one back. See docs/frontend/plan-day/optimize.md.
 */
@Injectable()
export class PlanOptimizeService implements OnModuleDestroy {
  private readonly logger = new Logger(PlanOptimizeService.name);
  private readonly pool = new DayOptimizerPool({
    size: Math.max(
      1,
      Number(process.env.PLAN_OPTIMIZE_WORKERS) ||
        Math.min(2, availableParallelism() - 1),
    ),
    maxQueue: 64,
    timeoutMs: 20_000,
    workerFile: compiledWorkerFile(),
  });

  constructor(
    private readonly parksService: ParksService,
    private readonly planDayService: PlanDayService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async onModuleDestroy(): Promise<void> {
    await this.pool.close();
  }

  async optimize(
    path: {
      continent: string;
      country: string;
      city: string;
      parkSlug: string;
    },
    body: PlanOptimizeRequestDto,
  ): Promise<PlanOptimizeResponseDto> {
    PlanOptimizeService.check(body);
    const park = await this.parksService.findByGeographicPath(
      path.continent,
      path.country,
      path.city,
      path.parkSlug,
    );
    if (!park) {
      throw new NotFoundException(
        `Park with slug "${path.parkSlug}" not found in ${path.city}, ${path.country}, ${path.continent}`,
      );
    }

    const clock = dayClockFor(park.timezone, body.date, body.nowMinute);
    // A walked day is refused before anything is loaded for it.
    const day =
      clock.phase === "past" ? null : await this.dayFor(park, body.date);
    const request: DayOptimizerRequest = {
      mode: body.mode,
      entries: body.entries.map(plainEntry),
      prefs: body.prefs && {
        ...(body.prefs.riderHeightCm !== undefined && {
          riderHeightCm: body.prefs.riderHeightCm,
        }),
        ...(body.prefs.avoidWet !== undefined && {
          avoidWet: body.prefs.avoidWet,
        }),
        ...(body.prefs.earlyEntry !== undefined && {
          earlyEntry: body.prefs.earlyEntry,
        }),
      },
      add: body.add,
      choice: body.choice && {
        dropped: body.choice.dropped,
        droppedBlocks: body.choice.droppedBlocks,
        shortBlocks: body.choice.shortBlocks,
        priority: body.choice.priority,
      },
      levers: body.levers,
      preview: body.preview,
    };

    try {
      const result = await this.pool.run({ day, request, clock });
      return { date: body.date, ...result } as PlanOptimizeResponseDto;
    } catch (error) {
      if (
        error instanceof DayOptimizerBusyError ||
        error instanceof DayOptimizerTimeoutError
      ) {
        this.logger.warn(`plan/optimize ${park.slug}: ${error.message}`);
        throw new ServiceUnavailableException(
          "The optimizer is busy; try again shortly.",
        );
      }
      throw error;
    }
  }

  /**
   * What class-validator cannot say about one field alone: a real calendar day, ids unique within
   * the day, and the fit-only fields only on a fit request.
   */
  private static check(body: PlanOptimizeRequestDto): void {
    if (!isRealIsoDate(body.date)) {
      throw new BadRequestException(
        `Invalid date "${body.date}" — expected YYYY-MM-DD`,
      );
    }
    const ids = new Set(body.entries.map((entry) => entry.id));
    if (ids.size !== body.entries.length) {
      throw new BadRequestException("entries: every id must be unique");
    }
    if (
      body.mode !== "fit" &&
      (body.add !== undefined ||
        body.choice !== undefined ||
        body.levers !== undefined)
    ) {
      throw new BadRequestException(
        "add, choice and levers are only read with mode fit",
      );
    }
    if (body.mode === "fit" && body.preview !== undefined) {
      throw new BadRequestException("preview is not read with mode fit");
    }
  }

  /**
   * The day the planner draws: `/plan/day` from its own cache, with the rides a works window shuts
   * that day and the plan leaves out, which the optimizer holds still rather than prices.
   */
  private async dayFor(park: Park, date: string): Promise<OptimizerDay> {
    const [plan, works] = await Promise.all([
      cachedPlanDay(this.redis, this.planDayService, park, date),
      cachedJson(
        this.redis,
        CacheKeys.planDayRidesInWorks(park.id, date),
        PLAN_DAY_CACHE_SECONDS,
        () => this.planDayService.ridesInWorks(park, date),
      ),
    ]);
    const planned = new Set(plan.rides.map((ride) => ride.attractionSlug));
    const ridesInWorks = works.filter(
      (ride) => !planned.has(ride.attractionSlug),
    );
    const day: OptimizerDay = plan;
    return ridesInWorks.length > 0 ? { ...day, ridesInWorks } : day;
  }
}
