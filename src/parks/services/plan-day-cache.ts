import type { Redis } from "ioredis";
import { CacheKeys } from "../../common/cache/cache-keys";
import { cachedJson } from "../../common/cache/redis-json-cache";
import type { PlanDayDto } from "../dto/plan-day.dto";
import type { Park } from "../entities/park.entity";
import type { PlanDayService } from "./plan-day.service";

/** How long a built day is kept: a quarter hour, the width of its own slots. */
export const PLAN_DAY_CACHE_SECONDS = 15 * 60;

/**
 * `/plan/day` for one park and date, built once per quarter hour and shared by every reader: the
 * endpoint itself and the day optimizer, which plans against exactly the day the planner draws. A
 * day that says a dependency failed is not kept, so its "try again shortly" does not outlive the
 * fault.
 */
export function cachedPlanDay(
  redis: Redis,
  planDayService: PlanDayService,
  park: Park,
  date: string,
): Promise<PlanDayDto> {
  return cachedJson(
    redis,
    CacheKeys.planDayResponse(park.id, date),
    PLAN_DAY_CACHE_SECONDS,
    () => planDayService.buildPlanDay(park, date),
    (plan) => plan.ridesUnavailable?.reason !== "data_unavailable",
  );
}
