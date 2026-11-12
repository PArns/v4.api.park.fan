import { BadRequestException, Inject, Injectable } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { CacheKeys } from "../common/cache/cache-keys";
import { TRIP_REGION_RESPONSE_TTL } from "../common/cache/cache-ttl";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { HOTEL_KINDS, type HotelKind } from "./regions";
import {
  MAX_PARK_DAYS,
  MAX_TRIP_DAYS,
  MAX_VISITS_PER_PARK,
  optimizeTrip,
  type PartiesOption,
  type PlanCell,
  type PlanInput,
  type PlanLock,
  type PlannedDay,
  type RestOption,
} from "./trip-optimizer";
import {
  buildLegs,
  findLeg,
  hopPairs,
  legFromHotel,
  type TripLeg,
} from "./travel";
import { TripRegionService } from "./trip-region.service";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";
import type { TripPlanDto } from "./dto/trip-region-response.dto";

/** The plan's options after validation, in one canonical form. */
export interface NormalizedPlanQuery {
  from: string;
  to: string;
  parks: Array<{ slug: string; visits: number }>;
  hotel: HotelKind;
  area?: string;
  arrival: "free" | "park";
  departure: "free" | "park";
  blocks: boolean;
  parties: PartiesOption;
  rest: RestOption;
  hop: boolean;
  locks: PlanLock[];
}

const flag = (value: string | undefined): boolean =>
  value === "1" || value === "true";

/** A leg as a plan day shows it. */
function travelOf(leg: TripLeg | undefined) {
  if (!leg) return undefined;
  return {
    from: leg.from,
    to: leg.to,
    ...(leg.km != null && { km: leg.km }),
    minutes: leg.minutes,
    mode: leg.mode,
    ...(leg.via && { via: leg.via }),
    ...(leg.transferMinutes != null && {
      transferMinutes: leg.transferMinutes,
    }),
  };
}

/**
 * `GET /v1/trip-regions/:region/plan`: reads the region calendar for the trip
 * once, hands it to the optimizer and adds the travel. Cached an hour by the
 * normalized query. Contract: docs/frontend/trip-regions.md.
 */
@Injectable()
export class TripPlanService {
  private readonly flight = new SingleFlight();

  constructor(
    private readonly regions: TripRegionService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /** Validates the query against the region; 400 with the reason otherwise. */
  async normalize(
    slug: string,
    query: TripPlanQueryDto,
  ): Promise<NormalizedPlanQuery> {
    const region = this.regions.region(slug);
    this.regions.validateWindow(region, query.from, query.to, MAX_TRIP_DAYS);
    const tripDays =
      Math.round(
        (Date.parse(`${query.to}T00:00:00Z`) -
          Date.parse(`${query.from}T00:00:00Z`)) /
          86_400_000,
      ) + 1;

    const resolved = await this.regions.resolveParks(region);
    const known = new Set(resolved.map((p) => p.config.slug));
    const parks: Array<{ slug: string; visits: number }> = [];
    if (query.parks) {
      for (const part of query.parks.split(",")) {
        const [slug, n] = part.split(":");
        const visits = n ? Number(n) : 1;
        if (!known.has(slug)) {
          throw new BadRequestException(
            `${slug} is not a park of ${region.name}`,
          );
        }
        if (parks.some((p) => p.slug === slug)) {
          throw new BadRequestException(`${slug} is listed twice`);
        }
        if (visits < 1 || visits > MAX_VISITS_PER_PARK) {
          throw new BadRequestException(
            `A park takes 1 to ${MAX_VISITS_PER_PARK} visits`,
          );
        }
        parks.push({ slug, visits });
      }
    } else {
      for (const p of resolved) {
        if (p.config.headliner) parks.push({ slug: p.config.slug, visits: 1 });
      }
    }
    parks.sort((a, b) => a.slug.localeCompare(b.slug));

    const hop = flag(query.hop);
    const parkDays = parks.reduce((s, p) => s + p.visits, 0);
    if (parkDays > MAX_PARK_DAYS) {
      throw new BadRequestException(
        `A plan takes at most ${MAX_PARK_DAYS} park visits`,
      );
    }
    if (parkDays > tripDays * (hop ? 2 : 1)) {
      throw new BadRequestException(
        hop
          ? "More park visits than two a day"
          : "More park visits than trip days: set hop=1 or choose fewer",
      );
    }

    const area = query.area;
    const areaConfig = area
      ? region.hotelAreas.find((h) => h.key === area)
      : undefined;
    if (area && !areaConfig) {
      throw new BadRequestException(
        `${area} is not a hotel area of ${region.name}`,
      );
    }
    const hotel = (query.hotel ?? areaConfig?.hotel ?? "offsite") as HotelKind;
    if (!HOTEL_KINDS.includes(hotel)) {
      throw new BadRequestException(
        `hotel must be one of ${HOTEL_KINDS.join(", ")}`,
      );
    }

    const rest: RestOption =
      !query.rest || query.rest === "auto"
        ? { mode: "auto" }
        : query.rest === "none"
          ? { mode: "none" }
          : { mode: "max", n: Number(query.rest.slice(4)) };

    const locks: PlanLock[] = [];
    for (const part of query.lock ? query.lock.split(",") : []) {
      const [target, date] = part.split("@");
      if (target !== "rest" && !parks.some((p) => p.slug === target)) {
        throw new BadRequestException(
          `lock ${part}: ${target} is not one of the plan's parks`,
        );
      }
      if (date < query.from || date > query.to) {
        throw new BadRequestException(
          `lock ${part}: the date is outside the trip`,
        );
      }
      locks.push({ target, date });
    }
    locks.sort((a, b) =>
      `${a.date}${a.target}`.localeCompare(`${b.date}${b.target}`),
    );

    return {
      from: query.from,
      to: query.to,
      parks,
      hotel,
      ...(area && { area }),
      arrival: query.arrival === "park" ? "park" : "free",
      departure: query.departure === "park" ? "park" : "free",
      blocks: flag(query.blocks),
      parties: (query.parties ?? "avoid") as PartiesOption,
      rest,
      hop,
      locks,
    };
  }

  /** The canonical query string a plan is cached by. */
  cacheQuery(q: NormalizedPlanQuery): string {
    return [
      q.from,
      q.to,
      q.parks.map((p) => `${p.slug}:${p.visits}`).join(","),
      q.hotel,
      q.area ?? "-",
      q.arrival,
      q.departure,
      q.blocks ? 1 : 0,
      q.parties,
      q.rest.mode === "max" ? `max:${q.rest.n}` : q.rest.mode,
      q.hop ? 1 : 0,
      q.locks.map((l) => `${l.target}@${l.date}`).join(","),
    ].join("|");
  }

  async plan(slug: string, query: TripPlanQueryDto): Promise<TripPlanDto> {
    const q = await this.normalize(slug, query);
    const key = CacheKeys.tripRegionPlan(slug, this.cacheQuery(q));
    const cached = safeJsonParse<TripPlanDto>(await this.redis.get(key));
    if (cached) return cached;
    return this.flight.run(key, async () => {
      const built = await this.build(slug, q);
      await this.redis
        .set(key, JSON.stringify(built), "EX", TRIP_REGION_RESPONSE_TTL)
        .catch(() => undefined);
      return built;
    });
  }

  private async build(
    slug: string,
    q: NormalizedPlanQuery,
  ): Promise<TripPlanDto> {
    const region = this.regions.region(slug);
    const calendar = await this.regions.internalCalendar(region, q.from, q.to);
    const configs = new Map(region.parks.map((p) => [p.slug, p]));
    const legs = buildLegs(region);
    const slugs = q.parks.map((p) => p.slug);

    const cells: PlanInput["cells"] = {};
    for (const s of slugs) {
      cells[s] = {};
      for (const day of calendar.days) {
        const c = day.parks[s];
        if (!c) continue;
        const cell: PlanCell = {
          status: c.status,
          basis: c.basis,
          ...(c.headlinerWait != null && { headlinerWait: c.headlinerWait }),
          ...(c.crowdLevel && { crowdLevel: c.crowdLevel }),
          ...(c.dayHours != null && { dayHours: c.dayHours }),
          ...(c.early && { early: { kind: c.early.kind } }),
          ...(c.earlyUnknown && { earlyUnknown: true }),
          ...(c.evening && { evening: { kind: c.evening.kind } }),
          ...(c.weather && { weather: c.weather }),
        };
        cells[s][day.date] = cell;
      }
    }

    const plan = optimizeTrip({
      days: calendar.days.map((d) => ({
        date: d.date,
        weekday: new Date(`${d.date}T12:00:00Z`).getUTCDay(),
        isHoliday: d.isHoliday,
        isSchoolVacation: d.isSchoolVacation,
      })),
      parks: q.parks.map((p) => {
        const config = configs.get(p.slug);
        return {
          slug: p.slug,
          visits: p.visits,
          group: config?.group ?? "",
          type: config?.type ?? "theme_park",
        };
      }),
      cells,
      options: {
        arrival: q.arrival,
        departure: q.departure,
        blocks: q.blocks,
        blockGroups: region.groups
          .filter((g) => g.kind === "resort")
          .map((g) => g.key),
        parties: q.parties,
        rest: q.rest,
        hop: q.hop,
        hopPairs: hopPairs(region, legs, slugs),
        perkRules: region.perks[q.hotel],
        locks: q.locks,
      },
    });

    const days = plan.days.map((day) => this.withTravel(day, legs, q.area));
    return {
      meta: {
        region: region.slug,
        from: q.from,
        to: q.to,
        computedAt: new Date().toISOString(),
        objective: "headliner_wait",
        exact: plan.exact,
        ...(plan.relaxed.length > 0 && { relaxed: plan.relaxed }),
      },
      days: days as unknown as Array<Record<string, unknown>>,
      parks: plan.parks as unknown as Array<Record<string, unknown>>,
      totals: plan.totals as unknown as Record<string, unknown>,
      unplaced: plan.unplaced,
      ...(plan.ignoredLocks.length > 0 && { ignoredLocks: plan.ignoredLocks }),
    };
  }

  /** A park day's travel from the hotel area; a hop day's leg between its parks. */
  private withTravel(day: PlannedDay, legs: TripLeg[], area?: string) {
    if (day.parks.length === 0) return day;
    const first = day.parks[0].slug;
    const travel = area ? travelOf(legFromHotel(legs, area, first)) : undefined;
    const between =
      day.parks.length > 1
        ? travelOf(
            (() => {
              const leg = findLeg(legs, first, day.parks[1].slug);
              return leg && { ...leg, from: first, to: day.parks[1].slug };
            })(),
          )
        : undefined;
    return {
      ...day,
      ...(travel && { travel }),
      ...(between && { between }),
    };
  }
}
