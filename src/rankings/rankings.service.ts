import { Inject, Injectable, NotFoundException } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { roundToNearest5Minutes } from "../common/utils/wait-time.utils";
import { ParksService } from "../parks/parks.service";
import { Park } from "../parks/entities/park.entity";
import {
  ParkHistoricalStatsService,
  YEAR_RANKING_MIN_ATTRACTION_DAYS,
  YearAttractionStat,
} from "../analytics/park-historical-stats.service";
import { HISTORICAL_STATS_CACHE_VERSION } from "../common/cache/cache-keys";
import {
  AttractionRankingDto,
  RankedAttractionDto,
} from "./dto/attraction-ranking.dto";

/**
 * The first year the measured history ranks: most parks' history starts in the last days of 2025,
 * so no ride reaches the year rankings' floor in 2025. The frontend's `FIRST_RANKING_YEAR` says the same.
 */
export const FIRST_RANKING_YEAR = 2026;

/**
 * The year an unnamed ranking means: the running one from March on, the previous one before,
 * when a new year's ranking holds only the few indoor rides open since January. The frontend's `currentRankingYear` follows
 * the same rule, so Parky and the site quote the same year.
 */
export function defaultRankingYear(now: Date = new Date()): number {
  const year = now.getUTCFullYear();
  const shown = now.getUTCMonth() < 2 ? year - 1 : year;
  return Math.max(FIRST_RANKING_YEAR, shown);
}

/** Parks whose year rows are computed at once on a cold cache. */
const PARK_CONCURRENCY = 4;
const CACHE_TTL_SECONDS = 24 * 60 * 60;

interface Scope {
  continentSlug: string;
  countrySlug: string | null;
}

/**
 * Year rankings across parks: which rides had the longest waits in a country
 * or a continent. Every row is a park's own `topAttractions` row over the
 * calendar year, so the order inside one park never differs from the park's
 * statistics page.
 */
@Injectable()
export class RankingsService {
  private readonly flight = new SingleFlight();

  constructor(
    private readonly parksService: ParksService,
    private readonly historicalStats: ParkHistoricalStatsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async getAttractionRanking(
    scope: Scope,
    year: number,
    limit: number,
  ): Promise<AttractionRankingDto> {
    const full = await this.getFullRanking(scope, year);
    return {
      attractions: full.attractions.slice(0, limit),
      meta: full.meta,
    };
  }

  private async getFullRanking(
    scope: Scope,
    year: number,
  ): Promise<AttractionRankingDto> {
    const key = `rankings:attractions:v${HISTORICAL_STATS_CACHE_VERSION}:${scope.continentSlug}:${scope.countrySlug ?? "*"}:${year}`;
    const cached = safeJsonParse<AttractionRankingDto>(
      await this.redis.get(key),
    );
    if (cached) return cached;

    return this.flight.run(key, async () => {
      const parks = scope.countrySlug
        ? await this.parksService.findByCountry(
            scope.continentSlug,
            scope.countrySlug,
          )
        : await this.parksService.findByContinent(scope.continentSlug);
      if (parks.length === 0) {
        throw new NotFoundException(
          `No parks in ${scope.continentSlug}${scope.countrySlug ? `/${scope.countrySlug}` : ""}`,
        );
      }

      const perPark = await this.mapLimited(parks, (park) =>
        this.historicalStats.getYearAttractionStats(park, year),
      );
      const ranking = rankAttractions(parks, perPark, scope, year);
      await this.redis.set(
        key,
        JSON.stringify(ranking),
        "EX",
        CACHE_TTL_SECONDS,
      );
      return ranking;
    });
  }

  private async mapLimited<T, R>(
    items: T[],
    fn: (item: T) => Promise<R>,
  ): Promise<R[]> {
    const results: R[] = new Array<R>(items.length);
    let next = 0;
    const worker = async () => {
      while (next < items.length) {
        const index = next++;
        results[index] = await fn(items[index]);
      }
    };
    await Promise.all(
      Array.from({ length: Math.min(PARK_CONCURRENCY, items.length) }, worker),
    );
    return results;
  }
}

/**
 * Merges the parks' year rows into one ranking: by the unrounded average P90,
 * then the unrounded P50, then the name, so equal rounded figures still get a
 * stable order. Exported for the unit test.
 */
export function rankAttractions(
  parks: Park[],
  perPark: YearAttractionStat[][],
  scope: Scope,
  year: number,
): AttractionRankingDto {
  const rows = parks.flatMap((park, i) =>
    perPark[i].map((stat) => ({ park, stat })),
  );
  rows.sort(
    (a, b) =>
      b.stat.avgP90 - a.stat.avgP90 ||
      b.stat.avgP50 - a.stat.avgP50 ||
      a.stat.attractionName.localeCompare(b.stat.attractionName),
  );

  const attractions: RankedAttractionDto[] = rows.map(({ park, stat }, i) => ({
    rank: i + 1,
    attractionSlug: stat.attractionSlug,
    attractionName: stat.attractionName,
    land: stat.land,
    parkSlug: park.slug,
    parkName: park.curatedName ?? park.name,
    continentSlug: park.continentSlug,
    countrySlug: park.countrySlug,
    citySlug: park.citySlug,
    avgWaitP50: roundToNearest5Minutes(stat.avgP50),
    avgWaitP90: roundToNearest5Minutes(stat.avgP90),
    sampleDays: stat.sampleDays,
  }));

  const dataTo = rows.reduce<string | null>(
    (latest, { stat }) =>
      latest === null || stat.dataTo > latest ? stat.dataTo : latest,
    null,
  );
  return {
    attractions,
    meta: {
      year,
      continentSlug: scope.continentSlug,
      countrySlug: scope.countrySlug,
      yearComplete:
        rows.length > 0 &&
        rows.every(({ stat }) => stat.dataTo === `${year}-12-31`),
      dataTo,
      minAttractionDays: YEAR_RANKING_MIN_ATTRACTION_DAYS,
      parkCount: perPark.filter((stats) => stats.length > 0).length,
      totalRanked: attractions.length,
      generatedAt: new Date().toISOString(),
    },
  };
}
