import {
  BadRequestException,
  ConflictException,
  HttpException,
  HttpStatus,
  Inject,
  Injectable,
  Logger,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { randomUUID } from "crypto";
import { Redis } from "ioredis";
import { Repository } from "typeorm";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { checkRateLimit } from "../../common/redis/rate-limit.util";
import { isCanonicalUuid } from "../../common/utils/uuid-param.util";
import { clientErrorMessage } from "../../common/utils/client-error-message.util";
import { Park } from "../../parks/entities/park.entity";
import { ParkSeason } from "../../parks/entities/park-season.entity";
import {
  ParkSeasonService,
  type SeasonInput,
  type SeasonQuery,
} from "../../parks/services/park-season.service";
import { resolveCuratedPark } from "../../parks/utils/curated-park-facts.util";
import { TripRegionService } from "../../trip-regions/trip-region.service";
import { AdminAuditService } from "../auth/admin-audit.service";
import type { AdminPrincipal } from "../auth/admin-principal";
import { AdminCurationService } from "./admin-curation.service";
import {
  regionParksOf,
  seasonOnFile,
  seasonRowInput,
  seasonRowLabel,
  seasonRowProblem,
  type SeasonImportOutcome,
  type SeasonImportRow,
  type SeasonOnFile,
} from "./season-import";

/** Rows one import takes; the largest researched file has 56. */
export const MAX_SEASON_IMPORT_ROWS = 100;

/** Imports, dry runs included, one account may start per hour. */
export const SEASON_IMPORTS_PER_HOUR = 20;

/** How long a writing import holds the lock, at most. */
const IMPORT_LOCK_SECONDS = 5 * 60;
const IMPORT_LOCK_KEY = "admin:season-import:lock";

export interface AdminSeasonQuery extends Omit<SeasonQuery, "parkIds"> {
  /** A trip region's slug: the seasons of its parks. */
  region?: string;
}

/** A season with its park's curated name, as the admin's list shows it. */
export type AdminSeasonView = Omit<ParkSeason, "park"> & {
  park: { id: string; name: string; slug: string } | null;
};

export interface SeasonImportRowResult {
  /** 1-based, as the file is read. */
  row: number;
  label: string;
  outcome: SeasonImportOutcome;
  parkId?: string;
  /** The season written, or the one on file the row repeats. */
  seasonId?: string;
  reason?: string;
}

export interface SeasonImportResult {
  dryRun: boolean;
  /** `create` counts the rows written, or that would be in a dry run. */
  counts: { create: number; exists: number; invalid: number; failed: number };
  rows: SeasonImportRowResult[];
}

/** A row that passed the checks, with its park. */
interface CheckedRow {
  index: number;
  row: SeasonImportRow;
  parkId: string;
  input: SeasonInput;
}

/**
 * The admin's seasons: list, create, edit, delete and import, each write
 * audited and published outside-in through `AdminCurationService.publish`.
 * The admin routes (`AdminContentController`) and the MCP server's season
 * tools both write through here, so a season reads the same whichever wrote
 * it. The rules of a row live in `ParkSeasonService`; the import's in
 * `season-import.ts`.
 */
@Injectable()
export class AdminSeasonService {
  private readonly logger = new Logger(AdminSeasonService.name);

  constructor(
    private readonly seasons: ParkSeasonService,
    private readonly curation: AdminCurationService,
    private readonly audit: AdminAuditService,
    private readonly regions: TripRegionService,
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /** Seasons across parks, newest start first, filtered; 400 on a bad park id, 404 on an unknown region. */
  async list(
    query: AdminSeasonQuery,
  ): Promise<{ total: number; seasons: AdminSeasonView[] }> {
    const { region, ...rest } = query;
    if (rest.parkId !== undefined) rest.parkId = requireUuid(rest.parkId);
    let parkIds: string[] | undefined;
    if (region !== undefined) {
      const parks = await this.regions.resolveParks(
        this.regions.region(region),
      );
      parkIds = parks.map(({ park }) => park.id);
    }
    const result = await this.seasons.list({ ...rest, parkIds });
    return {
      total: result.total,
      seasons: result.seasons.map((season) => ({
        ...season,
        park: season.park
          ? {
              id: season.park.id,
              name: resolveCuratedPark(season.park).name,
              slug: season.park.slug,
            }
          : null,
      })),
    };
  }

  /** One season with its park; 404 when there is none. */
  async findOne(id: string): Promise<ParkSeason> {
    return this.seasons.findOne(requireUuid(id));
  }

  /**
   * A new season, audited and published. `publish: false` leaves the
   * publishing to a caller that writes several, as the import does.
   */
  async create(
    parkId: string,
    input: SeasonInput,
    admin: AdminPrincipal,
    options: { publish?: boolean } = {},
  ): Promise<ParkSeason> {
    const season = await this.seasons.create(
      requireUuid(parkId),
      input,
      admin.userId,
    );
    await this.audit.record({
      actor: admin,
      action: "park.season.create",
      entityType: "park_season",
      entityId: season.id,
      entityLabel: `${season.kind} ${season.startDate}`,
      after: {
        kind: season.kind,
        startDate: season.startDate,
        endDate: season.endDate,
      },
      sourceUrl: season.sourceUrl,
      reason: season.note,
    });
    // A season changes the park page, not the ride cards, so only the park's
    // own caches need dropping.
    if (options.publish !== false)
      await this.curation.publish(season.parkId, []);
    return season;
  }

  /** A PATCH: the fields given replace the stored ones, `null` clears; the merged row is checked whole. */
  async update(
    id: string,
    input: Partial<SeasonInput>,
    admin: AdminPrincipal,
  ): Promise<ParkSeason> {
    const before = await this.findOne(id);
    const season = await this.seasons.update(before.id, input, admin.userId);
    await this.audit.record({
      actor: admin,
      action: "park.season.update",
      entityType: "park_season",
      entityId: season.id,
      entityLabel: `${season.kind} ${season.startDate}`,
      before: {
        kind: before.kind,
        startDate: before.startDate,
        endDate: before.endDate,
        status: before.status,
        dates: before.dates,
      },
      after: {
        kind: season.kind,
        startDate: season.startDate,
        endDate: season.endDate,
        status: season.status,
        dates: season.dates,
      },
      sourceUrl: season.sourceUrl,
    });
    await this.curation.publish(season.parkId, []);
    return season;
  }

  /** A hard delete; the audit row keeps the whole season. */
  async remove(id: string, admin: AdminPrincipal): Promise<ParkSeason> {
    const season = await this.seasons.remove(requireUuid(id));
    await this.audit.record({
      actor: admin,
      action: "park.season.delete",
      entityType: "park_season",
      entityId: season.id,
      entityLabel: `${season.kind} ${season.startDate}`,
      // The whole row, not a summary. A season is hand-researched — an outer
      // range plus, for a Walibi-style Halloween, twenty-odd individually
      // picked dates and the source they were read from — and the delete is a
      // hard one that no undo covers. Three fields in the audit row is a
      // record of the fact that something was destroyed; this is a record of
      // what, and enough to type it back in.
      before: {
        parkId: season.parkId,
        kind: season.kind,
        name: season.name,
        startDate: season.startDate,
        endDate: season.endDate,
        dates: season.dates,
        status: season.status,
        separateTicket: season.separateTicket,
        priceFrom: season.priceFrom,
        priceCurrency: season.priceCurrency,
        opensAt: season.opensAt,
        closesAt: season.closesAt,
        attractionIds: season.attractionIds,
        url: season.url,
        sourceUrl: season.sourceUrl,
        note: season.note,
      },
    });
    await this.curation.publish(season.parkId, []);
    return season;
  }

  /**
   * Imports researched seasons: every row checked as the admin's create
   * checks it, its park found, compared with what is on file, and in a dry
   * run (the default) only reported. A written row is a `create` with its own
   * audit row; the parks are published once at the end. One writing import
   * runs at a time, and an account starts at most
   * {@link SEASON_IMPORTS_PER_HOUR} an hour.
   */
  async import(
    rows: SeasonImportRow[],
    options: { dryRun: boolean },
    admin: AdminPrincipal,
  ): Promise<SeasonImportResult> {
    if (rows.length === 0 || rows.length > MAX_SEASON_IMPORT_ROWS) {
      throw new BadRequestException(
        `An import takes 1 to ${MAX_SEASON_IMPORT_ROWS} rows; split the file`,
      );
    }
    const verdict = await checkRateLimit({
      redis: this.redis,
      logger: this.logger,
      label: "Season import limiter",
      key: `admin:season-import:count:${admin.userId ?? admin.email}`,
      max: SEASON_IMPORTS_PER_HOUR,
      windowSeconds: 60 * 60,
    });
    if (!verdict.allowed) {
      throw new HttpException(
        `At most ${SEASON_IMPORTS_PER_HOUR} season imports an hour per account; ` +
          `try again in ${Math.ceil(verdict.retryAfterSeconds / 60)} minutes`,
        HttpStatus.TOO_MANY_REQUESTS,
      );
    }
    const lock = options.dryRun ? null : await this.lock();
    try {
      return await this.run(rows, options.dryRun, admin);
    } finally {
      if (lock) await this.unlock(lock);
    }
  }

  private async run(
    rows: SeasonImportRow[],
    dryRun: boolean,
    admin: AdminPrincipal,
  ): Promise<SeasonImportResult> {
    const results: SeasonImportRowResult[] = [];
    const counts = { create: 0, exists: 0, invalid: 0, failed: 0 };
    const report = (
      index: number,
      outcome: SeasonImportOutcome,
      extra: Omit<SeasonImportRowResult, "row" | "label" | "outcome"> = {},
    ) => {
      results.push({
        row: index + 1,
        label: seasonRowLabel(index, rows[index]),
        outcome,
        ...extra,
      });
      counts[outcome === "created" ? "create" : outcome]++;
    };

    const parkIds = new Map<string, { id: string } | { problem: string }>();
    const checked: CheckedRow[] = [];
    for (const [index, row] of rows.entries()) {
      const problem = seasonRowProblem(row);
      if (problem) {
        report(index, "invalid", { reason: problem });
        continue;
      }
      const key = `${row.region ?? ""}|${row.parkSlug}`;
      const park =
        parkIds.get(key) ?? (await this.parkOf(row.parkSlug, row.region));
      parkIds.set(key, park);
      if ("problem" in park) {
        report(index, "invalid", { reason: park.problem });
        continue;
      }
      const input = seasonRowInput(row);
      const refusal = await this.seasons.validate(park.id, input).then(
        () => null,
        (error: unknown) => {
          const message = clientErrorMessage(error);
          if (message === null) throw error;
          return message;
        },
      );
      if (refusal) {
        report(index, "invalid", { parkId: park.id, reason: refusal });
        continue;
      }
      checked.push({ index, row, parkId: park.id, input });
    }

    const starts = checked.map((c) => c.input.startDate).sort();
    const ends = checked.map((c) => c.input.endDate).sort();
    const known: Array<SeasonOnFile & { row?: number }> =
      checked.length === 0
        ? []
        : await this.seasons.forParksInRange(
            [...new Set(checked.map((c) => c.parkId))],
            starts[0],
            ends[ends.length - 1],
          );

    const written = new Set<string>();
    for (const { index, row, parkId, input } of checked) {
      const same = seasonOnFile(known, parkId, row);
      if (same) {
        report(
          index,
          "exists",
          same.row
            ? { parkId, reason: `repeats row #${same.row} of this import` }
            : { parkId, seasonId: same.id },
        );
        continue;
      }
      if (dryRun) {
        report(index, "create", { parkId });
        known.push({
          id: "",
          parkId,
          kind: input.kind,
          name: input.name ?? null,
          startDate: input.startDate,
          endDate: input.endDate,
          row: index + 1,
        });
        continue;
      }
      try {
        const season = await this.create(parkId, input, admin, {
          publish: false,
        });
        known.push(season);
        written.add(parkId);
        report(index, "created", { parkId, seasonId: season.id });
      } catch (error) {
        const reason = clientErrorMessage(error);
        if (reason === null) {
          this.logger.warn(
            `Season import row ${index + 1} failed: ${(error as Error).message}`,
          );
        }
        report(index, "failed", {
          parkId,
          reason: reason ?? "the write failed on the server",
        });
      }
    }

    await this.curation.publishParks([...written]);
    results.sort((a, b) => a.row - b.row);
    return { dryRun, counts, rows: results };
  }

  /**
   * The park a row's slug means: the trip region's geographic path when a
   * region lists the slug, else the one park carrying the slug.
   */
  private async parkOf(
    slug: string,
    region: string | null | undefined,
  ): Promise<{ id: string } | { problem: string }> {
    const found = regionParksOf(slug, region);
    if ("problem" in found) return found;
    for (const { config } of found.parks) {
      const park = await this.parks.findOne({
        where: {
          continentSlug: config.continent,
          countrySlug: config.country,
          citySlug: config.city,
          slug: config.slug,
        },
        select: { id: true },
      });
      if (park) return { id: park.id };
    }
    const bySlug = await this.parks.find({
      where: { slug },
      select: { id: true },
      take: 2,
    });
    if (bySlug.length !== 1) {
      return {
        problem: `${slug}: ${bySlug.length === 0 ? "no park" : "several parks"} carry this slug; add it to a region or fix the file`,
      };
    }
    return { id: bySlug[0].id };
  }

  /** The import lock's token, or null when Redis is down (an import then runs unlocked). */
  private async lock(): Promise<string | null> {
    const token = randomUUID();
    let answer: string | null;
    try {
      answer = await this.redis.set(
        IMPORT_LOCK_KEY,
        token,
        "EX",
        IMPORT_LOCK_SECONDS,
        "NX",
      );
    } catch (error) {
      this.logger.warn(
        `Season import lock unavailable, running unlocked: ${(error as Error).message}`,
      );
      return null;
    }
    if (answer !== "OK") {
      throw new ConflictException(
        "Another season import is writing; try again when it has finished",
      );
    }
    return token;
  }

  private async unlock(token: string): Promise<void> {
    try {
      if ((await this.redis.get(IMPORT_LOCK_KEY)) === token) {
        await this.redis.del(IMPORT_LOCK_KEY);
      }
    } catch {
      // The lock expires on its own.
    }
  }
}

/** A lowercase uuid, or 400: a bad id must not reach a uuid column as text. */
function requireUuid(id: string): string {
  const lower = id.trim().toLowerCase();
  if (!isCanonicalUuid(lower)) {
    throw new BadRequestException(`${id} is not a uuid`);
  }
  return lower;
}
