import {
  BadRequestException,
  Inject,
  Injectable,
  Logger,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import type { Redis } from "ioredis";
import { IsNull, Repository } from "typeorm";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { safeJsonParse } from "../common/utils/json.util";
import { generateToken, hashToken } from "../admin/auth/token-hash.util";
import {
  API_KEY_PREFIX,
  API_KEY_VISIBLE_CHARS,
  DEFAULT_REQUESTS_PER_HOUR,
  type ApiKeyUsage,
} from "./api-key.constants";
import { ApiKey } from "./entities/api-key.entity";

/** What the admin list shows of a key: everything but the hash. */
export interface ApiKeyItem {
  id: string;
  name: string;
  contact: string | null;
  usage: ApiKeyUsage;
  note: string | null;
  prefix: string;
  /** The hourly allowance; null means only the general per-address limit. */
  requestsPerHour: number | null;
  /** The parks the key may read; null means every park. */
  allowedParkIds: string[] | null;
  /** Those parks by name, for the list (list only). */
  allowedParks?: { id: string; name: string }[] | null;
  /** When the key stops working; null means never. */
  expiresAt: Date | null;
  /** Requests counted in the current clock hour (list only). */
  requestsThisHour?: number;
  /** Requests per UTC day and per month, for the admin's statistics (list only). */
  stats?: ApiKeyUsageStats;
  createdAt: Date;
  lastUsedAt: Date | null;
  revokedAt: Date | null;
}

/** The key a request was let through with, as the guard attaches it. */
export interface ResolvedApiKey {
  id: string;
  usage: ApiKeyUsage;
  requestsPerHour: number | null;
  allowedParkIds: string[] | null;
  /** ISO 8601, so the cached copy reads back the same; null means never. */
  expiresAt: string | null;
}

/** What a presented value turned out to be. */
export type ApiKeyLookup =
  | { status: "active"; key: ResolvedApiKey }
  | { status: "expired"; expiresAt: string }
  | { status: "unknown" };

/** What `update` may change; a field left out stays as it is. */
export interface ApiKeyPatch {
  requestsPerHour?: number | null;
  allowedParkIds?: string[] | null;
  expiresAt?: Date | null;
}

/** One UTC day of a key's use: what it asked for, and how much of that its allowance refused. */
export interface ApiKeyDayUsage {
  /** `YYYY-MM-DD`, UTC. */
  date: string;
  requests: number;
  /** Requests answered 429 because the hourly allowance was used up. */
  limited: number;
}

/** A key's use as the admin list shows it: the last days, oldest first, and the last months. */
export interface ApiKeyUsageStats {
  days: ApiKeyDayUsage[];
  /** `YYYY-MM`, UTC, oldest first. */
  months: { month: string; requests: number }[];
}

/** How many days and months of use the counters keep and the list returns. */
export const USAGE_DAYS = 30;
export const USAGE_MONTHS = 12;

/** Where a key stands against its hourly allowance after one more request. */
export interface HourlyUsage {
  limit: number;
  remaining: number;
  /** Seconds until the next clock hour, when the count starts over. */
  resetSeconds: number;
  exceeded: boolean;
}

/** How long a key that was found valid is believed without asking Postgres. */
const VALID_TTL_SECONDS = 300;
/** How long an unknown or revoked key is refused without asking Postgres. */
const INVALID_TTL_SECONDS = 60;
/** `last_used_at` is written at most this often per key. */
const TOUCH_INTERVAL_SECONDS = 600;
const INVALID_MARKER = "-";

const HOUR_MS = 3_600_000;
const DAY_MS = 86_400_000;
/** A day counter outlives its window by a couple of days, a month counter by a month. */
const DAY_COUNTER_TTL_SECONDS = (USAGE_DAYS + 2) * 86_400;
const MONTH_COUNTER_TTL_SECONDS = (USAGE_MONTHS + 1) * 31 * 86_400;

const cacheKey = (hash: string): string => `api-key:${hash}`;
const touchKey = (id: string): string => `api-key:touched:${id}`;
/** One counter per key and clock hour (UTC); it expires shortly after its hour. */
const hourKey = (id: string, hour: number): string =>
  `api-key:hour:${id}:${hour}`;
const currentHour = (now: number): number => Math.floor(now / HOUR_MS);
/** Per key and UTC day; per key, UTC day and refusal; per key and UTC month. */
const dayKey = (id: string, date: string): string =>
  `api-key:day:${id}:${date}`;
const limitedKey = (id: string, date: string): string =>
  `api-key:day-limited:${id}:${date}`;
const monthKey = (id: string, month: string): string =>
  `api-key:month:${id}:${month}`;
const utcDate = (ms: number): string => new Date(ms).toISOString().slice(0, 10);

/** The last `count` UTC days up to the one `now` falls on, oldest first. */
function lastDays(now: number, count: number): string[] {
  return Array.from({ length: count }, (_, i) =>
    utcDate(now - (count - 1 - i) * DAY_MS),
  );
}

/** The last `count` UTC months up to the one `now` falls on, oldest first. */
function lastMonths(now: number, count: number): string[] {
  const at = new Date(now);
  return Array.from({ length: count }, (_, i) =>
    new Date(
      Date.UTC(at.getUTCFullYear(), at.getUTCMonth() - (count - 1 - i), 1),
    )
      .toISOString()
      .slice(0, 7),
  );
}

/**
 * A data key is the prefix and 43 base64url characters; anything else is
 * refused before it costs a hash, a Redis call or a query.
 */
export function looksLikeApiKey(value: string): boolean {
  return /^pfk_[A-Za-z0-9_-]{43}$/.test(value);
}

function toResolved(row: ApiKey): ResolvedApiKey {
  return {
    id: row.id,
    usage: row.usage,
    requestsPerHour: row.requestsPerHour,
    allowedParkIds: row.allowedParkIds,
    expiresAt: row.expiresAt?.toISOString() ?? null,
  };
}

/** Whether a key's end date has passed. */
export function isExpired(key: ResolvedApiKey, now = Date.now()): boolean {
  return key.expiresAt !== null && Date.parse(key.expiresAt) <= now;
}

/**
 * Refuses an end date that has already passed: a key that would be born
 * expired is a mistake, and taking a key out of use is what revoking is for.
 */
function futureExpiry(expiresAt: Date | null | undefined): Date | null {
  if (!expiresAt) return null;
  if (expiresAt.getTime() <= Date.now()) {
    throw new BadRequestException("expiresAt must be in the future");
  }
  return expiresAt;
}

function toItem(row: ApiKey): ApiKeyItem {
  return {
    id: row.id,
    name: row.name,
    contact: row.contact,
    usage: row.usage,
    note: row.note,
    prefix: row.prefix,
    requestsPerHour: row.requestsPerHour,
    allowedParkIds: row.allowedParkIds,
    expiresAt: row.expiresAt,
    createdAt: row.createdAt,
    lastUsedAt: row.lastUsedAt,
    revokedAt: row.revokedAt,
  };
}

/**
 * Issues, lists, revokes and checks the keys that unlock the forecast routes.
 *
 * Every forecast request asks `lookup`, so the answer is cached in Redis both
 * ways: a found key for five minutes, an unknown one for one. A revocation or
 * an edit writes over the cached entry at once, and an end date is compared on
 * every read, so the five minutes never keep a key alive past either.
 */
@Injectable()
export class ApiKeyService {
  private readonly logger = new Logger(ApiKeyService.name);
  /** When this process last recorded each key's use. */
  private readonly touchedAt = new Map<string, number>();

  constructor(
    @InjectRepository(ApiKey) private readonly keys: Repository<ApiKey>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /** Issues a key. The key is in the result and nowhere else; only its hash is stored. */
  async create(
    input: {
      name: string;
      contact?: string | null;
      usage?: ApiKeyUsage;
      note?: string | null;
      requestsPerHour?: number | null;
      allowedParkIds?: string[] | null;
      expiresAt?: Date | null;
    },
    createdBy: string | null,
  ): Promise<{ key: string; item: ApiKeyItem }> {
    const expiresAt = futureExpiry(input.expiresAt);
    const allowedParkIds = await this.knownParkIds(input.allowedParkIds);
    const key = generateToken(API_KEY_PREFIX);
    const row = await this.keys.save(
      this.keys.create({
        name: input.name,
        contact: input.contact || null,
        usage: input.usage ?? "non-commercial",
        note: input.note || null,
        requestsPerHour:
          input.requestsPerHour !== undefined
            ? input.requestsPerHour
            : input.usage === "internal"
              ? null
              : DEFAULT_REQUESTS_PER_HOUR,
        allowedParkIds,
        expiresAt,
        prefix: key.slice(0, API_KEY_VISIBLE_CHARS),
        keyHash: hashToken(key),
        createdBy,
      }),
    );
    // A key presented before it existed would have been cached as invalid.
    await this.redis.del(cacheKey(row.keyHash));
    return { key, item: toItem(row) };
  }

  /**
   * Every key, active ones first, newest first within each group, with this
   * hour's count and its use over the last days and months. One MGET for all
   * of it; a Redis failure shows zeros rather than no list.
   */
  async list(now = Date.now()): Promise<ApiKeyItem[]> {
    const rows = await this.keys
      .createQueryBuilder("k")
      .orderBy("k.revoked_at IS NULL", "DESC")
      .addOrderBy("k.created_at", "DESC")
      .getMany();
    if (rows.length === 0) return [];
    const hour = currentHour(now);
    const days = lastDays(now, USAGE_DAYS);
    const months = lastMonths(now, USAGE_MONTHS);
    const perKey = 1 + 2 * days.length + months.length;
    const counts = await this.redis
      .mget(
        ...rows.flatMap((row) => [
          hourKey(row.id, hour),
          ...days.map((date) => dayKey(row.id, date)),
          ...days.map((date) => limitedKey(row.id, date)),
          ...months.map((month) => monthKey(row.id, month)),
        ]),
      )
      .catch(() => [] as (string | null)[]);
    const names = await this.parkNames(
      rows.flatMap((row) => row.allowedParkIds ?? []),
    );
    return rows.map((row, i) => {
      const at = (offset: number) => Number(counts[i * perKey + offset] ?? 0);
      return {
        ...toItem(row),
        requestsThisHour: at(0),
        stats: {
          days: days.map((date, d) => ({
            date,
            requests: at(1 + d),
            limited: at(1 + days.length + d),
          })),
          months: months.map((month, m) => ({
            month,
            requests: at(1 + 2 * days.length + m),
          })),
        },
        allowedParks:
          row.allowedParkIds?.map((id) => ({
            id,
            name: names.get(id) ?? id,
          })) ?? null,
      };
    });
  }

  /**
   * Changes an active key's hourly allowance, parks and end date; null when
   * there is no such key or it was revoked. An expired key counts as active
   * here, so a new end date brings it back. `before` and `after` hold only the
   * fields that were sent.
   */
  async update(
    id: string,
    patch: ApiKeyPatch,
  ): Promise<{
    before: ApiKeyPatch;
    after: ApiKeyPatch;
    item: ApiKeyItem;
  } | null> {
    const row = await this.keys.findOne({ where: { id, revokedAt: IsNull() } });
    if (!row) return null;
    const before: ApiKeyPatch = {};
    const after: ApiKeyPatch = {};
    if (patch.requestsPerHour !== undefined) {
      before.requestsPerHour = row.requestsPerHour;
      row.requestsPerHour = patch.requestsPerHour;
      after.requestsPerHour = row.requestsPerHour;
    }
    if (patch.allowedParkIds !== undefined) {
      before.allowedParkIds = row.allowedParkIds;
      row.allowedParkIds = await this.knownParkIds(patch.allowedParkIds);
      after.allowedParkIds = row.allowedParkIds;
    }
    if (patch.expiresAt !== undefined) {
      before.expiresAt = row.expiresAt;
      row.expiresAt = futureExpiry(patch.expiresAt);
      after.expiresAt = row.expiresAt;
    }
    await this.keys.save(row);
    // Written over the cached lookup rather than deleted: a resolve that read
    // the old row before this save could otherwise put it back for five
    // minutes. `resolve` stores only where nothing is cached (NX).
    await this.cacheValid(row.keyHash, toResolved(row)).catch(() =>
      this.redis.del(cacheKey(row.keyHash)),
    );
    return { before, after, item: toItem(row) };
  }

  /**
   * The given park ids, lowercased and deduplicated, after checking each names
   * a park; null means every park. An unknown id is a 400, because a key
   * limited to a park that does not exist would silently open nothing. An
   * empty list never reaches here: the DTOs refuse it, so removing a key's
   * last park cannot quietly open every park.
   */
  private async knownParkIds(
    ids: string[] | null | undefined,
  ): Promise<string[] | null> {
    if (!ids) return null;
    const unique = [...new Set(ids.map((id) => id.toLowerCase()))];
    const names = await this.parkNames(unique);
    const unknown = unique.filter((id) => !names.has(id));
    if (unknown.length > 0) {
      throw new BadRequestException(`Unknown park ids: ${unknown.join(", ")}`);
    }
    return unique;
  }

  /** Park names by id, for the ids that exist. */
  private async parkNames(ids: string[]): Promise<Map<string, string>> {
    if (ids.length === 0) return new Map();
    const rows = (await this.keys.query(
      `SELECT id, name FROM parks WHERE id = ANY($1::uuid[])`,
      [[...new Set(ids)]],
    )) as { id: string; name: string }[];
    return new Map(rows.map((row) => [row.id, row.name]));
  }

  /**
   * Counts one request of the key, per clock hour for its allowance and per
   * UTC day and month for the statistics, and says where it stands against
   * the allowance; null when the key has none (`internal` never does), though
   * its use is counted all the same. A fixed window per clock hour: simple to
   * explain to a holder ("1,000 an hour, counted from the full hour") and one
   * Redis round trip.
   */
  async countRequest(
    key: ResolvedApiKey,
    now = Date.now(),
  ): Promise<HourlyUsage | null> {
    const hour = currentHour(now);
    const date = utcDate(now);
    const counter = hourKey(key.id, hour);
    let count: number;
    try {
      // Each expiry is set with the first increment or not at all, so no
      // counter outlives its window (EXPIRE NX, Redis 7+).
      const replies = await this.redis
        .multi()
        .incr(counter)
        .expire(counter, 3_700, "NX")
        .incr(dayKey(key.id, date))
        .expire(dayKey(key.id, date), DAY_COUNTER_TTL_SECONDS, "NX")
        .incr(monthKey(key.id, date.slice(0, 7)))
        .expire(
          monthKey(key.id, date.slice(0, 7)),
          MONTH_COUNTER_TTL_SECONDS,
          "NX",
        )
        .exec();
      count = Number(replies?.[0]?.[1]);
      if (!Number.isFinite(count)) return null;
    } catch (error: unknown) {
      // Fail open, like every limiter here: Redis being down must not turn
      // every keyed forecast into an error.
      this.logger.warn(`Could not count API key ${key.id}: ${String(error)}`);
      return null;
    }
    if (key.requestsPerHour === null || key.usage === "internal") return null;
    const exceeded = count > key.requestsPerHour;
    if (exceeded) this.countLimited(key.id, date);
    return {
      limit: key.requestsPerHour,
      remaining: Math.max(0, key.requestsPerHour - count),
      resetSeconds: Math.ceil(((hour + 1) * HOUR_MS - now) / 1000),
      exceeded,
    };
  }

  /** Records a request the allowance refused. Not awaited: a statistic must not delay a 429. */
  private countLimited(id: string, date: string): void {
    void this.redis
      .multi()
      .incr(limitedKey(id, date))
      .expire(limitedKey(id, date), DAY_COUNTER_TTL_SECONDS, "NX")
      .exec()
      .catch(() => undefined);
  }

  /** Revokes an active key; null when there is no such active key. */
  async revoke(id: string): Promise<ApiKeyItem | null> {
    const row = await this.keys.findOne({ where: { id, revokedAt: IsNull() } });
    if (!row) return null;
    row.revokedAt = new Date();
    await this.keys.save(row);
    // The refusal is written, not the entry deleted: a resolve that read the
    // still-active row a moment earlier stores only where nothing is (NX), so
    // it cannot bring the key back for the next five minutes.
    await this.redis.set(
      cacheKey(row.keyHash),
      INVALID_MARKER,
      "EX",
      VALID_TTL_SECONDS,
    );
    return toItem(row);
  }

  /** The active key behind a presented value, or null: unknown, revoked or expired. */
  async resolve(
    presented: string,
    now = Date.now(),
  ): Promise<ResolvedApiKey | null> {
    const found = await this.lookup(presented, now);
    return found.status === "active" ? found.key : null;
  }

  /**
   * What a presented value is: an active key, a key past its end date (so the
   * refusal can say so), or nothing usable. The end date travels in the cached
   * entry and is compared on every read, so a key stops at its date, not up to
   * five minutes later.
   */
  async lookup(presented: string, now = Date.now()): Promise<ApiKeyLookup> {
    if (!looksLikeApiKey(presented)) return { status: "unknown" };
    const hash = hashToken(presented);

    // A Redis failure reads as a miss: the key is then checked in Postgres.
    const cached = await this.redis.get(cacheKey(hash)).catch(() => null);
    if (cached === INVALID_MARKER) return { status: "unknown" };
    let resolved = safeJsonParse<ResolvedApiKey>(cached);
    if (resolved) {
      resolved.requestsPerHour ??= null;
      resolved.allowedParkIds ??= null;
      resolved.expiresAt ??= null;
    } else {
      const row = await this.keys.findOne({
        where: { keyHash: hash, revokedAt: IsNull() },
      });
      if (!row) {
        await this.redis
          .set(cacheKey(hash), INVALID_MARKER, "EX", INVALID_TTL_SECONDS, "NX")
          .catch(() => undefined);
        return { status: "unknown" };
      }
      resolved = toResolved(row);
      await this.redis
        .set(
          cacheKey(hash),
          JSON.stringify(resolved),
          "EX",
          VALID_TTL_SECONDS,
          "NX",
        )
        .catch(() => undefined);
    }
    if (isExpired(resolved, now)) {
      return { status: "expired", expiresAt: resolved.expiresAt as string };
    }
    this.touch(resolved.id);
    return { status: "active", key: resolved };
  }

  /** Overwrites the cached lookup of a key with its current state. */
  private async cacheValid(
    hash: string,
    resolved: ResolvedApiKey,
  ): Promise<void> {
    await this.redis.set(
      cacheKey(hash),
      JSON.stringify(resolved),
      "EX",
      VALID_TTL_SECONDS,
    );
  }

  /**
   * Records that a key was used, at most once per interval. Never awaited by
   * the request it came from: a slow write must not delay a forecast.
   */
  private touch(id: string): void {
    // Asked in-process first, so a busy key does not send a Redis write with
    // every request; the Redis NX below keeps several processes to one write.
    const now = Date.now();
    if ((this.touchedAt.get(id) ?? 0) > now - TOUCH_INTERVAL_SECONDS * 1000) {
      return;
    }
    this.touchedAt.set(id, now);
    void this.redis
      .set(touchKey(id), "1", "EX", TOUCH_INTERVAL_SECONDS, "NX")
      .then((claimed) =>
        claimed === "OK"
          ? this.keys.update({ id }, { lastUsedAt: new Date() })
          : undefined,
      )
      .catch((error: unknown) =>
        this.logger.warn(
          `Could not record use of API key ${id}: ${String(error)}`,
        ),
      );
  }
}
