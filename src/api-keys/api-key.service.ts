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

const cacheKey = (hash: string): string => `api-key:${hash}`;
const touchKey = (id: string): string => `api-key:touched:${id}`;
/** One counter per key and clock hour (UTC); it expires shortly after its hour. */
const hourKey = (id: string, hour: number): string =>
  `api-key:hour:${id}:${hour}`;
const currentHour = (now: number): number => Math.floor(now / HOUR_MS);

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

  /** Every key, active ones first, newest first within each group, with this hour's count. */
  async list(): Promise<ApiKeyItem[]> {
    const rows = await this.keys
      .createQueryBuilder("k")
      .orderBy("k.revoked_at IS NULL", "DESC")
      .addOrderBy("k.created_at", "DESC")
      .getMany();
    if (rows.length === 0) return [];
    const hour = currentHour(Date.now());
    const counts = await this.redis.mget(
      ...rows.map((row) => hourKey(row.id, hour)),
    );
    const names = await this.parkNames(
      rows.flatMap((row) => row.allowedParkIds ?? []),
    );
    return rows.map((row, i) => ({
      ...toItem(row),
      requestsThisHour: Number(counts[i] ?? 0),
      allowedParks:
        row.allowedParkIds?.map((id) => ({ id, name: names.get(id) ?? id })) ??
        null,
    }));
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
   * Counts one request against the key's hourly allowance. Null when the key
   * has none. A fixed window per clock hour: simple to explain to a holder
   * ("1,000 an hour, counted from the full hour") and one Redis call.
   */
  async countRequest(
    key: ResolvedApiKey,
    now = Date.now(),
  ): Promise<HourlyUsage | null> {
    if (key.requestsPerHour === null || key.usage === "internal") return null;
    const hour = currentHour(now);
    const counter = hourKey(key.id, hour);
    let count: number;
    try {
      // One round trip; the expiry is set with the first increment or not at
      // all, so a counter never outlives its hour (EXPIRE NX, Redis 7+).
      const replies = await this.redis
        .multi()
        .incr(counter)
        .expire(counter, 3_700, "NX")
        .exec();
      count = Number(replies?.[0]?.[1]);
      if (!Number.isFinite(count)) return null;
    } catch (error: unknown) {
      // Fail open, like every limiter here: Redis being down must not turn
      // every keyed forecast into an error.
      this.logger.warn(`Could not count API key ${key.id}: ${String(error)}`);
      return null;
    }
    return {
      limit: key.requestsPerHour,
      remaining: Math.max(0, key.requestsPerHour - count),
      resetSeconds: Math.ceil(((hour + 1) * HOUR_MS - now) / 1000),
      exceeded: count > key.requestsPerHour,
    };
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
