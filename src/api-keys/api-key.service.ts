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
}

/** What `update` may change; a field left out stays as it is. */
export interface ApiKeyPatch {
  requestsPerHour?: number | null;
  allowedParkIds?: string[] | null;
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
    createdAt: row.createdAt,
    lastUsedAt: row.lastUsedAt,
    revokedAt: row.revokedAt,
  };
}

/**
 * Issues, lists, revokes and checks the keys that unlock the forecast routes.
 *
 * Every forecast request asks `resolve`, so the answer is cached in Redis both
 * ways: a valid key for five minutes, an unknown or revoked one for one. A
 * revocation deletes the valid entry at once, so the five minutes only ever
 * apply to keys that are still good.
 */
@Injectable()
export class ApiKeyService {
  private readonly logger = new Logger(ApiKeyService.name);

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
    },
    createdBy: string | null,
  ): Promise<{ key: string; item: ApiKeyItem }> {
    const allowedParkIds = await this.knownParkIds(input.allowedParkIds);
    const key = generateToken(API_KEY_PREFIX);
    const row = await this.keys.save(
      this.keys.create({
        name: input.name,
        contact: input.contact || null,
        usage: input.usage ?? "non-commercial",
        note: input.note || null,
        requestsPerHour:
          input.requestsPerHour === undefined
            ? DEFAULT_REQUESTS_PER_HOUR
            : input.requestsPerHour,
        allowedParkIds,
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
   * Changes an active key's hourly allowance and parks; null when there is no
   * such active key. `before` and `after` hold only the fields that were sent.
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
    await this.keys.save(row);
    // The cached lookup carries both, so the change applies at once.
    await this.redis.del(cacheKey(row.keyHash));
    return { before, after, item: toItem(row) };
  }

  /**
   * The given park ids, deduplicated, after checking each names a park; null or
   * an empty list means every park. An unknown id is a 400, because a key
   * limited to a park that does not exist would silently open nothing.
   */
  private async knownParkIds(
    ids: string[] | null | undefined,
  ): Promise<string[] | null> {
    if (!ids || ids.length === 0) return null;
    const unique = [...new Set(ids)];
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
    if (key.requestsPerHour === null) return null;
    const hour = currentHour(now);
    const counter = hourKey(key.id, hour);
    const count = await this.redis.incr(counter);
    if (count === 1) await this.redis.expire(counter, 3_700);
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
    await this.redis.del(cacheKey(row.keyHash));
    return toItem(row);
  }

  /** The active key behind a presented value, or null. */
  async resolve(presented: string): Promise<ResolvedApiKey | null> {
    if (!looksLikeApiKey(presented)) return null;
    const hash = hashToken(presented);

    const cached = await this.redis.get(cacheKey(hash));
    if (cached === INVALID_MARKER) return null;
    if (cached) {
      const resolved = JSON.parse(cached) as ResolvedApiKey;
      resolved.requestsPerHour ??= null;
      resolved.allowedParkIds ??= null;
      this.touch(resolved.id);
      return resolved;
    }

    const row = await this.keys.findOne({
      where: { keyHash: hash, revokedAt: IsNull() },
    });
    if (!row) {
      await this.redis.set(
        cacheKey(hash),
        INVALID_MARKER,
        "EX",
        INVALID_TTL_SECONDS,
      );
      return null;
    }
    const resolved: ResolvedApiKey = {
      id: row.id,
      usage: row.usage,
      requestsPerHour: row.requestsPerHour,
      allowedParkIds: row.allowedParkIds,
    };
    await this.redis.set(
      cacheKey(hash),
      JSON.stringify(resolved),
      "EX",
      VALID_TTL_SECONDS,
    );
    this.touch(row.id);
    return resolved;
  }

  /**
   * Records that a key was used, at most once per interval. Never awaited by
   * the request it came from: a slow write must not delay a forecast.
   */
  private touch(id: string): void {
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
