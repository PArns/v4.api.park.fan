import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import type { Redis } from "ioredis";
import { IsNull, Repository } from "typeorm";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { generateToken, hashToken } from "../admin/auth/token-hash.util";
import {
  API_KEY_PREFIX,
  API_KEY_VISIBLE_CHARS,
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
  createdAt: Date;
  lastUsedAt: Date | null;
  revokedAt: Date | null;
}

/** The key a request was let through with, as the guard attaches it. */
export interface ResolvedApiKey {
  id: string;
  usage: ApiKeyUsage;
}

/** How long a key that was found valid is believed without asking Postgres. */
const VALID_TTL_SECONDS = 300;
/** How long an unknown or revoked key is refused without asking Postgres. */
const INVALID_TTL_SECONDS = 60;
/** `last_used_at` is written at most this often per key. */
const TOUCH_INTERVAL_SECONDS = 600;
const INVALID_MARKER = "-";

const cacheKey = (hash: string): string => `api-key:${hash}`;
const touchKey = (id: string): string => `api-key:touched:${id}`;

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
    },
    createdBy: string | null,
  ): Promise<{ key: string; item: ApiKeyItem }> {
    const key = generateToken(API_KEY_PREFIX);
    const row = await this.keys.save(
      this.keys.create({
        name: input.name,
        contact: input.contact || null,
        usage: input.usage ?? "non-commercial",
        note: input.note || null,
        prefix: key.slice(0, API_KEY_VISIBLE_CHARS),
        keyHash: hashToken(key),
        createdBy,
      }),
    );
    // A key presented before it existed would have been cached as invalid.
    await this.redis.del(cacheKey(row.keyHash));
    return { key, item: toItem(row) };
  }

  /** Every key, active ones first, newest first within each group. */
  async list(): Promise<ApiKeyItem[]> {
    const rows = await this.keys
      .createQueryBuilder("k")
      .orderBy("k.revoked_at IS NULL", "DESC")
      .addOrderBy("k.created_at", "DESC")
      .getMany();
    return rows.map(toItem);
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
    const resolved: ResolvedApiKey = { id: row.id, usage: row.usage };
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
