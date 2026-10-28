import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { utcDay } from "../common/utils/reader-hash.util";
import { SiteBlockedEvent } from "./entities/site-blocked-event.entity";
import { SITE_ANALYTICS_KEYS as KEYS } from "./site-analytics.constants";

/** How long an instance trusts its copy of the block list. */
const BLOCKLIST_TTL_MS = 60 * 1000;

/**
 * Event names the admin stopped recording. The collector asks on every
 * request, so each instance keeps a copy for a minute; a change made on one
 * instance reaches the others within that minute.
 */
@Injectable()
export class SiteAnalyticsBlocklist {
  private readonly logger = new Logger(SiteAnalyticsBlocklist.name);
  private cached: Set<string> = new Set();
  private loadedAt = 0;
  private loading: Promise<Set<string>> | null = null;

  constructor(
    @InjectRepository(SiteBlockedEvent)
    private readonly blocked: Repository<SiteBlockedEvent>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /** The blocked names; the last known list when the database is away. */
  async names(now = Date.now()): Promise<ReadonlySet<string>> {
    if (now - this.loadedAt < BLOCKLIST_TTL_MS) return this.cached;
    this.loading ??= this.blocked
      .query(`SELECT name FROM site_blocked_events`)
      .then((rows: Array<{ name: string }>) => {
        this.cached = new Set(rows.map((row) => row.name));
        this.loadedAt = Date.now();
        return this.cached;
      })
      .catch((error: Error) => {
        this.logger.warn(`Block list unavailable: ${error.message}`);
        // Back off for the TTL; a down database must not stall every request.
        this.loadedAt = Date.now();
        return this.cached;
      })
      .finally(() => {
        this.loading = null;
      });
    return this.loading;
  }

  /** Every blocked name, newest first. */
  async list(): Promise<
    Array<{ name: string; blockedAt: string; blockedBy: string }>
  > {
    const rows = await this.blocked.find({ order: { blockedAt: "DESC" } });
    return rows.map((row) => ({
      name: row.name,
      blockedAt: new Date(row.blockedAt).toISOString(),
      blockedBy: row.blockedBy,
    }));
  }

  /**
   * Blocks a name; blocking it twice keeps the first entry. The name also
   * leaves the day's names guard and its combinations go, so the place it
   * took is free for a real event at once: a script's names, blocked, stop
   * crowding out the site's own.
   */
  async block(name: string, by: string, now = new Date()): Promise<void> {
    await this.blocked.query(
      `INSERT INTO site_blocked_events (name, blocked_by) VALUES ($1, $2)
       ON CONFLICT (name) DO NOTHING`,
      [name, by],
    );
    this.loadedAt = 0;
    const day = utcDay(now);
    try {
      await this.redis
        .pipeline()
        .srem(KEYS.guard("names", day), name)
        .del(KEYS.guard("combinations", day, name))
        .exec();
    } catch (error) {
      this.logger.warn(
        `Blocked ${name}, but its guard entries stay: ${(error as Error).message}`,
      );
    }
  }

  async unblock(name: string): Promise<void> {
    await this.blocked.delete({ name });
    this.loadedAt = 0;
  }
}
