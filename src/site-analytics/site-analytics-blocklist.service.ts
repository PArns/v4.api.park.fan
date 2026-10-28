import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { SiteBlockedEvent } from "./entities/site-blocked-event.entity";

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

  /** Blocks a name; blocking it twice keeps the first entry. */
  async block(name: string, by: string): Promise<void> {
    await this.blocked.query(
      `INSERT INTO site_blocked_events (name, blocked_by) VALUES ($1, $2)
       ON CONFLICT (name) DO NOTHING`,
      [name, by],
    );
    this.loadedAt = 0;
  }

  async unblock(name: string): Promise<void> {
    await this.blocked.delete({ name });
    this.loadedAt = 0;
  }
}
