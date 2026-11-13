import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Redis } from "ioredis";
import { Repository } from "typeorm";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { CacheKeys } from "../common/cache/cache-keys";
import { TRIP_TICKETS_TTL } from "../common/cache/cache-ttl";
import { safeJsonParse } from "../common/utils/json.util";
import { SingleFlight } from "../common/utils/single-flight.util";
import { TRIP_REGIONS, type RegionConfig } from "./regions";
import { TicketProductRecord } from "./entities/ticket-product.entity";
import {
  catalogVersion,
  productOfRow,
  seedCatalog,
  seedRows,
  withCatalog,
  type TicketCatalog,
} from "./ticket-catalog";

/**
 * How long an instance keeps a region's catalog in memory. The instance that
 * writes drops its own at once; the others let go within this.
 */
export const TICKET_CATALOG_MEMORY_MS = 30_000;

/**
 * The one provider of a trip region's ticket catalog: `ticket_products`,
 * cached in memory and Redis, versioned, and handed to every reader on the
 * region object (`withTickets`): the plan, `/when`, the suggestion, the
 * region and tickets routes and Parky. On boot it seeds a region whose table
 * has no row from `tickets.ts`, once. Contract:
 * docs/frontend/trip-regions.md#editing-the-catalog.
 */
@Injectable()
export class TicketCatalogService implements OnApplicationBootstrap {
  private readonly logger = new Logger(TicketCatalogService.name);
  private readonly memory = new Map<
    string,
    { at: number; catalog: TicketCatalog }
  >();
  private readonly flight = new SingleFlight();

  constructor(
    @InjectRepository(TicketProductRecord)
    private readonly rows: Repository<TicketProductRecord>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    for (const region of Object.values(TRIP_REGIONS)) {
      await this.seed(region).catch((err: Error) =>
        this.logger.error(
          `Ticket catalog seed failed for ${region.slug}: ${err.message}`,
        ),
      );
    }
  }

  /**
   * Writes the seed for a region whose table has no row, and says so once.
   * Two instances booting together insert each product once: the unique
   * (region, slug) index makes the second insert a no-op. Returns the rows
   * this call inserted.
   */
  async seed(region: { slug: string }): Promise<number> {
    const existing = await this.rows.count({ where: { region: region.slug } });
    if (existing > 0) return 0;
    const rows = seedRows(region);
    if (rows.length === 0) return 0;
    const result = await this.rows
      .createQueryBuilder()
      .insert()
      .into(TicketProductRecord)
      .values(rows)
      .orIgnore()
      .execute();
    const inserted = Array.isArray(result.raw) ? result.raw.length : 0;
    if (inserted > 0) {
      this.logger.log(
        `Seeded ${inserted} ticket products for ${region.slug} from tickets.ts`,
      );
      await this.evict(region.slug);
    }
    return inserted;
  }

  /** A region's catalog: memory, then Redis, then the table. */
  async catalog(region: { slug: string }): Promise<TicketCatalog> {
    const hit = this.memory.get(region.slug);
    if (hit && Date.now() - hit.at < TICKET_CATALOG_MEMORY_MS) {
      return hit.catalog;
    }
    return this.flight.run(`tickets:${region.slug}`, async () => {
      const key = CacheKeys.tripTickets(region.slug);
      const cached = safeJsonParse<TicketCatalog>(
        await this.redis.get(key).catch(() => null),
      );
      let catalog = cached;
      if (!catalog) {
        const loaded = await this.load(region);
        catalog = loaded.catalog;
        // A fallback to the seed is not what the table says: never kept in Redis.
        if (loaded.fromTable) {
          await this.redis
            .set(key, JSON.stringify(catalog), "EX", TRIP_TICKETS_TTL)
            .catch(() => undefined);
        }
      }
      this.memory.set(region.slug, { at: Date.now(), catalog });
      return catalog;
    });
  }

  /** The region with its catalog on it: what every reader of the catalog takes. */
  async withTickets(region: RegionConfig): Promise<RegionConfig> {
    return withCatalog(region, await this.catalog(region));
  }

  /** Drops a region's catalog from this instance's memory and from Redis. */
  async evict(region: string): Promise<void> {
    this.memory.delete(region);
    await this.redis.del(CacheKeys.tripTickets(region)).catch(() => undefined);
  }

  private async load(region: {
    slug: string;
  }): Promise<{ catalog: TicketCatalog; fromTable: boolean }> {
    try {
      const rows = await this.rows.find({
        where: { region: region.slug },
        order: { sortOrder: "ASC", slug: "ASC" },
      });
      if (rows.length === 0) {
        this.logger.warn(
          `No ticket products for ${region.slug} in the table: planning with the seed`,
        );
        return { catalog: seedCatalog(region), fromTable: false };
      }
      const products = rows.map(productOfRow);
      const stamps = rows
        .map((r) => new Date(r.updatedAt).toISOString())
        .sort();
      const updatedAt = stamps[stamps.length - 1];
      return {
        catalog: {
          region: region.slug,
          version: catalogVersion(products),
          ...(updatedAt && { updatedAt }),
          products,
        },
        fromTable: true,
      };
    } catch (err) {
      this.logger.warn(
        `Ticket catalog for ${region.slug} unreadable, planning with the seed: ${(err as Error).message}`,
      );
      return { catalog: seedCatalog(region), fromTable: false };
    }
  }
}
