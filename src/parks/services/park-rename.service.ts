import { Inject, Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
import { generateSlug } from "../../common/utils/slug.util";
import { ParkSlugAlias } from "../entities/park-slug-alias.entity";
import { Park } from "../entities/park.entity";

/** The four slugs that make up a park's public URL. */
export interface ParkGeoPath {
  continentSlug: string;
  countrySlug: string;
  citySlug: string;
  slug: string;
}

/** Snapshot of a park's current path, taken BEFORE any slug is reassigned. */
export function captureParkPath(park: Park): ParkGeoPath | null {
  if (
    !park.continentSlug ||
    !park.countrySlug ||
    !park.citySlug ||
    !park.slug
  ) {
    return null;
  }
  return {
    continentSlug: park.continentSlug,
    countrySlug: park.countrySlug,
    citySlug: park.citySlug,
    slug: park.slug,
  };
}

function samePath(a: ParkGeoPath, b: ParkGeoPath): boolean {
  return (
    a.continentSlug === b.continentSlug &&
    a.countrySlug === b.countrySlug &&
    a.citySlug === b.citySlug &&
    a.slug === b.slug
  );
}

/**
 * Keeps a park reachable — and its caches honest — across renames.
 *
 * The metadata processor rewrites `park.slug` (and the geo slugs) whenever an upstream source
 * reports a different name. Two things have to happen when that lands, and neither did before:
 *
 * 1. **Bust the caches.** The discovery geo skeleton is cached for 24 h and is what the frontend
 *    builds every park link, redirect and sitemap entry from. A rename left it advertising the
 *    OLD slug, so for up to a day the API's own structure endpoint handed out URLs that its park
 *    endpoint answered with 404. `invalidateParkCaches` already evicts exactly this key — it was
 *    just never called from the rename path (only from merge/repair).
 *
 * 2. **Remember the old path.** Even with a fresh cache the previously published URL stays dead,
 *    dropping the page's search ranking and breaking inbound links. Recording it lets the park
 *    lookup answer with a 301 to the current path instead.
 *
 * Call {@link handlePathChange} after saving a park whose slugs may have changed; it is a no-op
 * when the path is unchanged, so call sites don't need to compare first.
 */
/**
 * Renames that already happened before this service existed, and whose old paths are therefore
 * not recorded anywhere. Without these rows those URLs stay 404 forever — they are live pages
 * with accumulated search ranking (Magic Kingdom and Hollywood Studios are among the busiest on
 * the site) and they are still what our published sitemap points at.
 *
 * Each entry is the path the park WAS reachable at, keyed by the slug it answers on today.
 * Seeded once at startup and idempotent; safe to delete once the redirects are established.
 */
const HISTORICAL_PARK_PATHS: ReadonlyArray<
  ParkGeoPath & { currentSlug: string }
> = [
  {
    continentSlug: "europe",
    countrySlug: "netherlands",
    citySlug: "sevenum",
    slug: "attractiepark-toverland",
    currentSlug: "toverland",
  },
  {
    continentSlug: "north-america",
    countrySlug: "united-states",
    citySlug: "orlando",
    slug: "magic-kingdom-park",
    currentSlug: "disney-magic-kingdom",
  },
  {
    continentSlug: "north-america",
    countrySlug: "united-states",
    citySlug: "orlando",
    slug: "disneys-hollywood-studios",
    currentSlug: "disney-hollywood-studios",
  },
];

@Injectable()
export class ParkRenameService implements OnModuleInit {
  private readonly logger = new Logger(ParkRenameService.name);

  constructor(
    @InjectRepository(ParkSlugAlias)
    private readonly aliasRepository: Repository<ParkSlugAlias>,
    @Inject(REDIS_CLIENT)
    private readonly redis: Redis,
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    private readonly revalidation: RevalidationService,
  ) {}

  /** Seeds {@link HISTORICAL_PARK_PATHS}. Idempotent; failures are logged, never fatal. */
  async onModuleInit(): Promise<void> {
    for (const entry of HISTORICAL_PARK_PATHS) {
      const { currentSlug, ...previous } = entry;
      try {
        const park = await this.parkRepository.findOne({
          where: {
            continentSlug: previous.continentSlug,
            countrySlug: previous.countrySlug,
            citySlug: previous.citySlug,
            slug: currentSlug,
          },
          select: ["id"],
        });
        // The park may have been renamed again, or not exist in this environment.
        if (!park) continue;

        await this.aliasRepository
          .createQueryBuilder()
          .insert()
          .into(ParkSlugAlias)
          .values({ parkId: park.id, ...previous })
          .orIgnore()
          .execute();
      } catch (error) {
        this.logger.warn(
          `Could not seed historical path ${previous.slug}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
  }

  /**
   * Records `previous` as an alias of the park's current path and evicts the park-scoped and
   * discovery caches. No-op when the path did not actually change.
   *
   * Never throws: a rename must not fail because bookkeeping did.
   */
  async handlePathChange(
    park: Park,
    previous: ParkGeoPath | null,
  ): Promise<void> {
    const current = captureParkPath(park);
    if (!previous || !current || samePath(previous, current)) return;

    this.logger.log(
      `Park path changed for "${park.name}": ` +
        `${previous.continentSlug}/${previous.countrySlug}/${previous.citySlug}/${previous.slug} → ` +
        `${current.continentSlug}/${current.countrySlug}/${current.citySlug}/${current.slug}`,
    );

    try {
      // The new path may itself be a recorded alias from an earlier rename that got reverted
      // (upstream names do flip back). Drop that row first so a park never aliases to itself.
      await this.aliasRepository.delete({
        continentSlug: current.continentSlug,
        countrySlug: current.countrySlug,
        citySlug: current.citySlug,
        slug: current.slug,
      });

      // `orIgnore` keeps a re-run idempotent and tolerates the unique index when two parks
      // have swapped paths; the existing row already points somewhere valid.
      await this.aliasRepository
        .createQueryBuilder()
        .insert()
        .into(ParkSlugAlias)
        .values({ parkId: park.id, ...previous })
        .orIgnore()
        .execute();
    } catch (error) {
      this.logger.warn(
        `Could not record slug alias for park ${park.id}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }

    try {
      // Without this the geo skeleton keeps advertising `previous` for up to 24 h.
      await invalidateParkCaches(this.redis, park.id);
    } catch (error) {
      this.logger.warn(
        `Could not invalidate caches after renaming park ${park.id}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }

    // Evicting our own Redis only fixes what WE serve. The frontend caches the geo skeleton and
    // the attraction sitemap under the `geo` tag for 24 h of its own, so without this webhook a
    // rename keeps being published at the dead path for another day — which is exactly how
    // sitemap-attractions.xml ended up advertising 546 URLs under `attractiepark-toverland`,
    // `magic-kingdom-park` and `disneys-hollywood-studios` days after those slugs stopped
    // resolving. `geo` covers the structure + sitemap fetches, `parks`/`attractions` the page
    // shells that embed the park's links.
    // (revalidateTags is already best-effort internally; the guard keeps this method's
    // "never throws" contract even if the client ever changes.)
    try {
      await this.revalidation.revalidateTags(["geo", "parks", "attractions"]);
    } catch (error) {
      this.logger.warn(
        `Could not revalidate the frontend after renaming park ${park.id}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  /**
   * Current canonical path for a previously used one, or `null` when the path was never a park's.
   * Returns `null` if the alias points at a park that has since been deleted.
   */
  /**
   * Corrects a park's real-world location.
   *
   * Used when a geocode failed badly enough to put a park on the wrong
   * continent — Universal Studios Hollywood sat at "Bull Creek" 28.0/-81.0,
   * smooth-rounded coordinates in Florida for a park in California. Merging
   * kept that row because it holds the wiki entity id and the destination
   * link, so the location is fixed here rather than by discarding the row.
   *
   * Routes through {@link handlePathChange}, so a changed citySlug records an
   * alias for the old path and tells the frontend to rebuild. Coordinate-only
   * corrections leave the path untouched and record nothing.
   */
  async correctLocation(
    parkId: string,
    location: {
      city?: string;
      citySlug?: string;
      latitude?: number;
      longitude?: number;
    },
  ): Promise<{ park: Park; pathChanged: boolean }> {
    const park = await this.parkRepository.findOne({ where: { id: parkId } });
    if (!park) {
      throw new Error(`Park not found: ${parkId}`);
    }

    const previous = captureParkPath(park);

    if (location.city !== undefined) {
      park.city = location.city;
      park.citySlug = location.citySlug ?? generateSlug(location.city);
    } else if (location.citySlug !== undefined) {
      park.citySlug = location.citySlug;
    }
    if (location.latitude !== undefined) park.latitude = location.latitude;
    if (location.longitude !== undefined) park.longitude = location.longitude;

    await this.parkRepository.save(park);

    const current = captureParkPath(park);
    const pathChanged = !!previous && !!current && !samePath(previous, current);

    await this.handlePathChange(park, previous);

    this.logger.log(
      `Corrected location for "${park.name}": ${park.city} (${park.latitude}/${park.longitude})` +
        (pathChanged ? " — path changed, alias recorded" : ""),
    );

    return { park, pathChanged };
  }

  async resolveAlias(path: ParkGeoPath): Promise<ParkGeoPath | null> {
    const alias = await this.aliasRepository.findOne({
      where: path,
      relations: ["park"],
    });
    if (!alias?.park) return null;

    const current = captureParkPath(alias.park);
    if (!current || samePath(current, path)) return null;
    return current;
  }
}
