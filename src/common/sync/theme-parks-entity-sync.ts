import type { Park } from "../../parks/entities/park.entity";
import type { ParksService } from "../../parks/parks.service";
import type { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import type { EntityResponse } from "../../external-apis/themeparks/themeparks.types";
import { isThemeParksWikiId } from "../utils/external-id.util";
import { generateSlug, generateUniqueSlug } from "../utils/slug.util";

/**
 * What one park's pass keeps in memory before the first child is looked at.
 *
 * Subclasses widen this with whatever their own matching needs; the template
 * only ever touches `usedSlugs`, and it adds every slug it mints so two
 * identically named children in the same response cannot collide.
 */
export interface ParkSyncState {
  /** Slugs already taken inside this park. */
  usedSlugs: Set<string>;
}

/**
 * The park walk `syncAttractions`, `syncShows` and `syncRestaurants` used to
 * carry three times over: load the parks, skip the ones that are not from
 * ThemeParks.wiki, fetch their children, keep the children of one entity type,
 * map each one, decide update-or-insert, mint a slug that is unique inside the
 * park, and write.
 *
 * Only that skeleton lives here. Everything the three genuinely disagree about
 * stays with them:
 *
 * - `filterChildren` — which `entityType` this sync consumes.
 * - `loadParkState` — attractions and shows read one park's rows, restaurants
 *   look their rows up by `externalId` across parks.
 * - `mapChild` — the mapper call, and the one veto (`null` drops the child
 *   without counting it).
 * - `reconcile` — attractions match across sources, the other two by
 *   `externalId`.
 * - `persist` — one `save()` per park for restaurants, `update()`s plus a
 *   batched `save()` for shows, row-by-row writes for attractions.
 *
 * `TUpdate` is whatever `reconcile` hands to `persist`: a patch, or the
 * existing row with the new values already assigned.
 */
export abstract class ThemeParksEntitySync<
  TEntity extends { name: string; slug: string },
  TChild,
  TState extends ParkSyncState,
  TUpdate,
  TOptions = void,
> {
  protected constructor(
    private readonly syncParksService: ParksService,
    private readonly syncThemeParksClient: ThemeParksClient,
  ) {}

  /**
   * Parks whose children list holds nothing for this sync are dropped before
   * `loadParkState` runs. Restaurants need it: their prefetch keys on
   * `In(externalIds)`, and an empty list is not a query worth sending.
   */
  protected readonly skipParksWithoutChildren: boolean = false;

  /** Keep the children this sync is about. */
  protected abstract filterChildren(children: EntityResponse[]): TChild[];

  /** One round of reads per park: existing rows plus the slugs already taken. */
  protected abstract loadParkState(
    park: Park,
    children: TChild[],
  ): Promise<TState>;

  /**
   * Turn one child into a row. Returning `null` drops the child silently and
   * it is not counted — attractions do that for a child without an
   * `externalId`, which has nothing to match on.
   */
  protected abstract mapChild(
    child: TChild,
    parkId: string,
  ): Partial<TEntity> | null;

  /**
   * Decide what this row is. Returning an update means the row exists and
   * `persist` should write this payload; returning `null` means it is new, and
   * the template mints its slug before handing it to `persist`.
   */
  protected abstract reconcile(
    mapped: Partial<TEntity>,
    state: TState,
  ): TUpdate | null;

  /** Write one park's batch. */
  protected abstract persist(
    toInsert: Partial<TEntity>[],
    toUpdate: TUpdate[],
    state: TState,
  ): Promise<void>;

  /**
   * Lets a sync take a park before the wiki path sees it, and report how many
   * entities it synced there. `null` leaves the park to the template.
   * Attractions use it for their Queue-Times and Wartezeiten branches, which
   * read entirely different APIs.
   */
  protected claimPark(park: Park): Promise<number | null> {
    void park;
    return Promise.resolve(null);
  }

  /**
   * The child the mapper should see. Restaurants override it to re-fetch the
   * entity in deep mode; everyone else maps the summary from the children list.
   */
  protected resolveChild(child: TChild, options?: TOptions): Promise<TChild> {
    void options;
    return Promise.resolve(child);
  }

  /**
   * Walk every park and sync this entity type. Returns the number synced.
   *
   * `options` is whatever the public sync method was called with; it is handed
   * to `resolveChild` unchanged rather than parked on the instance, so two
   * runs cannot read each other's flags.
   */
  protected async syncFromThemeParksWiki(options?: TOptions): Promise<number> {
    const parks = await this.syncParksService.ensureParksLoaded();

    let syncedCount = 0;

    for (const park of parks) {
      const claimed = await this.claimPark(park);
      if (claimed !== null) {
        syncedCount += claimed;
        continue;
      }

      // Skip parks that are not from ThemeParks.wiki (e.g. Queue-Times or
      // Wartezeiten) — and parks without an external ID at all, which this
      // API has nothing to answer about.
      if (!isThemeParksWikiId(park.externalId)) {
        continue;
      }

      syncedCount += await this.syncPark(park, options);
    }

    return syncedCount;
  }

  private async syncPark(park: Park, options?: TOptions): Promise<number> {
    const childrenResponse = await this.syncThemeParksClient.getEntityChildren(
      park.externalId,
    );

    const children = this.filterChildren(childrenResponse.children);
    if (children.length === 0 && this.skipParksWithoutChildren) {
      return 0;
    }

    const state = await this.loadParkState(park, children);

    const toInsert: Partial<TEntity>[] = [];
    const toUpdate: TUpdate[] = [];
    let syncedCount = 0;

    for (const child of children) {
      const mapped = this.mapChild(
        await this.resolveChild(child, options),
        park.id,
      );
      if (mapped === null) {
        continue;
      }

      const update = this.reconcile(mapped, state);
      if (update !== null) {
        toUpdate.push(update);
      } else {
        mapped.slug = this.takeUniqueSlug(mapped, state.usedSlugs);
        toInsert.push(mapped);
      }

      syncedCount++;
    }

    await this.persist(toInsert, toUpdate, state);

    return syncedCount;
  }

  /**
   * A slug no other row in this park holds. The mapper's own slug is the
   * preferred base, the name is the fallback, and the result is added to
   * `usedSlugs` so the next child in the same response cannot repeat it.
   */
  private takeUniqueSlug(
    mapped: Partial<TEntity>,
    usedSlugs: Set<string>,
  ): TEntity["slug"] {
    const baseSlug = mapped.slug || generateSlug(mapped.name!);
    const uniqueSlug = generateUniqueSlug(baseSlug, [...usedSlugs]);
    usedSlugs.add(uniqueSlug);
    return uniqueSlug;
  }
}
