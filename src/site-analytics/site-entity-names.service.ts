import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, Repository } from "typeorm";
import {
  frontendAttractionPath,
  frontendParkPath,
  frontendRestaurantsPath,
  frontendShowsPath,
} from "../common/utils/frontend-url.util";
import { isCanonicalUuid } from "../common/utils/uuid-param.util";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";

export type SiteEntityKind = "park" | "attraction" | "show" | "restaurant";

export interface SiteEntityName {
  kind: SiteEntityKind;
  name: string;
  /** Frontend path without a locale; null where a geo slug is missing. */
  url: string | null;
}

/** How long an instance trusts its copy of the park and ride ids. */
const KNOWN_IDS_TTL_MS = 10 * 60 * 1000;

/** Every park's and ride's id, retired ones included. */
export interface KnownEntityIds {
  parks: ReadonlySet<string>;
  attractions: ReadonlySet<string>;
}

/**
 * Names and frontend paths for the ids an event was about, and the ids that
 * exist at all. An event says only the id, so each id is looked up in all
 * four tables, one query per table; ids are unique across them.
 */
@Injectable()
export class SiteEntityNames {
  private readonly logger = new Logger(SiteEntityNames.name);
  private known: KnownEntityIds | null = null;
  private knownAt = 0;
  private loadingKnown: Promise<KnownEntityIds | null> | null = null;

  constructor(
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
    @InjectRepository(Show) private readonly shows: Repository<Show>,
    @InjectRepository(Restaurant)
    private readonly restaurants: Repository<Restaurant>,
  ) {}

  async resolve(ids: string[]): Promise<Map<string, SiteEntityName>> {
    const named = new Map<string, SiteEntityName>();
    // A non-uuid inside `IN (…)` on a uuid column fails the whole statement.
    const valid = [...new Set(ids.filter(isCanonicalUuid))];
    if (valid.length === 0) return named;
    const withPark = { where: { id: In(valid) }, relations: { park: true } };
    const [parks, attractions, shows, restaurants] = await Promise.all([
      this.parks.findBy({ id: In(valid) }),
      this.attractions.find(withPark),
      this.shows.find(withPark),
      this.restaurants.find(withPark),
    ]);
    for (const park of parks) {
      named.set(park.id, {
        kind: "park",
        name: park.name,
        url: frontendParkPath(park),
      });
    }
    for (const attraction of attractions) {
      named.set(attraction.id, {
        kind: "attraction",
        name: attraction.name,
        url: attraction.park
          ? frontendAttractionPath(attraction.park, attraction)
          : null,
      });
    }
    for (const show of shows) {
      named.set(show.id, {
        kind: "show",
        name: show.name,
        url: show.park ? frontendShowsPath(show.park) : null,
      });
    }
    for (const restaurant of restaurants) {
      named.set(restaurant.id, {
        kind: "restaurant",
        name: restaurant.name,
        url: restaurant.park ? frontendRestaurantsPath(restaurant.park) : null,
      });
    }
    return named;
  }

  /**
   * The ids of every park and ride, retired ones included, so a `page_read`
   * can be checked against them before it counts: a made-up id would
   * otherwise be a row in the rankings' table. Ten minutes old at most; the
   * last copy while the database is away, and null before the first one.
   */
  async knownIds(now = Date.now()): Promise<KnownEntityIds | null> {
    if (this.known && now - this.knownAt < KNOWN_IDS_TTL_MS) return this.known;
    this.loadingKnown ??= Promise.all([
      this.parks.query(`SELECT id FROM parks`),
      this.attractions.query(`SELECT id FROM attractions`),
    ])
      .then(([parks, attractions]: Array<Array<{ id: string }>>) => {
        this.known = {
          parks: new Set(parks.map((row) => row.id)),
          attractions: new Set(attractions.map((row) => row.id)),
        };
        this.knownAt = Date.now();
        return this.known;
      })
      .catch((error: Error) => {
        this.logger.warn(`Park and ride ids unavailable: ${error.message}`);
        return this.known;
      })
      .finally(() => {
        this.loadingKnown = null;
      });
    return this.loadingKnown;
  }
}
