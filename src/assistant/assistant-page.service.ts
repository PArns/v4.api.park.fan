import { Inject, Injectable, Logger } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { ParksService } from "../parks/parks.service";
import { AttractionsService } from "../attractions/attractions.service";
import { AssistantPostsClient } from "./tools/assistant-posts.client";

const CACHE_PREFIX = "assistant:page:";
/** Park, ride and post names change rarely; a renamed one is right again within hours. */
const CACHE_SECONDS = 6 * 60 * 60;
/** A slug that names nothing is asked again after this, not on every message. */
const MISS_SECONDS = 10 * 60;
/** Long enough for two indexed queries or a post fetch; a slower page keeps its slug hint. */
const RESOLVE_MS = 1500;

/** A park's own sub-pages, which sit where a ride's slug would. */
const PARK_CHAPTERS: Record<string, string> = {
  "wait-time-calendar": "wait-time calendar",
  "average-wait-times": "average wait times page",
  "with-kids": "page for a visit with kids",
};

/**
 * The site writes some segments in the reader's language (`/de/glossar`, `/de/beste-reisezeit`,
 * a park's `/durchschnittliche-wartezeiten`), and the widget sends the path as the browser shows
 * it. Each spelling maps to the English segment the tables here use; the frontend keeps the same
 * lists, gathered in its `lib/i18n/localized-path.ts`.
 */
const LOCALIZED_TOP: Record<string, string> = spellings({
  glossary: ["glossar", "glossaire", "glossario", "woordenboek", "glosario"],
  "best-time-to-visit": [
    "beste-reisezeit",
    "meilleure-periode-pour-visiter",
    "periodo-migliore-per-visitare",
    "beste-tijd-om-te-bezoeken",
    "mejor-epoca-para-visitar",
  ],
  "trip-planner": [
    "tagesplaner",
    "planificateur",
    "pianificatore",
    "dagplanner",
    "planificador",
  ],
  "how-park-fan-works": [
    "so-funktioniert-park-fan",
    "comment-fonctionne-park-fan",
    "come-funziona-park-fan",
    "hoe-park-fan-werkt",
    "como-funciona-park-fan",
  ],
});

/** A park's sub-page segments in every language, mapped to the English one. */
const LOCALIZED_CHAPTERS: Record<string, string> = spellings({
  "wait-time-calendar": [
    "wartezeiten-kalender",
    "calendrier-temps-attente",
    "calendario-tempi-attesa",
    "wachttijden-kalender",
    "calendario-tiempos-espera",
  ],
  "average-wait-times": [
    "durchschnittliche-wartezeiten",
    "temps-attente-moyens",
    "tempi-di-attesa-medi",
    "gemiddelde-wachttijden",
    "tiempos-de-espera-medios",
  ],
  "with-kids": [
    "mit-kindern",
    "avec-enfants",
    "con-bambini",
    "met-kinderen",
    "con-ninos",
  ],
});

/** `{ english: [other spellings] }` as one lookup from every spelling to the English one. */
function spellings(families: Record<string, string[]>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(families).flatMap(([english, others]) =>
      [english, ...others].map((spelling) => [spelling, english]),
    ),
  );
}

/** A table's own entry for a segment from the URL, never an inherited one ("constructor"). */
function own(table: Record<string, string>, key: string): string | undefined {
  return Object.prototype.hasOwnProperty.call(table, key)
    ? table[key]
    : undefined;
}

/** Pages without one park, ride or post, by their first segment after the locale. */
const SITE_PAGES: Record<string, string> = {
  "": "the park.fan home page",
  parks: "the list of all parks",
  search: "the search page",
  "trip-planner": "the trip planner",
  favorites: "the user's favorite parks and rides",
  news: "the news index",
  blog: "the blog index",
  glossary: "the glossary index",
  parky: "Parky's own page, about chatting with Parky",
  "best-time-to-visit": "the best-time-to-visit overview",
  compare: "the park comparison",
  "how-park-fan-works": "the page on how park.fan works",
};

/** Brand overviews under `/parks/`, where a continent's slug would be. */
const PARK_GROUPS: Record<string, string> = {
  "six-flags": "the overview of the Six Flags parks",
  universal: "the overview of the Universal parks",
};

/** `/blog/<segment>` pages that list posts instead of showing one. */
const BLOG_LISTS = new Set(["page", "category", "tag", "authors"]);

interface ParkRef {
  continent: string;
  country: string;
  city: string;
  park: string;
}

/** What a park.fan path shows, as far as the path alone tells. */
type Page =
  | { kind: "site"; text: string }
  | { kind: "park"; at: ParkRef; chapter?: string }
  | { kind: "ride"; at: ParkRef; ride: string }
  | { kind: "post"; section: "news" | "blog"; slug: string }
  | { kind: "term"; slug: string };

function parsePage(pagePath: string): Page | null {
  const [, written = "", ...rest] = pagePath.split("/").filter(Boolean);
  const section = own(LOCALIZED_TOP, written) ?? written;
  if (section === "parks") {
    const [continent, country, city, park, next] = rest;
    if (park) {
      const at = { continent, country, city, park };
      if (!next) return { kind: "park", at };
      const chapter = own(PARK_CHAPTERS, own(LOCALIZED_CHAPTERS, next) ?? next);
      return chapter
        ? { kind: "park", at, chapter }
        : { kind: "ride", at, ride: next };
    }
    if (!continent) return { kind: "site", text: SITE_PAGES.parks };
    const group = own(PARK_GROUPS, continent);
    if (!country && group) {
      return { kind: "site", text: group };
    }
    const [scope, slug] = city
      ? ["city", city]
      : country
        ? ["country", country]
        : ["continent", continent];
    return {
      kind: "site",
      text: `the list of parks in the ${scope} with slug "${slug}"`,
    };
  }
  const [slug] = rest;
  if (section === "news" && slug && slug !== "page") {
    return { kind: "post", section, slug };
  }
  if (section === "blog" && slug && !BLOG_LISTS.has(slug)) {
    return { kind: "post", section, slug };
  }
  if (section === "glossary" && slug) return { kind: "term", slug };
  const text = own(SITE_PAGES, section);
  return text ? { kind: "site", text } : null;
}

/** A name from the database or a post, quoted and cut short: it is data, never instructions. */
function quoted(text: string): string {
  return JSON.stringify(text.replace(/\s+/g, " ").trim().slice(0, 120));
}

/** The page's subject from its slugs alone: the hint when nothing better is known. */
export function pageSubject(pagePath: string): string | null {
  const page = parsePage(pagePath);
  if (!page) return null;
  switch (page.kind) {
    case "site":
      return page.text;
    case "park":
    case "ride": {
      const park = `the park with slug "${page.at.park}"`;
      if (page.kind === "ride")
        return `the ride with slug "${page.ride}" in ${park}`;
      return page.chapter ? `the ${page.chapter} of ${park}` : park;
    }
    case "post":
      return postSubject(page.section, null, page.slug);
    case "term":
      return termSubject(null, page.slug);
  }
}

/** A post by its quoted title, or by its slug alone when the title is not known. */
function postSubject(
  section: "news" | "blog",
  title: string | null,
  slug: string,
): string {
  const kind = section === "news" ? "news post" : "article";
  const name = title ? `${title} (slug "${slug}"; ` : `with slug "${slug}" (`;
  return `the ${kind} ${name}read it with get_post)`;
}

/** A glossary term by its quoted name, or by its slug alone. */
function termSubject(name: string | null, slug: string): string {
  const label = name ? `${name} (slug "${slug}"; ` : `with slug "${slug}" (`;
  return `the glossary entry ${label}look it up with look_up_glossary)`;
}

/**
 * Names the page the user is reading, for Parky's per-request context: the
 * park, ride, post or glossary term it shows, by its real name, or what kind of
 * page it is. Names are cached in Redis per park, ride, post or term, so a
 * chat's next message asks no database and no frontend; a lookup that fails,
 * misses or is slow falls back to {@link pageSubject} and never fails the chat.
 */
@Injectable()
export class AssistantPageService {
  private readonly logger = new Logger(AssistantPageService.name);

  constructor(
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly moduleRef: ModuleRef,
  ) {}

  async subject(pagePath: string, locale: string): Promise<string | null> {
    const fallback = pageSubject(pagePath);
    const page = parsePage(pagePath);
    if (!page || page.kind === "site") return fallback;

    let timer: NodeJS.Timeout | undefined;
    const slow = new Promise<null>((resolve) => {
      timer = setTimeout(resolve, RESOLVE_MS, null);
    });
    try {
      const name = await Promise.race([
        this.cachedName(page, locale).catch((error: Error) => {
          this.logger.warn(`Page ${pagePath} not resolved: ${error.message}`);
          return null;
        }),
        slow,
      ]);
      if (!name) return fallback;
      if (page.kind === "park" && page.chapter) {
        return `the ${page.chapter} of ${name}`;
      }
      return name;
    } finally {
      clearTimeout(timer);
    }
  }

  /** The subject's description from the cache, or looked up and cached; "" is a cached miss. */
  private async cachedName(
    page: Exclude<Page, { kind: "site" }>,
    locale: string,
  ): Promise<string | null> {
    const key = `${CACHE_PREFIX}${locale}:${cacheId(page)}`;
    const cached = await this.redis.get(key).catch(() => null);
    if (cached !== null) return cached || null;

    const name = await this.lookUp(page, locale);
    await this.redis
      .set(key, name ?? "", "EX", name ? CACHE_SECONDS : MISS_SECONDS)
      .catch(() => undefined);
    return name;
  }

  private async lookUp(
    page: Exclude<Page, { kind: "site" }>,
    locale: string,
  ): Promise<string | null> {
    const get = <T>(token: new (...args: never[]) => T) =>
      this.moduleRef.get(token, { strict: false });
    switch (page.kind) {
      case "park":
      case "ride": {
        const { continent, country, city, park: slug } = page.at;
        const park = await get(ParksService).findByGeographicPath(
          continent,
          country,
          city,
          slug,
        );
        if (!park) return null;
        const place = [park.city, park.country].filter(Boolean).join(", ");
        const label =
          `the park ${quoted(park.name)}` +
          (place ? ` in ${quoted(place)}` : "") +
          ` (park id ${park.id})`;
        if (page.kind === "park") return label;
        const ride = await get(AttractionsService).findBySlugInPark(
          park.id,
          page.ride,
        );
        return ride
          ? `the ride ${quoted(ride.name)} (ride id ${ride.id}) in ${label}`
          : null;
      }
      case "post": {
        const post = (await get(AssistantPostsClient).post(
          locale,
          page.slug,
        )) as { title?: unknown };
        return typeof post.title === "string" && post.title
          ? postSubject(page.section, quoted(post.title), page.slug)
          : null;
      }
      case "term": {
        const found = (await get(AssistantPostsClient).glossary(
          locale,
          page.slug.replace(/-/g, " "),
        )) as { results?: Array<{ slug?: string; name?: string }> };
        const term = found.results?.find((t) => t.slug === page.slug);
        return term?.name ? termSubject(quoted(term.name), page.slug) : null;
      }
    }
  }
}

/** One cache entry per park, ride, post or term; a park's sub-pages share the park's. */
function cacheId(page: Exclude<Page, { kind: "site" }>): string {
  switch (page.kind) {
    case "park":
    case "ride": {
      const { continent, country, city, park } = page.at;
      const path = `${continent}/${country}/${city}/${park}`;
      return page.kind === "ride"
        ? `ride:${path}/${page.ride}`
        : `park:${path}`;
    }
    case "post":
      return `${page.section}:${page.slug}`;
    case "term":
      return `glossary:${page.slug}`;
  }
}
