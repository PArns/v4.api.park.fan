import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { isApnsConfigured } from "../../push/apns";
import type { DevicePushKind } from "../../push/push-config";
import { PushSubscription } from "../../push/entities/push-subscription.entity";
import { isWithinQuietHours } from "../../push/quiet-hours";
import { PushService, type PushMessage } from "../../push/push.service";

/**
 * The two post exports on media.park.fan, each its own device kind. Both files
 * have one shape, so everything below is written once and reads the feed.
 *
 * `narrowed`: whether the device's `newsParks` applies. News only: a guide is
 * often about several parks or none, so the news rule would keep most guides
 * from a device narrowed to its favourites, and `guides` is its own switch.
 */
export const POST_FEEDS = [
  { kind: "news", folder: "news", path: "/news/", narrowed: true },
  { kind: "guides", folder: "guides", path: "/blog/", narrowed: false },
] as const satisfies readonly {
  kind: DevicePushKind;
  folder: string;
  path: string;
  narrowed: boolean;
}[];
export type PostFeed = (typeof POST_FEEDS)[number];

/** The languages the exports are written in; anything else reads English. */
export const NEWS_LOCALES = ["de", "en", "nl", "fr", "es", "it"] as const;

/** A post older than this is never announced, even when first seen now. */
export const NEWS_MAX_AGE_MS = 6 * 60 * 60_000;

/** The fields of an export post (`<base>/<folder>/<locale>.json`) this reads. */
export interface NewsExportPost {
  slug: string;
  title: string;
  teaser?: string;
  publishedAt: string;
  parks?: string[];
  parkSlugs?: string[];
  /** An image id of the media index, or null. */
  cover?: string | null;
}

/**
 * The line under a post's title, in Parky's voice: who is telling, and for a
 * guide that it is one. Calm, `du`, no promise about the post.
 */
const PARKY_SAYS: Record<PostFeed["kind"], Record<string, string>> = {
  news: {
    de: "Parky hat Neues für dich",
    en: "Parky has news for you",
    nl: "Parky heeft nieuws voor je",
    fr: "Parky a du nouveau pour toi",
    es: "Parky tiene novedades para ti",
    it: "Parky ha novità per te",
  },
  guides: {
    de: "Parky hat einen neuen Guide",
    en: "Parky has a new guide",
    nl: "Parky heeft een nieuwe gids",
    fr: "Parky a un nouveau guide",
    es: "Parky tiene una guía nueva",
    it: "Parky ha una nuova guida",
  },
};

export function newsLocaleOf(locale: string): string {
  const base = locale.split("-")[0]?.toLowerCase() ?? "en";
  return (NEWS_LOCALES as readonly string[]).includes(base) ? base : "en";
}

/** Whether a device's narrowing (`null` = all news) wants this post. */
export function wantsNewsPost(
  newsParks: string[] | null,
  post: NewsExportPost,
): boolean {
  if (newsParks === null) return true;
  const parks = post.parks ?? [];
  if (parks.length > 0) return parks.some((p) => newsParks.includes(p));
  const slugs = post.parkSlugs ?? [];
  return newsParks.some((p) => slugs.includes(p.split("/").pop() ?? ""));
}

/** The notification for one post: the export is per locale, so it is already in the reader's language. */
export function writeNewsMessage(
  post: NewsExportPost,
  feed: PostFeed = POST_FEEDS[0],
  locale = "en",
): PushMessage {
  return {
    title: post.title,
    subtitle: PARKY_SAYS[feed.kind][newsLocaleOf(locale)],
    body: post.teaser ?? "",
    ...(typeof post.cover === "string" ? { photo: post.cover } : {}),
    url: `${feed.path}${post.slug}`,
    tag: `${feed.kind}:${post.slug}`,
    kind: feed.kind,
  };
}

/**
 * The `news` half of the push job: a new post in the news export goes to the
 * devices that switched `news` on, a new guide in the guides export to the
 * devices that switched `guides` on (`POST_FEEDS`). The feeds share nothing but
 * the code: own seen-sets, own tags, so a post and a guide in one run are two
 * notifications.
 *
 * Polled rather than called by the publish pipeline: the export on
 * media.park.fan is what the app itself reads, so a post is announced exactly
 * when the app can open it, and the website's publish flow needs no new call
 * or secret. Five minutes late is fine for news.
 *
 * "New" is a Redis set of slugs per locale with no TTL, so `volatile-lru`
 * never evicts it; `SADD` answering 1 is the claim, so two overlapping ticks
 * cannot both send. A missing set (first run, or a flushed Redis) is filled
 * silently — never a burst of a hundred old posts — and `NEWS_MAX_AGE_MS`
 * keeps a backfilled or re-slugged old post quiet too.
 *
 * Only the slug is compared, so an edit (`updatedAt`) never announces again,
 * and since a slug is never removed from the set, neither does a post that
 * leaves the export and comes back.
 */
@Injectable()
export class NewsPushService {
  private readonly logger = new Logger(NewsPushService.name);
  private static readonly SEND_BATCH_SIZE = 20;

  constructor(
    private readonly pushService: PushService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async run(nowMs: number): Promise<number> {
    if (!isApnsConfigured()) return 0;
    let sent = 0;
    for (const feed of POST_FEEDS) {
      const devices = await this.pushService.devicesWithKind(feed.kind);
      const byLocale = new Map<string, PushSubscription[]>();
      for (const device of devices) {
        const locale = newsLocaleOf(device.locale);
        byLocale.set(locale, [...(byLocale.get(locale) ?? []), device]);
      }
      for (const [locale, readers] of byLocale) {
        try {
          sent += await this.runLocale(feed, locale, readers, nowMs);
        } catch (error) {
          this.logger.warn(
            `${feed.kind} push for ${locale} failed: ${(error as Error)?.message}`,
          );
        }
      }
    }
    return sent;
  }

  private async runLocale(
    feed: PostFeed,
    locale: string,
    readers: PushSubscription[],
    nowMs: number,
  ): Promise<number> {
    const posts = await this.fetchPosts(feed, locale);
    if (posts.length === 0) return 0;
    const key = `push:${feed.kind}:seen:${locale}`;
    // shortcut: the set grows by one slug per post, forever; a few KB a year.
    if (!(await this.redis.exists(key))) {
      await this.redis.sadd(key, ...posts.map((p) => p.slug));
      return 0;
    }

    let sent = 0;
    for (const post of posts) {
      if ((await this.redis.sadd(key, post.slug)) !== 1) continue;
      const age = nowMs - Date.parse(post.publishedAt);
      if (!(age >= 0 && age <= NEWS_MAX_AGE_MS)) continue;

      const message = writeNewsMessage(post, feed, locale);
      const due = readers.filter(
        (d) =>
          (!feed.narrowed || wantsNewsPost(d.newsParks, post)) &&
          !isWithinQuietHours(d.timezone, nowMs),
      );
      for (let i = 0; i < due.length; i += NewsPushService.SEND_BATCH_SIZE) {
        const results = await Promise.all(
          due
            .slice(i, i + NewsPushService.SEND_BATCH_SIZE)
            .map((d) => this.pushService.send(d, message)),
        );
        sent += results.filter(Boolean).length;
      }
    }
    return sent;
  }

  private async fetchPosts(
    feed: PostFeed,
    locale: string,
  ): Promise<NewsExportPost[]> {
    const base = process.env.NEWS_EXPORT_BASE ?? "https://media.park.fan";
    const response = await fetch(`${base}/${feed.folder}/${locale}.json`, {
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok) return [];
    const json = (await response.json()) as { posts?: unknown };
    return Array.isArray(json.posts)
      ? (json.posts as NewsExportPost[]).filter(
          (p) =>
            typeof p?.slug === "string" &&
            typeof p.title === "string" &&
            typeof p.publishedAt === "string",
        )
      : [];
  }
}
