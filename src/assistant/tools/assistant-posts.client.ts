import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { getFrontendUrl } from "../../config/oauth.config";

const CACHE_PREFIX = "assistant:posts:";
const CACHE_SECONDS = 10 * 60;
const TIMEOUT_MS = 5000;

/** A post as the frontend's post search returns it. */
export interface FoundPost {
  title: string;
  description: string | null;
  date: string;
  kind: "news" | "article";
  slug: string;
  url: string;
  /** The card's fields; a frontend from before the post cards sends none of them. */
  updated?: boolean;
  readingTimeMinutes?: number;
  category?: string | null;
  image?: string | null;
  imagePosition?: string | null;
}

/** A glossary term as the frontend's glossary search returns it. */
export interface FoundTerm {
  name: string;
  slug: string;
  shortDefinition: string;
  category?: string;
  /** The term's canonical page; a frontend from before the glossary cards sends none. */
  url?: string;
}

/**
 * park.fan's blog and news, which live as files in the frontend and nowhere
 * in this API. The frontend answers two small read-only routes for Parky:
 * a post search and one post's text. Answers are cached for ten minutes, so
 * a popular question does not reach the frontend per chat.
 */
@Injectable()
export class AssistantPostsClient {
  private readonly logger = new Logger(AssistantPostsClient.name);

  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  /** Up to six posts matching `query`, best first; with `slugs`, exactly those, in that order. */
  search(
    locale: string,
    query: string,
    kind: "news" | "article" | "any",
    slugs?: readonly string[],
  ): Promise<unknown> {
    return this.get("assistant-posts", {
      locale,
      q: query,
      kind,
      ...(slugs?.length ? { slugs: slugs.join(",") } : {}),
    });
  }

  /** With a question, the frontend sends the intro and the sections that answer it best. */
  post(locale: string, slug: string, question?: string): Promise<unknown> {
    return this.get("assistant-post", {
      locale,
      slug,
      ...(question ? { q: question } : {}),
    });
  }

  /**
   * Up to five glossary terms for a query, with their short definitions. `deep` also searches the
   * long definitions, where a term named in English in every language carries the reader's word
   * ("Blockbremse" for "Block Brake"); the site's search palette leaves that pass out.
   */
  glossary(locale: string, query: string): Promise<unknown> {
    return this.get("glossary-search", { locale, q: query, deep: "1" });
  }

  private async get(
    route: string,
    params: Record<string, string>,
  ): Promise<unknown> {
    const search = new URLSearchParams(params).toString();
    const cacheKey = `${CACHE_PREFIX}${route}:${search}`;
    const cached = await this.redis.get(cacheKey).catch(() => null);
    if (cached) return JSON.parse(cached) as unknown;

    const response = await fetch(`${getFrontendUrl()}/api/${route}?${search}`, {
      signal: AbortSignal.timeout(TIMEOUT_MS),
      // Named, so park.fan's Cloudflare rules and logs can tell these reads apart; the three
      // routes need a skip rule there for the API server (docs/frontend/assistant.md).
      headers: { "User-Agent": "api.park.fan-assistant" },
    });
    if (response.status === 404) return { found: false };
    if (!response.ok) {
      this.logger.warn(`${route} answered ${response.status}`);
      throw new Error(`${route} answered ${response.status}`);
    }
    const body = (await response.json()) as unknown;
    await this.redis
      .set(cacheKey, JSON.stringify(body), "EX", CACHE_SECONDS)
      .catch(() => undefined);
    return body;
  }
}
