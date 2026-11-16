import type { ApiKeyRequestTier } from "./entities/api-key-request.entity";

/**
 * What a key request asks, the values each answer may take, and which key the
 * terms allow for it. The frontend's form offers the same values
 * (`lib/developers/key-request.ts`).
 */

export const PROJECT_KINDS = [
  "mobile-app",
  "website",
  "smart-home",
  "chat-bot",
  "research",
  "internal-tool",
  "other",
] as const;

export const PLATFORMS = [
  "ios",
  "android",
  "web",
  "desktop",
  "server",
  "other",
] as const;

export const SOURCE_MODELS = ["open-source", "closed-source"] as const;

/** How a project earns money. `none` stands alone. */
export const REVENUE_MODELS = [
  "none",
  "donations",
  "ads",
  "affiliate",
  "paid",
  "client-work",
  "company",
] as const;

/**
 * What a free key allows besides earning nothing: donations and sponsorship
 * of an open-source project. Ads, affiliate or paid links, a price, client
 * work and a company's product make it a paid key.
 */
export const FREE_REVENUE: ReadonlySet<string> = new Set(["none", "donations"]);

export const REQUEST_VOLUMES = [
  "under-1k",
  "under-10k",
  "under-100k",
  "more",
] as const;

/** Undecided requests one account may have open at once. */
export const MAX_PENDING_REQUESTS_PER_ACCOUNT = 3;

/** Field lengths, shared with the frontend's form. */
export const REQUEST_LIMITS = {
  projectName: 80,
  url: 300,
  languages: 200,
  descriptionMin: 40,
  description: 3000,
  license: 80,
  endpoints: 2000,
  parks: 500,
  caching: 1000,
  message: 2000,
  reason: 2000,
} as const;

/**
 * Which key the terms allow: free only for open source that earns nothing
 * but donations; everything else is a paid key under a written agreement.
 */
export function requestTier(input: {
  source: string;
  revenue: readonly string[];
}): ApiKeyRequestTier {
  const earnsNothing =
    input.revenue.length > 0 &&
    input.revenue.every((model) => FREE_REVENUE.has(model));
  return input.source === "open-source" && earnsNothing ? "free" : "paid";
}
