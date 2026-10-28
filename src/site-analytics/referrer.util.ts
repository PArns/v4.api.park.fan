import type { Channel } from "./site-analytics.constants";

/**
 * Where a visit came from: the site behind its referrer host (`google.de`,
 * `www.google.com` → `google`) and the channel that site belongs to. The
 * lists are what the statistic knows; a site not in them is a `referral`
 * under its own host. Extending a list is one line here and nothing else.
 */

interface ReferrerRule {
  site: string;
  channel: Exclude<Channel, "campaign" | "direct">;
  /** Hosts matched as themselves or as a parent domain (`x.y` matches `a.x.y`). */
  hosts?: readonly string[];
  /** For sites with country domains (`google.co.uk`). */
  pattern?: RegExp;
}

/** Any number of subdomains, then the name, then a country or generic suffix. */
const anyTld = (name: string) =>
  new RegExp(
    `^(?:[a-z0-9-]+\\.)*${name}\\.(?:[a-z]{2,6}|co\\.[a-z]{2}|com\\.[a-z]{2})$`,
  );

/**
 * First match wins, so an assistant on a search engine's domain
 * (`gemini.google.com`) comes before the engine.
 */
export const REFERRER_RULES: readonly ReferrerRule[] = [
  // AI assistants
  { site: "chatgpt", channel: "ai", hosts: ["chatgpt.com", "chat.openai.com"] },
  { site: "perplexity", channel: "ai", hosts: ["perplexity.ai"] },
  {
    site: "gemini",
    channel: "ai",
    hosts: ["gemini.google.com", "bard.google.com"],
  },
  {
    site: "copilot",
    channel: "ai",
    hosts: ["copilot.microsoft.com", "copilot.cloud.microsoft"],
  },
  { site: "claude", channel: "ai", hosts: ["claude.ai"] },
  { site: "meta-ai", channel: "ai", hosts: ["meta.ai"] },
  { site: "grok", channel: "ai", hosts: ["grok.com"] },
  { site: "deepseek", channel: "ai", hosts: ["deepseek.com"] },
  { site: "mistral", channel: "ai", hosts: ["chat.mistral.ai"] },
  { site: "you", channel: "ai", hosts: ["you.com"] },
  { site: "phind", channel: "ai", hosts: ["phind.com"] },
  // Search engines
  { site: "google", channel: "search", pattern: anyTld("google") },
  { site: "bing", channel: "search", hosts: ["bing.com"] },
  { site: "duckduckgo", channel: "search", hosts: ["duckduckgo.com"] },
  { site: "ecosia", channel: "search", hosts: ["ecosia.org"] },
  { site: "yahoo", channel: "search", pattern: anyTld("yahoo") },
  { site: "yandex", channel: "search", pattern: anyTld("yandex") },
  { site: "yandex", channel: "search", hosts: ["ya.ru"] },
  { site: "baidu", channel: "search", hosts: ["baidu.com"] },
  { site: "qwant", channel: "search", hosts: ["qwant.com"] },
  { site: "startpage", channel: "search", hosts: ["startpage.com"] },
  { site: "brave", channel: "search", hosts: ["search.brave.com"] },
  { site: "kagi", channel: "search", hosts: ["kagi.com"] },
  { site: "naver", channel: "search", hosts: ["naver.com"] },
  { site: "seznam", channel: "search", hosts: ["seznam.cz"] },
  { site: "aol", channel: "search", hosts: ["search.aol.com"] },
  { site: "t-online", channel: "search", hosts: ["suche.t-online.de"] },
  { site: "web.de", channel: "search", hosts: ["suche.web.de"] },
  {
    site: "gmx",
    channel: "search",
    hosts: ["suche.gmx.net", "suche.gmx.at", "suche.gmx.ch"],
  },
  // Social networks and messengers
  {
    site: "facebook",
    channel: "social",
    hosts: ["facebook.com", "fb.com", "fb.me"],
  },
  { site: "instagram", channel: "social", hosts: ["instagram.com"] },
  { site: "x", channel: "social", hosts: ["x.com", "twitter.com", "t.co"] },
  { site: "reddit", channel: "social", hosts: ["reddit.com"] },
  { site: "youtube", channel: "social", hosts: ["youtube.com", "youtu.be"] },
  { site: "tiktok", channel: "social", hosts: ["tiktok.com"] },
  { site: "pinterest", channel: "social", pattern: anyTld("pinterest") },
  { site: "pinterest", channel: "social", hosts: ["pin.it"] },
  { site: "linkedin", channel: "social", hosts: ["linkedin.com", "lnkd.in"] },
  { site: "whatsapp", channel: "social", hosts: ["whatsapp.com", "wa.me"] },
  { site: "telegram", channel: "social", hosts: ["t.me", "telegram.org"] },
  { site: "threads", channel: "social", hosts: ["threads.net", "threads.com"] },
  { site: "bluesky", channel: "social", hosts: ["bsky.app"] },
  { site: "mastodon", channel: "social", hosts: ["mastodon.social"] },
  { site: "snapchat", channel: "social", hosts: ["snapchat.com"] },
  { site: "tumblr", channel: "social", hosts: ["tumblr.com"] },
  { site: "vk", channel: "social", hosts: ["vk.com"] },
  { site: "discord", channel: "social", hosts: ["discord.com", "discord.gg"] },
];

/** Prefixes a site's own pages carry in front of its name. */
const HOST_PREFIX = /^(?:www\d?|m|l|lm|mobile|amp)\./;

function matches(rule: ReferrerRule, host: string): boolean {
  if (rule.pattern?.test(host)) return true;
  return (rule.hosts ?? []).some((h) => host === h || host.endsWith(`.${h}`));
}

/**
 * The site and channel of a visit. The wire's `referrer` is a host, or the
 * `utm_source` value when the link had one; a value without a dot is taken
 * for a `utm_source`, since every host has one. A visit with a campaign or a
 * `utm_source` is `campaign`, one without a referrer `direct`.
 */
export function classifyReferrer(
  referrer: string | null,
  campaign: string | null,
): { referrer: string; channel: Channel } {
  if (!referrer)
    return { referrer: "", channel: campaign ? "campaign" : "direct" };
  if (!referrer.includes(".")) return { referrer, channel: "campaign" };
  const host = referrer.replace(HOST_PREFIX, "");
  const rule = REFERRER_RULES.find((r) => matches(r, host));
  if (campaign) return { referrer: rule?.site ?? host, channel: "campaign" };
  return rule
    ? { referrer: rule.site, channel: rule.channel }
    : { referrer: host, channel: "referral" };
}
