import { REFERRER_RULES, classifyReferrer } from "./referrer.util";

/**
 * Where a visit came from. What is pinned: a site is one name across its
 * country domains and prefixes; an assistant on a search engine's domain is
 * an assistant; an unknown host is a referral under its own name; a
 * `utm_source` or a campaign makes the visit a campaign; no referrer is
 * direct.
 */
describe("classifyReferrer", () => {
  it.each([
    ["google.de", "google", "search"],
    ["www.google.com", "google", "search"],
    ["google.co.uk", "google", "search"],
    ["google.com.au", "google", "search"],
    ["bing.com", "bing", "search"],
    ["duckduckgo.com", "duckduckgo", "search"],
    ["ecosia.org", "ecosia", "search"],
    ["search.yahoo.com", "yahoo", "search"],
    ["de.search.yahoo.com", "yahoo", "search"],
    ["yandex.ru", "yandex", "search"],
    ["search.brave.com", "brave", "search"],
    ["suche.t-online.de", "t-online", "search"],
    ["gemini.google.com", "gemini", "ai"],
    ["chatgpt.com", "chatgpt", "ai"],
    ["chat.openai.com", "chatgpt", "ai"],
    ["perplexity.ai", "perplexity", "ai"],
    ["www.perplexity.ai", "perplexity", "ai"],
    ["copilot.microsoft.com", "copilot", "ai"],
    ["claude.ai", "claude", "ai"],
    ["m.facebook.com", "facebook", "social"],
    ["l.facebook.com", "facebook", "social"],
    ["l.instagram.com", "instagram", "social"],
    ["t.co", "x", "social"],
    ["out.reddit.com", "reddit", "social"],
    ["pinterest.de", "pinterest", "social"],
    ["youtube.com", "youtube", "social"],
    ["coastersandmore.de", "coastersandmore.de", "referral"],
    ["www.forum.example.org", "forum.example.org", "referral"],
    // Not a search engine because the name contains one.
    ["notgoogle.com", "notgoogle.com", "referral"],
    // Google's other services send readers like any site, not as a search.
    ["mail.google.com", "mail.google.com", "referral"],
    ["docs.google.com", "docs.google.com", "referral"],
    ["sites.google.com", "sites.google.com", "referral"],
    ["maps.google.de", "maps.google.de", "referral"],
    ["news.google.com", "news.google.com", "referral"],
    ["mail.yahoo.com", "mail.yahoo.com", "referral"],
    ["mail.yandex.ru", "mail.yandex.ru", "referral"],
  ])("%s → %s (%s)", (host, referrer, channel) => {
    expect(classifyReferrer(host, null)).toEqual({ referrer, channel });
  });

  it("files a visit without a referrer as direct", () => {
    expect(classifyReferrer(null, null)).toEqual({
      referrer: "",
      channel: "direct",
    });
  });

  it("files a utm_source, or any visit with a campaign, as a campaign", () => {
    expect(classifyReferrer("newsletter", null)).toEqual({
      referrer: "newsletter",
      channel: "campaign",
    });
    expect(classifyReferrer("www.google.de", "spring")).toEqual({
      referrer: "google",
      channel: "campaign",
    });
    expect(classifyReferrer(null, "spring")).toEqual({
      referrer: "",
      channel: "campaign",
    });
  });

  it("names every site in lowercase, the way it is stored", () => {
    for (const rule of REFERRER_RULES) {
      expect(rule.site).toMatch(/^[a-z0-9.-]+$/);
    }
  });
});
