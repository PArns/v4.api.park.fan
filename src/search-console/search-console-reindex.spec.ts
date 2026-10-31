import { parseSitemap, reindexState } from "./search-console-reindex";

describe("reindexState", () => {
  const changedOn = "2026-10-30";

  it("is unknown without a crawl", () => {
    expect(reindexState(changedOn, null)).toBe("unknown");
  });

  it("is stale for a crawl before the day the change can have happened", () => {
    expect(reindexState(changedOn, new Date("2026-10-28T23:59:00Z"))).toBe(
      "stale",
    );
  });

  it("is uncertain for a crawl inside the change's window", () => {
    expect(reindexState(changedOn, new Date("2026-10-29T19:51:00Z"))).toBe(
      "uncertain",
    );
    expect(reindexState(changedOn, new Date("2026-10-30T05:59:00Z"))).toBe(
      "uncertain",
    );
  });

  it("is fresh for a crawl after the content-change crawl saw it", () => {
    expect(reindexState(changedOn, new Date("2026-10-30T06:00:00Z"))).toBe(
      "fresh",
    );
  });
});

describe("parseSitemap", () => {
  it("reads urls with their lastmod day, ignoring alternates", () => {
    const xml = `<?xml version="1.0"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<url>
<loc>https://park.fan/en/parks</loc>
<xhtml:link rel="alternate" hreflang="de" href="https://park.fan/de/parks" />
<lastmod>2026-10-23T00:00:00.000Z</lastmod>
</url>
<url><loc>https://park.fan/en?a=1&amp;b=2</loc></url>
</urlset>`;
    expect(parseSitemap(xml)).toEqual({
      index: false,
      entries: [
        { loc: "https://park.fan/en/parks", lastmod: "2026-10-23" },
        { loc: "https://park.fan/en?a=1&b=2", lastmod: null },
      ],
    });
  });

  it("recognises a sitemap index", () => {
    const xml = `<sitemapindex><sitemap><loc>https://park.fan/sitemap-attractions/de.xml</loc></sitemap></sitemapindex>`;
    expect(parseSitemap(xml)).toEqual({
      index: true,
      entries: [
        { loc: "https://park.fan/sitemap-attractions/de.xml", lastmod: null },
      ],
    });
  });
});
