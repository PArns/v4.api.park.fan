import {
  parseSitemap,
  reindexPriority,
  reindexState,
} from "./search-console-reindex";

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

describe("reindexPriority", () => {
  const inspectedAt = new Date("2026-11-01T10:00:00Z");
  const crawled = (iso: string) => new Date(iso);

  it("counts the impressions once per day Google had not crawled the page", () => {
    expect(
      reindexPriority(
        14648,
        "stale",
        crawled("2026-10-30T10:00:00Z"),
        inspectedAt,
      ),
    ).toBe(29296);
  });

  it("counts at most 30 days", () => {
    expect(
      reindexPriority(
        1593,
        "stale",
        crawled("2026-09-01T10:00:00Z"),
        inspectedAt,
      ),
    ).toBe(47790);
  });

  it("counts 30 days for a page never crawled, and nothing without impressions", () => {
    expect(reindexPriority(40, "unknown", null, inspectedAt)).toBe(1200);
    expect(reindexPriority(0, "unknown", null, inspectedAt)).toBe(0);
  });

  it("halves an uncertain page: Google may have seen the change", () => {
    expect(
      reindexPriority(
        1000,
        "uncertain",
        crawled("2026-10-31T10:00:00Z"),
        inspectedAt,
      ),
    ).toBe(500);
  });

  it("counts no days for a crawl after the inspection", () => {
    expect(
      reindexPriority(
        1000,
        "stale",
        crawled("2026-11-01T11:00:00Z"),
        inspectedAt,
      ),
    ).toBe(0);
  });

  it("puts a page Google left alone for months above one it crawls every few days", () => {
    const glossary = reindexPriority(
      1593,
      "stale",
      crawled("2026-09-01T10:00:00Z"),
      inspectedAt,
    );
    const park = reindexPriority(
      14648,
      "stale",
      crawled("2026-10-30T10:00:00Z"),
      inspectedAt,
    );
    expect(glossary).toBeGreaterThan(park);
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
