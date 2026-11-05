import {
  localeOfPath,
  normalizeQuery,
  pagePath,
} from "./search-console-url.util";

const HOSTS = ["park.fan", "www.park.fan"];

describe("pagePath", () => {
  it("strips the origin, the query, the fragment and the trailing slash", () => {
    expect(
      pagePath(
        "https://park.fan/de/parks/europe/germany/bruehl/phantasialand/?utm_source=x#top",
        HOSTS,
      ),
    ).toBe("/de/parks/europe/germany/bruehl/phantasialand");
  });

  it("lower-cases the path, so it joins the site statistic's", () => {
    expect(pagePath("https://park.fan/DE/Parks/Europe", HOSTS)).toBe(
      "/de/parks/europe",
    );
  });

  it("makes the http, https, www and bare forms of one page one path", () => {
    const paths = [
      "http://park.fan/en/news",
      "https://park.fan/en/news/",
      "https://www.park.fan/en/news",
      "https://WWW.PARK.FAN/en/news",
    ].map((url) => pagePath(url, HOSTS));
    expect(new Set(paths)).toEqual(new Set(["/en/news"]));
  });

  it("keeps the root as a slash", () => {
    expect(pagePath("https://park.fan/", HOSTS)).toBe("/");
    expect(pagePath("https://park.fan", HOSTS)).toBe("/");
  });

  it("drops a page on a host the property does not count", () => {
    expect(pagePath("https://api.park.fan/v1/parks", HOSTS)).toBeNull();
    expect(pagePath("https://example.com/de", HOSTS)).toBeNull();
  });

  it("drops text that is no URL and a path no page has", () => {
    expect(pagePath("not a url", HOSTS)).toBeNull();
    expect(pagePath(`https://park.fan/${"a".repeat(900)}`, HOSTS)).toBeNull();
  });
});

describe("localeOfPath", () => {
  it("reads the language from the first segment", () => {
    expect(localeOfPath("/de/parks")).toBe("de");
    expect(localeOfPath("/it")).toBe("it");
  });

  it("is empty for a path outside the six", () => {
    expect(localeOfPath("/")).toBe("");
    expect(localeOfPath("/pl/parks")).toBe("");
    expect(localeOfPath("/deutsch/parks")).toBe("");
  });
});

describe("normalizeQuery", () => {
  it("trims, lower-cases and cuts", () => {
    expect(normalizeQuery("  Phantasialand Wartezeiten ")).toBe(
      "phantasialand wartezeiten",
    );
    expect(normalizeQuery("x".repeat(900))).toHaveLength(500);
  });
});
