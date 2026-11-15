import { acceptedLanguages, browserLanguage } from "./browser-language.util";

/** What is pinned: every tag's primary subtag that is not refused, two letters. */
describe("acceptedLanguages", () => {
  it.each([
    ["de-DE,de;q=0.9,en;q=0.8", ["de", "en"]],
    ["en-US,en;q=0.9", ["en"]],
    ["en-GB, nl ;q=0.5", ["en", "nl"]],
    ["fr;q=0, en", ["en"]],
    ["it;q=0.000, es;Q=0.1", ["es"]],
    ["*;q=0.5, *", []],
    ["gsw-CH, x-klingon", []],
    ["", []],
    [null, []],
  ])("%s → %j", (header, languages) => {
    expect([...acceptedLanguages(header)]).toEqual(languages);
  });

  it("reads no further than a real header reaches", () => {
    expect(acceptedLanguages(`${"x,".repeat(200)}de`).has("de")).toBe(false);
  });
});

/** What is pinned: the first tag's primary subtag, two letters, else `zz`. */
describe("browserLanguage", () => {
  it.each([
    ["de-DE,de;q=0.9,en;q=0.8", "de"],
    ["en-US", "en"],
    ["NL", "nl"],
    [" fr-CA ;q=1, en", "fr"],
    ["*", "zz"],
    ["", "zz"],
    [null, "zz"],
    ["zh-Hant-TW", "zh"],
    ["gsw-CH", "zz"],
    ["x".repeat(500), "zz"],
  ])("%s → %s", (header, language) => {
    expect(browserLanguage(header)).toBe(language);
  });
});
