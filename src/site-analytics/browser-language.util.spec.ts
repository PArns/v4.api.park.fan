import { browserLanguage } from "./browser-language.util";

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
