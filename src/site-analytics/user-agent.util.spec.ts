import { MAX_USER_AGENT_LENGTH, parseUserAgent } from "./user-agent.util";

/**
 * Real User-Agents of the browsers park.fan sees. What is pinned: the order
 * of the rules, since every Chromium browser also says Chrome, every iOS
 * browser also says Safari, an iPhone also says "Mac OS X" and Android and
 * ChromeOS also say Linux.
 */
describe("parseUserAgent", () => {
  const cases: Array<[string, string, string]> = [
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
      "safari",
      "ios",
    ],
    [
      "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
      "safari",
      "ios",
    ],
    [
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
      "safari",
      "macos",
    ],
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/130.0.6723.90 Mobile/15E148 Safari/604.1",
      "chrome",
      "ios",
    ],
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/132.0 Mobile/15E148 Safari/605.1.15",
      "firefox",
      "ios",
    ],
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) EdgiOS/130.0.2849.80 Version/18.0 Mobile/15E148 Safari/604.1",
      "edge",
      "ios",
    ],
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 350.0.0.32.104",
      "other",
      "ios",
    ],
    [
      "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
      "chrome",
      "android",
    ],
    [
      "Mozilla/5.0 (Linux; Android 14; SM-S921B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/26.0 Chrome/122.0.0.0 Mobile Safari/537.36",
      "samsung",
      "android",
    ],
    [
      "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36 OPR/85.0.0.0",
      "opera",
      "android",
    ],
    [
      "Mozilla/5.0 (Android 14; Mobile; rv:132.0) Gecko/132.0 Firefox/132.0",
      "firefox",
      "android",
    ],
    [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      "chrome",
      "windows",
    ],
    [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
      "edge",
      "windows",
    ],
    [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0",
      "opera",
      "windows",
    ],
    [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
      "firefox",
      "windows",
    ],
    [
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      "chrome",
      "linux",
    ],
    [
      "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
      "firefox",
      "linux",
    ],
    [
      "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      "chrome",
      "chromeos",
    ],
  ];

  it.each(cases)("%s → %s on %s", (userAgent, browser, os) => {
    expect(parseUserAgent(userAgent)).toEqual({ browser, os });
  });

  it("answers other for what it does not know, a missing header included", () => {
    expect(parseUserAgent("SomethingNew/1.0")).toEqual({
      browser: "other",
      os: "other",
    });
    expect(parseUserAgent(null)).toEqual({ browser: "other", os: "other" });
    expect(parseUserAgent("")).toEqual({ browser: "other", os: "other" });
  });

  it("reads only the first 512 characters, in linear time", () => {
    // Many engine tokens and no Safari one: the old `AppleWebKit\/.*\bSafari\/`
    // backtracked over every pair of them.
    const hostile = "AppleWebKit/".repeat(50_000);
    const started = performance.now();
    expect(parseUserAgent(hostile).browser).toBe("other");
    expect(performance.now() - started).toBeLessThan(50);

    const padded = `${"x".repeat(MAX_USER_AGENT_LENGTH)} Firefox/131.0`;
    expect(parseUserAgent(padded).browser).toBe("other");
  });
});
