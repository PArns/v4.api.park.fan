import {
  MAX_USER_AGENT_LENGTH,
  deviceClassOf,
  parseUserAgent,
} from "./user-agent.util";

/**
 * Real User-Agents of the browsers park.fan sees. What is pinned: the order
 * of the rules, since every Chromium browser also says Chrome, every iOS
 * browser also says Safari, an iPhone also says "Mac OS X" and Android and
 * ChromeOS also say Linux; and the device class that comes out of the same
 * header.
 */
describe("parseUserAgent", () => {
  const cases: Array<[string, string, string, string]> = [
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
      "safari",
      "ios",
      "mobile",
    ],
    [
      "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
      "safari",
      "ios",
      "tablet",
    ],
    [
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
      "safari",
      "macos",
      "desktop",
    ],
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/130.0.6723.90 Mobile/15E148 Safari/604.1",
      "chrome",
      "ios",
      "mobile",
    ],
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/132.0 Mobile/15E148 Safari/605.1.15",
      "firefox",
      "ios",
      "mobile",
    ],
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) EdgiOS/130.0.2849.80 Version/18.0 Mobile/15E148 Safari/604.1",
      "edge",
      "ios",
      "mobile",
    ],
    [
      "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 350.0.0.32.104",
      "other",
      "ios",
      "mobile",
    ],
    [
      "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
      "chrome",
      "android",
      "mobile",
    ],
    [
      "Mozilla/5.0 (Linux; Android 14; SM-S921B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/26.0 Chrome/122.0.0.0 Mobile Safari/537.36",
      "samsung",
      "android",
      "mobile",
    ],
    [
      "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36 OPR/85.0.0.0",
      "opera",
      "android",
      "mobile",
    ],
    [
      "Mozilla/5.0 (Android 14; Mobile; rv:132.0) Gecko/132.0 Firefox/132.0",
      "firefox",
      "android",
      "mobile",
    ],
    [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      "chrome",
      "windows",
      "desktop",
    ],
    [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
      "edge",
      "windows",
      "desktop",
    ],
    [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 OPR/115.0.0.0",
      "opera",
      "windows",
      "desktop",
    ],
    [
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
      "firefox",
      "windows",
      "desktop",
    ],
    [
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      "chrome",
      "linux",
      "desktop",
    ],
    [
      "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
      "firefox",
      "linux",
      "desktop",
    ],
    [
      "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
      "chrome",
      "chromeos",
      "desktop",
    ],
  ];

  it.each(cases)("%s → %s on %s, a %s", (userAgent, browser, os, device) => {
    expect(parseUserAgent(userAgent)).toEqual({ browser, os, device });
  });

  it("answers other for what it does not know, a missing header included", () => {
    const unknown = { browser: "other", os: "other", device: "desktop" };
    expect(parseUserAgent("SomethingNew/1.0")).toEqual(unknown);
    expect(parseUserAgent(null)).toEqual(unknown);
    expect(parseUserAgent("")).toEqual(unknown);
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

/**
 * The device class. What is pinned: a tablet is told by name or by an Android
 * without `Mobile`, and wins over `Mobile` (an iPad and Silk say both); a
 * phone by `Mobi…`, `iPhone`, `iPod`, `Windows Phone` or `Opera Mini`, which
 * also say `Android` and then no `Mobile`; everything else, an iPad that
 * sends a Mac User-Agent included, is a desktop.
 */
describe("deviceClassOf", () => {
  const cases: Array<[string, string, string]> = [
    [
      "tablet",
      "an iPad",
      "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    ],
    [
      "tablet",
      "Chrome on an Android tablet, which leaves out Mobile",
      "Mozilla/5.0 (Linux; Android 13; SM-X700) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    ],
    [
      "tablet",
      "Firefox on an Android tablet",
      "Mozilla/5.0 (Android 14; Tablet; rv:132.0) Gecko/132.0 Firefox/132.0",
    ],
    [
      "tablet",
      "Silk on a Kindle Fire",
      "Mozilla/5.0 (Linux; Android 9; KFMAWI) AppleWebKit/537.36 (KHTML, like Gecko) Silk/130.2.2 like Chrome/130.0.0.0 Safari/537.36",
    ],
    [
      "tablet",
      "Silk that also says Mobile",
      "Mozilla/5.0 (Linux; Android 9; KFMAWI) AppleWebKit/537.36 (KHTML, like Gecko) Silk/130.2.2 like Chrome/130.0.0.0 Mobile Safari/537.36",
    ],
    [
      "tablet",
      "an e-reader's Kindle browser",
      "Mozilla/5.0 (X11; U; Linux armv7l like Android; en-us) AppleWebKit/531.2+ (KHTML, like Gecko) Version/5.0 Safari/533.2+ Kindle/3.0+",
    ],
    [
      "tablet",
      "a PlayBook",
      "Mozilla/5.0 (PlayBook; U; RIM Tablet OS 2.1.0; en-US) AppleWebKit/536.2+ (KHTML, like Gecko) Version/7.2.1.0 Safari/536.2+",
    ],
    [
      "mobile",
      "an iPhone",
      "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
    ],
    [
      "mobile",
      "an iPod touch",
      "Mozilla/5.0 (iPod touch; CPU iPhone OS 15_8 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Mobile/15E148 Safari/604.1",
    ],
    [
      "mobile",
      "Chrome on an Android phone",
      "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
    ],
    [
      "mobile",
      "Firefox on an Android phone",
      "Mozilla/5.0 (Android 14; Mobile; rv:132.0) Gecko/132.0 Firefox/132.0",
    ],
    [
      "mobile",
      "Edge on Windows Phone",
      "Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 950) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Mobile Safari/537.36 Edge/15.15063",
    ],
    [
      "mobile",
      "Internet Explorer on Windows Phone",
      "Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 920)",
    ],
    [
      "mobile",
      "Windows Phone, which says Android and no Mobile",
      "Mozilla/5.0 (Windows Phone 10.0; Android 6.0.1; Microsoft; Lumia 950)",
    ],
    [
      "mobile",
      "Opera Mini on Android, which says Android and no Mobile",
      "Opera/9.80 (Android; Opera Mini/36.2.2254/119.132; U; id) Presto/2.12.423 Version/12.16",
    ],
    [
      "desktop",
      "Chrome on Windows",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    ],
    [
      "desktop",
      "Safari on a Mac, which is also what an iPad on iPadOS 13 or later sends",
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    ],
    [
      "desktop",
      "Firefox on Linux",
      "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
    ],
    [
      "desktop",
      "Chrome on ChromeOS",
      "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    ],
  ];

  it.each(cases)("%s: %s", (device, _name, userAgent) => {
    expect(deviceClassOf(userAgent)).toBe(device);
  });

  it("answers desktop for a header it does not know or that is missing", () => {
    expect(deviceClassOf("SomethingNew/1.0")).toBe("desktop");
    expect(deviceClassOf("")).toBe("desktop");
    expect(deviceClassOf(null)).toBe("desktop");
    expect(deviceClassOf(undefined)).toBe("desktop");
  });

  it("reads only the first 512 characters, in linear time", () => {
    const hostile = "Android Mobi iPad ".repeat(50_000);
    const started = performance.now();
    expect(deviceClassOf(hostile)).toBe("tablet");
    expect(performance.now() - started).toBeLessThan(50);

    expect(deviceClassOf(`${"x".repeat(MAX_USER_AGENT_LENGTH)} iPad`)).toBe(
      "desktop",
    );
  });
});
