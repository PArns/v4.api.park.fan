import { requestOrigin } from "./request-origin";

const geoip = {
  lookupCountry: (ip: string) => (ip === "203.0.113.9" ? "us" : null),
};
const SAFARI =
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1";

describe("requestOrigin", () => {
  it("keeps the country and the browser family, nothing else", () => {
    expect(requestOrigin(geoip, "203.0.113.9", SAFARI)).toEqual({
      country: "us",
      browser: "safari",
    });
  });

  it("knows neither without an address or a recorded User-Agent", () => {
    expect(requestOrigin(geoip, null, undefined)).toEqual({
      country: null,
      browser: null,
    });
    expect(requestOrigin(geoip, null, null).browser).toBeNull();
  });

  it("counts a request that sent no User-Agent as another browser", () => {
    expect(requestOrigin(geoip, null, "").browser).toBe("other");
  });
});
