import {
  addressBucket,
  getRateLimitIp,
  type RequestLike,
} from "./request.util";

describe("getRateLimitIp", () => {
  const ORIGINAL_ENV = process.env;
  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV, THROTTLE_BYPASS_KEYS: "frontend-key" };
    delete process.env.THROTTLE_BYPASS_HEADER;
  });
  afterAll(() => {
    process.env = ORIGINAL_ENV;
  });

  const req = (headers: RequestLike["headers"]): RequestLike => ({
    headers,
    ip: "198.51.100.66", // what `trust proxy` makes of a caller-written XFF
    socket: { remoteAddress: "172.16.0.1" },
  });

  it("ignores a caller-written X-Forwarded-For", () => {
    expect(
      getRateLimitIp(
        req({
          "x-forwarded-for": "198.51.100.66",
          "cf-connecting-ip": "203.0.113.7",
        }),
      ),
    ).toBe("203.0.113.7");
    expect(getRateLimitIp(req({ "x-forwarded-for": "198.51.100.66" }))).toBe(
      "172.16.0.1",
    );
  });

  it("trusts X-Forwarded-For from our own frontend proxy", () => {
    expect(
      getRateLimitIp(
        req({
          "x-auth-key": "frontend-key",
          "x-forwarded-for": "198.51.100.9, 10.0.0.1",
          "cf-connecting-ip": "203.0.113.7",
        }),
      ),
    ).toBe("198.51.100.9");
  });

  it("ignores a CF-Connecting-IP that is not an IP", () => {
    expect(getRateLimitIp(req({ "cf-connecting-ip": "garbage" }))).toBe(
      "172.16.0.1",
    );
  });
});

describe("addressBucket", () => {
  it("keeps an IPv4 address whole and cuts IPv6 to its /64", () => {
    expect(addressBucket("198.51.100.7")).toBe("198.51.100.7");
    expect(addressBucket("2001:db8:1:2:3:4:5:6")).toBe("2001:db8:1:2::/64");
    expect(addressBucket(null)).toBeNull();
  });

  it("cuts IPv6 to the /56 a provider delegates when asked", () => {
    expect(addressBucket("2001:db8:1:2a7f::1", 56)).toBe(
      "2001:db8:1:2a00::/56",
    );
    expect(addressBucket("2001:0db8:0001:2aff:9::1", 56)).toBe(
      "2001:db8:1:2a00::/56",
    );
    // Every /64 of one /56 is one bucket.
    expect(addressBucket("2001:db8::ff:1", 56)).toBe(
      addressBucket("2001:db8::1", 56),
    );
    expect(addressBucket("198.51.100.7", 56)).toBe("198.51.100.7");
  });
});
