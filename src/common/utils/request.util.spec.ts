import { getRateLimitIp, type RequestLike } from "./request.util";

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
