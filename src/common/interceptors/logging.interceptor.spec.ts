import {
  Logger,
  type CallHandler,
  type ExecutionContext,
} from "@nestjs/common";
import { firstValueFrom, of } from "rxjs";
import { LoggingInterceptor, addressForLog } from "./logging.interceptor";

/**
 * The privacy policy says the server logs no IP addresses. A log line may name
 * where an admin request came from, as a network and never as an address; every
 * other route's line names nothing.
 */
describe("addressForLog", () => {
  it("truncates the address of an admin request", () => {
    expect(addressForLog("/v1/admin/analytics/overview", "203.0.113.47")).toBe(
      "203.0.113.0/24",
    );
    expect(addressForLog("/v1/admin/auth/login", "2001:db8:1:2::9")).toBe(
      "2001:db8:1::/48",
    );
    expect(addressForLog("/v1/admin?x=1", "::ffff:203.0.113.47")).toBe(
      "203.0.113.0/24",
    );
  });

  it("names no address on any other route", () => {
    for (const url of [
      "/v1/page-views",
      "/v1/analytics/collect",
      "/v1/analytics/realtime",
      "/v1/parks/europe",
      "/v1/mcp",
      "/v1/oauth/token",
      "/v1/administrators",
    ]) {
      expect(addressForLog(url, "203.0.113.47")).toBeNull();
    }
  });

  it("names nothing it cannot tell is an address", () => {
    expect(addressForLog("/v1/admin/x", "203.0.113.47:8080")).toBeNull();
    expect(addressForLog("/v1/admin/x", "unknown")).toBeNull();
    expect(addressForLog("/v1/admin/x", undefined)).toBeNull();
  });
});

describe("LoggingInterceptor", () => {
  const FULL = "203.0.113.47";
  const FULL_V6 = "2001:db8:1:2:3:4:5:6";

  let lines: string[];
  beforeEach(() => {
    lines = [];
    jest
      .spyOn(Logger.prototype, "log")
      .mockImplementation((message: unknown) => {
        lines.push(String(message));
      });
  });
  afterEach(() => jest.restoreAllMocks());

  async function line(url: string, ip: string, statusCode = 200) {
    const context = {
      switchToHttp: () => ({
        getRequest: () => ({ method: "GET", url, ip }),
        getResponse: () => ({ statusCode }),
      }),
    } as unknown as ExecutionContext;
    const next: CallHandler = { handle: () => of("ok") };
    await firstValueFrom(new LoggingInterceptor().intercept(context, next));
    return lines[lines.length - 1];
  }

  it("logs an admin request with the network, not the address", async () => {
    const written = await line("/v1/admin/system-health", FULL);
    expect(written).toContain("/v1/admin/system-health 200");
    expect(written).toContain("203.0.113.0/24");
    expect(written).not.toContain(FULL);
  });

  it("logs an IPv6 admin caller as a /48", async () => {
    const written = await line("/v1/admin/system-health", FULL_V6);
    expect(written).toContain("2001:db8:1::/48");
    expect(written).not.toContain("2001:db8:1:2");
  });

  it("logs an error on a public route without any address", async () => {
    const written = await line("/v1/parks/europe/nowhere", FULL, 404);
    expect(written).toContain("❌ GET /v1/parks/europe/nowhere 404");
    expect(written).not.toContain("203.0.113");
  });

  it("logs a visitor's counter requests without any address, as before", async () => {
    expect(await line("/v1/page-views", FULL, 400)).not.toContain("203.0.113");
    expect(await line("/v1/analytics/collect", FULL, 400)).not.toContain(
      "203.0.113",
    );
  });

  it("does not log a quiet public request at all", async () => {
    const before = lines.length;
    await line("/v1/parks/europe", FULL, 200);
    expect(lines).toHaveLength(before);
  });
});
