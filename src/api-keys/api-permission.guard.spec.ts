import { ForbiddenException, UnauthorizedException } from "@nestjs/common";
import {
  authorizeApiRequest,
  type ForecastRequest,
} from "./forecast-access.guard";
import {
  API_PERMISSIONS,
  effectivePermissions,
  grantedPermissions,
  withheldPaths,
} from "./permissions";
import { omitPaths } from "./omit-paths";

const LIVE_ONLY = `pfk_${"a".repeat(43)}`;
const LEGACY = `pfk_${"b".repeat(43)}`;
const INTERNAL = `pfk_${"c".repeat(43)}`;

function setup() {
  const keys: Record<string, unknown> = {
    [LIVE_ONLY]: {
      id: "k-live",
      usage: "non-commercial",
      requestsPerHour: 100,
      allowedParkIds: null,
      permissions: ["live.waits", "parks.details"],
      expiresAt: null,
    },
    [LEGACY]: {
      id: "k-legacy",
      usage: "non-commercial",
      requestsPerHour: 100,
      allowedParkIds: null,
      permissions: null,
      expiresAt: null,
    },
    [INTERNAL]: {
      id: "k-site",
      usage: "internal",
      requestsPerHour: null,
      allowedParkIds: null,
      permissions: ["live.waits"],
      expiresAt: null,
    },
  };
  const apiKeys = {
    lookup: jest.fn(async (value: string) =>
      keys[value]
        ? { status: "active", key: keys[value] }
        : { status: "unknown" },
    ),
    countRequest: jest.fn(async () => null),
  };
  const sent = new Map<string, unknown>();
  const response = {
    headersSent: false,
    setHeader(name: string, value: unknown) {
      sent.set(name.toLowerCase(), value);
      return response;
    },
  };
  return { apiKeys, response, sent };
}

function request(headers: Record<string, unknown> = {}): ForecastRequest {
  return { headers, params: {}, query: {} };
}

describe("authorizeApiRequest", () => {
  it("lets a caller without a key read the wait times, and keeps the answer public", async () => {
    const { apiKeys, response, sent } = setup();
    const req = request();
    await authorizeApiRequest(apiKeys as never, req, response as never, {
      permission: "live.waits",
    });
    expect([...(req.apiAccess?.permissions ?? [])]).toEqual(["live.waits"]);
    expect(sent.get("cache-control")).toBeUndefined();
    expect(apiKeys.countRequest).not.toHaveBeenCalled();
  });

  it("refuses a caller without a key on every other route, naming the permission", async () => {
    const { apiKeys, response } = setup();
    const error = await authorizeApiRequest(
      apiKeys as never,
      request(),
      response as never,
      { permission: "parks.directory" },
    ).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(UnauthorizedException);
    expect((error as UnauthorizedException).getResponse()).toMatchObject({
      error: "api-key-required",
      portal: "https://park.fan/en/developers",
    });
  });

  it("refuses a key without the route's permission with 403, before counting", async () => {
    const { apiKeys, response } = setup();
    const error = await authorizeApiRequest(
      apiKeys as never,
      request({ "x-api-key": LIVE_ONLY }),
      response as never,
      { permission: "forecasts.crowd" },
    ).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ForbiddenException);
    expect((error as ForbiddenException).getResponse()).toMatchObject({
      error: "api-key-permission-missing",
      requiredPermission: "forecasts.crowd",
    });
    expect(apiKeys.countRequest).not.toHaveBeenCalled();
  });

  it("lets a key with the permission through, counts it and keeps the answer private", async () => {
    const { apiKeys, response, sent } = setup();
    const req = request({ "x-api-key": LIVE_ONLY });
    await authorizeApiRequest(apiKeys as never, req, response as never, {
      permission: "parks.details",
    });
    expect(apiKeys.countRequest).toHaveBeenCalledTimes(1);
    expect(sent.get("cache-control")).toBe("private, no-store");
    expect(req.apiAccess?.permissions.has("parks.details")).toBe(true);
  });

  it("counts a keyed request on the keyless route too", async () => {
    const { apiKeys, response } = setup();
    await authorizeApiRequest(
      apiKeys as never,
      request({ "x-api-key": LIVE_ONLY }),
      response as never,
      { permission: "live.waits" },
    );
    expect(apiKeys.countRequest).toHaveBeenCalledTimes(1);
  });

  it("refuses an unknown key even on the keyless route", async () => {
    const { apiKeys, response } = setup();
    const error = await authorizeApiRequest(
      apiKeys as never,
      request({ "x-api-key": `pfk_${"z".repeat(43)}` }),
      response as never,
      { permission: "live.waits" },
    ).catch((e: unknown) => e);
    expect((error as UnauthorizedException).getResponse()).toMatchObject({
      error: "api-key-invalid",
    });
  });

  it("reads a key from before permissions as holding every permission", async () => {
    const { apiKeys, response } = setup();
    for (const permission of API_PERMISSIONS) {
      await expect(
        authorizeApiRequest(
          apiKeys as never,
          request({ "x-api-key": LEGACY }),
          response as never,
          { permission },
        ),
      ).resolves.toBeUndefined();
    }
  });

  it("opens the internal routes only to park.fan's own key", async () => {
    const { apiKeys, response } = setup();
    await expect(
      authorizeApiRequest(
        apiKeys as never,
        request({ "x-api-key": INTERNAL }),
        response as never,
        { permission: "internal" },
      ),
    ).resolves.toBeUndefined();
    const refused = await authorizeApiRequest(
      apiKeys as never,
      request({ "x-api-key": LEGACY }),
      response as never,
      { permission: "internal" },
    ).catch((e: unknown) => e);
    expect(refused).toBeInstanceOf(ForbiddenException);
  });
});

describe("permissions", () => {
  it("stores a granted list in catalog order and refuses an empty or unknown one", () => {
    expect(grantedPermissions(["stats.live", "live.waits"])).toEqual([
      "live.waits",
      "stats.live",
    ]);
    expect(grantedPermissions(undefined)).toBeNull();
    expect(() => grantedPermissions([])).toThrow();
    expect(() => grantedPermissions(["live.waits", "everything"])).toThrow();
  });

  it("gives park.fan's own key every permission whatever its list says", () => {
    const held = effectivePermissions({
      usage: "internal",
      permissions: ["live.waits"],
    });
    expect(API_PERMISSIONS.every((p) => held.has(p))).toBe(true);
    expect(held.has("internal")).toBe(true);
  });

  it("drops the fields of a park detail a key may not read", () => {
    const park = {
      name: "Phantasialand",
      weather: { current: {} },
      schedule: [],
      analytics: { occupancy: 40 },
      attractions: [
        {
          name: "Taron",
          queues: [{ waitTime: 30 }],
          minimumHeight: 130,
          rideProfile: { manufacturer: "Intamin" },
          crowdLevel: "high",
          typicalWaits: { p50: 25 },
        },
      ],
    };
    const held = new Set(["parks.details", "live.weather"]);
    const result = omitPaths(park, withheldPaths("park", held)) as typeof park;
    expect(result.weather).toBeDefined();
    expect(result).not.toHaveProperty("schedule");
    expect(result).not.toHaveProperty("analytics");
    expect(result.attractions[0]).toEqual({
      name: "Taron",
      queues: [{ waitTime: 30 }],
    });
    // The cached original is never changed.
    expect(park.attractions[0].minimumHeight).toBe(130);
  });

  it("opens the typical waits and headliners alone with stats.typical-waits", () => {
    const park = {
      ropeDropHeadliners: ["Taron"],
      attractions: [
        {
          name: "Taron",
          typicalWaits: { p50: 25 },
          isHeadliner: true,
          statistics: { avg: 30 },
          ropeDrop: { gain: 20 },
        },
      ],
    };
    const held = effectivePermissions({
      usage: "non-commercial",
      permissions: ["parks.details", "stats.typical-waits"],
    });
    const result = omitPaths(park, withheldPaths("park", held)) as typeof park;
    expect(result).not.toHaveProperty("ropeDropHeadliners");
    expect(result.attractions[0]).toEqual({
      name: "Taron",
      typicalWaits: { p50: 25 },
      isHeadliner: true,
    });
    const ride = { name: "Taron", typicalWaits: { p50: 25 }, history: [] };
    expect(omitPaths(ride, withheldPaths("ride", held))).toEqual({
      name: "Taron",
      typicalWaits: { p50: 25 },
    });
  });

  it("lets a key with stats.history read the typical waits without storing them", () => {
    const key = { usage: "non-commercial", permissions: ["stats.history"] };
    expect(effectivePermissions(key).has("stats.typical-waits")).toBe(true);
    expect(grantedPermissions(key.permissions)).toEqual(["stats.history"]);
    expect(withheldPaths("park", effectivePermissions(key))).not.toContain(
      "attractions[].typicalWaits",
    );
  });
});
