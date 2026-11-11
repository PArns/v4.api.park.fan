import { UnauthorizedException } from "@nestjs/common";
import { lastValueFrom, of } from "rxjs";
import {
  ForecastFieldsInterceptor,
  wantsForecast,
} from "./forecast-fields.interceptor";
import { omitPaths } from "./omit-paths";

describe("omitPaths", () => {
  const park = {
    name: "Magic Kingdom",
    attractions: [
      { name: "TRON", waitTime: 70, bestVisitTimes: [{ time: "21:00" }] },
      { name: "Jungle Cruise", waitTime: 20 },
    ],
  };

  it("drops a field from every element of an array", () => {
    expect(omitPaths(park, ["attractions[].bestVisitTimes"])).toEqual({
      name: "Magic Kingdom",
      attractions: [
        { name: "TRON", waitTime: 70 },
        { name: "Jungle Cruise", waitTime: 20 },
      ],
    });
  });

  it("drops top-level fields and leaves the input untouched", () => {
    const ride = { name: "TRON", hourlyForecast: [1], predictionAccuracy: {} };
    expect(omitPaths(ride, ["hourlyForecast", "predictionAccuracy"])).toEqual({
      name: "TRON",
    });
    expect(ride).toHaveProperty("hourlyForecast");
    omitPaths(park, ["attractions[].bestVisitTimes"]);
    expect(park.attractions[0]).toHaveProperty("bestVisitTimes");
  });

  it("passes through what has no such field", () => {
    expect(omitPaths(null, ["a"])).toBeNull();
    expect(omitPaths([1, 2], ["a"])).toEqual([1, 2]);
    expect(omitPaths({ b: 1 }, ["a[].c"])).toEqual({ b: 1 });
  });
});

describe("wantsForecast", () => {
  it("reads forecast=true and forecast=1, nothing else", () => {
    expect(wantsForecast({ forecast: "true" })).toBe(true);
    expect(wantsForecast({ forecast: "1" })).toBe(true);
    expect(wantsForecast({ forecast: "yes" })).toBe(false);
    expect(wantsForecast({})).toBe(false);
    expect(wantsForecast(undefined)).toBe(false);
  });
});

describe("ForecastFieldsInterceptor", () => {
  const body = { name: "TRON", waitTime: 70, hourlyForecast: [1] };
  const previous = process.env.THROTTLE_BYPASS_KEYS;
  beforeEach(() => {
    process.env.THROTTLE_BYPASS_KEYS = "frontend-secret";
  });
  afterEach(() => {
    process.env.THROTTLE_BYPASS_KEYS = previous;
  });

  function run(query: Record<string, string>, headers = {}) {
    const sent = new Map<string, unknown>();
    const response = {
      headersSent: false,
      setHeader(name: string, value: unknown) {
        sent.set(name.toLowerCase(), value);
        return response;
      },
    };
    const context = {
      getHandler: () => undefined,
      switchToHttp: () => ({
        getRequest: () => ({ query, headers }),
        getResponse: () => response,
      }),
    };
    const reflector = { get: () => ["hourlyForecast"] };
    const apiKeys = { resolve: jest.fn(async () => null) };
    const interceptor = new ForecastFieldsInterceptor(
      reflector as never,
      { get: () => apiKeys } as never,
    );
    const result = lastValueFrom(
      interceptor.intercept(context as never, { handle: () => of(body) }),
    );
    return { result, sent };
  }

  it("leaves the forecast fields out when nobody asked for them", async () => {
    const { result, sent } = run({});
    await expect(result).resolves.toEqual({ name: "TRON", waitTime: 70 });
    expect(sent.has("cache-control")).toBe(false);
  });

  it("refuses forecast=true without a key", async () => {
    const { result, sent } = run({ forecast: "true" });
    await expect(result).rejects.toBeInstanceOf(UnauthorizedException);
    expect(sent.get("cache-control")).toBe("private, no-store");
  });

  it("answers in full and privately to park.fan's own frontend", async () => {
    const { result, sent } = run(
      { forecast: "true" },
      { "x-auth-key": "frontend-secret" },
    );
    await expect(result).resolves.toEqual(body);
    expect(sent.get("cache-control")).toBe("private, no-store");
  });
});
