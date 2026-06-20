import { Test, TestingModule } from "@nestjs/testing";
import axios from "axios";
import { ThemeParksClient } from "./themeparks.client";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { BROWSER_HEADERS } from "../../common/constants/http-headers.constant";

// Mock axios — the client now talks HTTP via an axios instance (migrated off
// native fetch). We mock the instance returned by axios.create and keep
// axios.isAxiosError working for our synthetic errors.
jest.mock("axios");
const mockedAxios = axios as jest.Mocked<typeof axios>;

/** Build an axios-style error the client's catch block understands. */
function axiosError(
  status: number,
  headers: Record<string, unknown> = {},
): any {
  // Real axios errors are Error instances — mirror that so the client's
  // `throw lastError` rejects with an Error (what `.rejects.toThrow()` expects).
  const err: any = new Error(`Request failed with status code ${status}`);
  err.isAxiosError = true;
  err.response = { status, statusText: `HTTP ${status}`, headers };
  return err;
}

describe("ThemeParksClient", () => {
  let client: ThemeParksClient;
  let mockAxiosInstance: any;
  let redis: any;

  beforeEach(async () => {
    mockAxiosInstance = { get: jest.fn() };
    mockedAxios.create = jest.fn().mockReturnValue(mockAxiosInstance);
    // Real-ish type guard so status detection works under the axios mock.
    (mockedAxios.isAxiosError as unknown) = (e: any): boolean =>
      Boolean(e?.isAxiosError);

    redis = {
      get: jest.fn().mockResolvedValue(null),
      ttl: jest.fn().mockResolvedValue(0),
      set: jest.fn().mockResolvedValue("OK"),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [ThemeParksClient, { provide: REDIS_CLIENT, useValue: redis }],
    }).compile();

    client = module.get<ThemeParksClient>(ThemeParksClient);
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.useRealTimers();
  });

  it("creates the axios instance with browser headers and the Wiki baseURL", () => {
    expect(mockedAxios.create).toHaveBeenCalledWith(
      expect.objectContaining({
        baseURL: "https://api.themeparks.wiki/v1",
        timeout: 20000,
        headers: expect.objectContaining({
          "User-Agent": BROWSER_HEADERS["User-Agent"],
          Accept: BROWSER_HEADERS["Accept"],
          "Accept-Language": BROWSER_HEADERS["Accept-Language"],
        }),
      }),
    );
  });

  describe("happy paths", () => {
    it("getDestinations returns the response body", async () => {
      const body = { destinations: [{ id: "d1" }] };
      mockAxiosInstance.get.mockResolvedValue({ data: body });

      await expect(client.getDestinations()).resolves.toEqual(body);
      expect(mockAxiosInstance.get).toHaveBeenCalledWith("/destinations");
    });

    it("getEntity targets the entity path", async () => {
      mockAxiosInstance.get.mockResolvedValue({ data: { id: "p1" } });

      await expect(client.getEntity("p1")).resolves.toEqual({ id: "p1" });
      expect(mockAxiosInstance.get).toHaveBeenCalledWith("/entity/p1");
    });

    it("getParkLiveData returns the full liveData array", async () => {
      const live = [{ id: "a1" }, { id: "a2" }];
      mockAxiosInstance.get.mockResolvedValue({ data: { liveData: live } });

      await expect(client.getParkLiveData("p1")).resolves.toEqual(live);
      expect(mockAxiosInstance.get).toHaveBeenCalledWith("/entity/p1/live");
    });

    it("getParkLiveData returns [] when liveData is absent", async () => {
      mockAxiosInstance.get.mockResolvedValue({ data: {} });
      await expect(client.getParkLiveData("p1")).resolves.toEqual([]);
    });

    it("getLiveData returns the first liveData element", async () => {
      mockAxiosInstance.get.mockResolvedValue({
        data: { liveData: [{ id: "first" }, { id: "second" }] },
      });
      await expect(client.getLiveData("a1")).resolves.toEqual({ id: "first" });
    });

    it("getScheduleForMonth zero-pads the month", async () => {
      mockAxiosInstance.get.mockResolvedValue({ data: { schedule: [] } });
      await client.getScheduleForMonth("p1", 2026, 3);
      expect(mockAxiosInstance.get).toHaveBeenCalledWith(
        "/entity/p1/schedule/2026/03",
      );
    });
  });

  describe("rate limiting", () => {
    it("fails fast without an HTTP call while a block is active", async () => {
      redis.get.mockResolvedValue("true");
      redis.ttl.mockResolvedValue(7);

      await expect(client.getDestinations()).rejects.toThrow(
        /Global Rate Limit \(blocked for 7s\)/,
      );
      expect(mockAxiosInstance.get).not.toHaveBeenCalled();
    });

    it("sets a distributed block on 429 and honours Retry-After", async () => {
      mockAxiosInstance.get.mockRejectedValue(
        axiosError(429, { "retry-after": "30" }),
      );

      await expect(client.getDestinations()).rejects.toThrow(
        /Rate limit exceeded \(blocked for 30s\)/,
      );
      expect(redis.set).toHaveBeenCalledWith(
        "ratelimit:themeparks:blocked",
        "true",
        "EX",
        30,
      );
      // 429 is terminal — no retry storm.
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(1);
    });

    it("defaults the block to 10s when Retry-After is missing", async () => {
      mockAxiosInstance.get.mockRejectedValue(axiosError(429));

      await expect(client.getDestinations()).rejects.toThrow();
      expect(redis.set).toHaveBeenCalledWith(
        "ratelimit:themeparks:blocked",
        "true",
        "EX",
        10,
      );
    });
  });

  describe("error handling", () => {
    it("does not retry 4xx client errors (e.g. far-future 404)", async () => {
      mockAxiosInstance.get.mockRejectedValue(axiosError(404));

      await expect(client.getScheduleForMonth("p1", 2030, 12)).rejects.toThrow(
        /Failed to fetch .*404/,
      );
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(1);
    });

    it("retries transient 5xx errors up to maxRetries, then throws", async () => {
      jest.useFakeTimers();
      mockAxiosInstance.get.mockRejectedValue(axiosError(503));

      const promise = client.getDestinations();
      const assertion = expect(promise).rejects.toThrow();
      await jest.runAllTimersAsync();
      await assertion;

      // 1 initial attempt + 3 retries.
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(4);
    });

    it("recovers if a retry succeeds after a transient failure", async () => {
      jest.useFakeTimers();
      const body = { destinations: [] };
      mockAxiosInstance.get
        .mockRejectedValueOnce(axiosError(502))
        .mockResolvedValueOnce({ data: body });

      const promise = client.getDestinations();
      await jest.runAllTimersAsync();

      await expect(promise).resolves.toEqual(body);
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(2);
    });
  });
});
