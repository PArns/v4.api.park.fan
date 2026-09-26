import { Test, TestingModule } from "@nestjs/testing";
import axios from "axios";
import { ThemeParksClient } from "./themeparks.client";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { BROWSER_HEADERS } from "../../common/constants/http-headers.constant";
import { ThemeParksRateLimitError } from "./themeparks.errors";

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
      del: jest.fn().mockResolvedValue(1),
      incr: jest.fn().mockResolvedValue(1),
      expire: jest.fn().mockResolvedValue(1),
      // The even-spacing reservation: 0 ms of wait unless a test says otherwise.
      eval: jest.fn().mockResolvedValue(0),
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
    it("spaces requests by the ms the reservation returns", async () => {
      jest.useFakeTimers();
      redis.eval.mockResolvedValue(250);
      mockAxiosInstance.get.mockResolvedValue({ data: { destinations: [] } });

      const promise = client.getDestinations();
      await jest.runAllTimersAsync();
      await promise;

      expect(redis.eval).toHaveBeenCalledWith(
        expect.stringContaining("redis.call('SET', KEYS[1], slot, 'PX', 60000)"),
        1,
        "ratelimit:themeparks:slot",
        expect.any(String),
        "250",
      );
    });

    it("waits out an active cooldown and then sends the request", async () => {
      jest.useFakeTimers();
      // Blocked on the first look, clear on the second.
      redis.get.mockResolvedValueOnce("true").mockResolvedValue(null);
      redis.ttl.mockResolvedValue(7);
      const body = { destinations: [] };
      mockAxiosInstance.get.mockResolvedValue({ data: body });

      const promise = client.getDestinations();
      await jest.runAllTimersAsync();

      await expect(promise).resolves.toEqual(body);
      // The point of the whole change: a cooldown costs time, not data.
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(1);
    });

    it("gives up with a rate-limit error when the cooldown outlasts the wait budget", async () => {
      jest.useFakeTimers();
      redis.get.mockResolvedValue("true");
      redis.ttl.mockResolvedValue(90); // 2 × 90s > the 120s budget

      const promise = client.getDestinations();
      const assertion = expect(promise).rejects.toThrow(
        ThemeParksRateLimitError,
      );
      await jest.runAllTimersAsync();
      await assertion;

      expect(mockAxiosInstance.get).not.toHaveBeenCalled();
    });

    it("drops a cooldown key that has no expiry instead of waiting forever", async () => {
      redis.get.mockResolvedValueOnce("true").mockResolvedValue(null);
      redis.ttl.mockResolvedValue(-1);
      mockAxiosInstance.get.mockResolvedValue({ data: { destinations: [] } });

      await expect(client.getDestinations()).resolves.toEqual({
        destinations: [],
      });
      expect(redis.del).toHaveBeenCalledWith("ratelimit:themeparks:blocked");
    });

    it("sets a cooldown on 429 honouring Retry-After, then retries into it", async () => {
      jest.useFakeTimers();
      const body = { destinations: [] };
      mockAxiosInstance.get
        .mockRejectedValueOnce(axiosError(429, { "retry-after": "30" }))
        .mockResolvedValueOnce({ data: body });

      const promise = client.getDestinations();
      await jest.runAllTimersAsync();

      await expect(promise).resolves.toEqual(body);
      expect(redis.set).toHaveBeenCalledWith(
        "ratelimit:themeparks:blocked",
        "true",
        "EX",
        30,
      );
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(2);
    });

    it("caps a hostile Retry-After at 15 minutes", async () => {
      jest.useFakeTimers();
      mockAxiosInstance.get.mockRejectedValue(
        axiosError(429, { "retry-after": "86400" }),
      );

      const promise = client.getDestinations();
      const assertion = expect(promise).rejects.toThrow(
        ThemeParksRateLimitError,
      );
      await jest.runAllTimersAsync();
      await assertion;

      expect(redis.set).toHaveBeenCalledWith(
        "ratelimit:themeparks:blocked",
        "true",
        "EX",
        900,
      );
    });

    it("defaults the cooldown to 10s when Retry-After is missing", async () => {
      jest.useFakeTimers();
      mockAxiosInstance.get.mockRejectedValue(axiosError(429));

      const promise = client.getDestinations();
      const assertion = expect(promise).rejects.toThrow(
        ThemeParksRateLimitError,
      );
      await jest.runAllTimersAsync();
      await assertion;

      expect(redis.set).toHaveBeenCalledWith(
        "ratelimit:themeparks:blocked",
        "true",
        "EX",
        10,
      );
    });

    it("pauses before the 429 when the upstream reports the window nearly spent", async () => {
      mockAxiosInstance.get.mockResolvedValue({
        data: { destinations: [] },
        headers: {
          "ratelimit-limit": "300",
          "ratelimit-remaining": "12",
          "ratelimit-reset": "18",
        },
      });

      await client.getDestinations();

      expect(redis.set).toHaveBeenCalledWith(
        "ratelimit:themeparks:blocked",
        "true",
        "EX",
        18,
      );
    });

    it("does not pause while the window still has room", async () => {
      mockAxiosInstance.get.mockResolvedValue({
        data: { destinations: [] },
        headers: {
          "ratelimit-limit": "300",
          "ratelimit-remaining": "260",
          "ratelimit-reset": "29",
        },
      });

      await client.getDestinations();

      expect(redis.set).not.toHaveBeenCalled();
    });

    it("ignores the budget headers on a CDN hit, whose numbers belong to another window", async () => {
      // Measured 2026-09-26: a Cloudflare HIT with `age: 212` still reported
      // `remaining: 260, reset: 29` — values that were four minutes old. It
      // also never reached the limiter, so it cost no budget at all.
      mockAxiosInstance.get.mockResolvedValue({
        data: { destinations: [] },
        headers: {
          "cf-cache-status": "HIT",
          age: "212",
          "ratelimit-remaining": "3",
          "ratelimit-reset": "29",
        },
      });

      await client.getDestinations();

      expect(redis.set).not.toHaveBeenCalled();
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

  describe("getScheduleExtended", () => {
    /** The 13 month calls plus the generic one all answer with `schedule`. */
    const monthBody = (n: number) => ({
      data: { schedule: Array.from({ length: n }, (_, i) => ({ date: i })) },
    });

    it("throws instead of reporting an empty schedule when the months were throttled", async () => {
      jest.useFakeTimers();
      // The generic call gets through and the source has nothing for those ~30
      // days; every month call after it runs into the cooldown.
      redis.get.mockResolvedValueOnce(null).mockResolvedValue("true");
      redis.ttl.mockResolvedValue(90);
      mockAxiosInstance.get.mockResolvedValue({ data: { schedule: [] } });

      const promise = client.getScheduleExtended("p1", 12);
      const assertion = expect(promise).rejects.toThrow(
        /returned nothing because it was throttled/,
      );
      await jest.runAllTimersAsync();
      await assertion;

      // The caller is told so rather than handed an empty list it would write
      // as "no change" and log as a successful fetch (PAR-480).
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(1);
    });

    it("does not walk the 13 months when the generic call is already throttled", async () => {
      jest.useFakeTimers();
      redis.get.mockResolvedValue("true");
      redis.ttl.mockResolvedValue(90);

      const promise = client.getScheduleExtended("p1", 12);
      const assertion = expect(promise).rejects.toThrow(
        /throttled before it started/,
      );
      await jest.runAllTimersAsync();
      await assertion;
    });

    it("reports what it got when a later month is throttled", async () => {
      jest.useFakeTimers();
      // Clear for the generic call and the first month, cooled down after.
      redis.get
        .mockResolvedValueOnce(null) // generic
        .mockResolvedValueOnce(null) // month -1
        .mockResolvedValue("true"); // every month from here
      redis.ttl.mockResolvedValue(90);
      mockAxiosInstance.get.mockResolvedValue(monthBody(2));

      const promise = client.getScheduleExtended("p1", 12);
      await jest.runAllTimersAsync();
      const result = await promise;

      // Generic (2) + one month (2). Partial, but real — and nothing is
      // deleted for the months that never arrived, because saveScheduleData
      // only touches the dates it was handed.
      expect(result.schedule).toHaveLength(4);
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(2);
    });

    it("returns an empty schedule when the source genuinely publishes none", async () => {
      mockAxiosInstance.get.mockResolvedValue({ data: { schedule: [] } });

      const result = await client.getScheduleExtended("p1", 12);

      expect(result.schedule).toEqual([]);
      // 1 generic + 13 months, all answered, none throttled.
      expect(mockAxiosInstance.get).toHaveBeenCalledTimes(14);
    });
  });
});
