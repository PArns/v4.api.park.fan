import axios from "axios";
import { ChronosShadowService } from "./chronos-shadow.service";

jest.mock("axios", () => {
  const actual = jest.requireActual("axios");
  return { __esModule: true, default: { ...actual, post: jest.fn() } };
});
const post = axios.post as jest.Mock;

describe("ChronosShadowService", () => {
  const origin = new Date("2026-10-08T08:05:00Z");
  let service: ChronosShadowService;
  let warn: jest.SpyInstance;

  beforeEach(() => {
    post.mockReset();
    delete process.env.CHRONOS_SHADOW_ENABLED;
    service = new ChronosShadowService();
    warn = jest
      .spyOn(
        (service as unknown as { logger: { warn: () => void } }).logger,
        "warn",
      )
      .mockImplementation(() => undefined);
    service.beginRun();
  });

  it("posts the origin as an instant and returns the curves", async () => {
    post.mockResolvedValue({ data: { status: "ok", curves: [{ x: 1 }] } });
    const res = await service.forecast("p1", origin, "live", 240);
    expect(res?.curves).toHaveLength(1);
    expect(post.mock.calls[0][1]).toEqual({
      park_id: "p1",
      origin_at: "2026-10-08T08:05:00.000Z",
      variant: "live",
      keep_minutes: 240,
    });
    expect(post.mock.calls[0][2]).toEqual({
      timeout: ChronosShadowService.TIMEOUT_MS,
    });
  });

  it("is switched off by CHRONOS_SHADOW_ENABLED=false (no call at all)", async () => {
    process.env.CHRONOS_SHADOW_ENABLED = "false";
    expect(await service.forecast("p1", origin, "grid")).toBeNull();
    expect(post).not.toHaveBeenCalled();
  });

  it("answers null while warming, without counting a failure", async () => {
    post.mockResolvedValue({ data: { status: "warming", curves: [] } });
    expect(await service.forecast("p1", origin, "grid")).toBeNull();
    expect(service.endRun()).toEqual({
      calls: 1,
      ok: 0,
      failures: 0,
      warming: 1,
      skipped: 0,
    });
  });

  it("opens the breaker after three failures and logs once per run", async () => {
    post.mockRejectedValue(new Error("timeout of 45000ms exceeded"));
    for (let i = 0; i < 6; i++) {
      expect(await service.forecast(`p${i}`, origin, "live")).toBeNull();
    }
    expect(post).toHaveBeenCalledTimes(3);
    expect(warn).toHaveBeenCalledTimes(1);
    expect(service.endRun()).toEqual({
      calls: 3,
      ok: 0,
      failures: 3,
      warming: 0,
      skipped: 3,
    });
    // A new run closes the breaker again.
    service.beginRun();
    post.mockResolvedValue({ data: { status: "ok", curves: [] } });
    expect(await service.forecast("p1", origin, "live")).not.toBeNull();
  });

  it("treats a 503 (switched off in nf-service, TFT training) as a skip, not a failure", async () => {
    const err = Object.assign(new Error("503"), {
      isAxiosError: true,
      response: { status: 503 },
    });
    post.mockRejectedValue(err);
    for (let i = 0; i < 5; i++) {
      await service.forecast("p1", origin, "live");
    }
    expect(post).toHaveBeenCalledTimes(5);
    expect(service.endRun()).toMatchObject({ failures: 0, skipped: 5 });
  });
});
