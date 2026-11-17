import axios from "axios";
import { RevalidationService } from "./revalidation.service";
import { forecastCacheTags } from "./park-status-transitions";

jest.mock("axios");
const mockedPost = axios.post as jest.Mock;

/**
 * The revalidation webhook must be inert unless a secret is configured (so dev
 * / test / CI never ping the production frontend), and a forecast warmup rings
 * each park's best-days snapshot and calendar together.
 */
const PHANTASIALAND = {
  id: "p1",
  slug: "phantasialand",
  continentSlug: "europe",
  countrySlug: "germany",
  citySlug: "bruehl",
};
const EFTELING = {
  id: "p2",
  slug: "efteling",
  continentSlug: "europe",
  countrySlug: "netherlands",
  citySlug: "kaatsheuvel",
};

describe("RevalidationService", () => {
  const ORIGINAL_ENV = process.env;
  let service: RevalidationService;

  beforeEach(() => {
    jest.resetAllMocks();
    process.env = { ...ORIGINAL_ENV };
    service = new RevalidationService();
  });

  afterAll(() => {
    process.env = ORIGINAL_ENV;
  });

  it("is a no-op (no POST) when REVALIDATE_SECRET is unset", async () => {
    delete process.env.REVALIDATE_SECRET;

    const ok = await service.revalidateTags(forecastCacheTags(PHANTASIALAND));

    expect(ok).toBe(false);
    expect(mockedPost).not.toHaveBeenCalled();
  });

  it("does nothing for an empty tag list even when configured", async () => {
    process.env.REVALIDATE_SECRET = "s3cret";

    const ok = await service.revalidateTags([]);

    expect(ok).toBe(false);
    expect(mockedPost).not.toHaveBeenCalled();
  });

  it("POSTs a park's forecast tags with a Bearer auth header when configured", async () => {
    process.env.REVALIDATE_SECRET = "s3cret";
    process.env.REVALIDATE_URL = "https://park.fan/api/revalidate";
    mockedPost.mockResolvedValue({ status: 200 });

    const ok = await service.revalidateTags(
      [PHANTASIALAND, EFTELING].flatMap(forecastCacheTags),
      { immediate: true },
    );

    expect(ok).toBe(true);
    expect(mockedPost).toHaveBeenCalledTimes(1);
    const [url, body, config] = mockedPost.mock.calls[0];
    expect(url).toBe("https://park.fan/api/revalidate");
    expect(body).toEqual({
      tags: [
        "best-days:phantasialand",
        "park-calendar:europe/germany/bruehl/phantasialand",
        "best-days:efteling",
        "park-calendar:europe/netherlands/kaatsheuvel/efteling",
      ],
      expire: 0,
    });
    // Frontend expects `Authorization: Bearer <secret>` (not a custom header).
    expect(config.headers.Authorization).toBe("Bearer s3cret");
  });

  it("chunks tags at the frontend's 50-per-request limit", async () => {
    process.env.REVALIDATE_SECRET = "s3cret";
    mockedPost.mockResolvedValue({ status: 200 });

    // 120 distinct tags → 50 + 50 + 20 across three POSTs (none dropped).
    const tags = Array.from({ length: 120 }, (_, i) => `t${i}`);
    const ok = await service.revalidateTags(tags);

    expect(ok).toBe(true);
    expect(mockedPost).toHaveBeenCalledTimes(3);
    expect(mockedPost.mock.calls[0][1].tags).toHaveLength(50);
    expect(mockedPost.mock.calls[1][1].tags).toHaveLength(50);
    expect(mockedPost.mock.calls[2][1].tags).toHaveLength(20);
    // Every tag is covered exactly once.
    const sent = mockedPost.mock.calls.flatMap((c) => c[1].tags);
    expect(new Set(sent).size).toBe(120);
  });

  it("dedupes tags and reports success even if one batch fails", async () => {
    process.env.REVALIDATE_SECRET = "s3cret";
    mockedPost.mockResolvedValue({ status: 200 });

    const ok = await service.revalidateTags(["a", "a", "b"]);

    expect(ok).toBe(true);
    expect(mockedPost.mock.calls[0][1]).toEqual({ tags: ["a", "b"] });
  });

  it("sends no expire by default — stale-while-revalidate is the frontend's default profile", async () => {
    process.env.REVALIDATE_SECRET = "s3cret";
    mockedPost.mockResolvedValue({ status: 200 });

    await service.revalidateTags(["parks"]);

    expect(mockedPost.mock.calls[0][1]).not.toHaveProperty("expire");
  });

  it("sends expire:0 when the caller cannot live with one more stale answer", async () => {
    process.env.REVALIDATE_SECRET = "s3cret";
    mockedPost.mockResolvedValue({ status: 200 });

    await service.revalidateTags(["park:europe/germany/bruehl/phantasialand"], {
      immediate: true,
    });

    expect(mockedPost.mock.calls[0][1]).toEqual({
      tags: ["park:europe/germany/bruehl/phantasialand"],
      expire: 0,
    });
  });

  it("swallows a failed POST (best-effort) and returns false", async () => {
    process.env.REVALIDATE_SECRET = "s3cret";
    mockedPost.mockRejectedValue(new Error("frontend down"));

    const ok = await service.revalidateTags(forecastCacheTags(PHANTASIALAND));

    expect(ok).toBe(false); // did not throw
  });
});
