import {
  photoKeyOf,
  pickPushImage,
  pushImageUrl,
  resetPushMediaCache,
  type MediaIndex,
} from "./push-media";

const INDEX: MediaIndex = {
  pattern: "{id}/{v}-{width}.webp",
  parks: { "europe/germany/bruehl/phantasialand": "phantasialand/background" },
  rides: { "europe/germany/bruehl/phantasialand/taron": "taron/01" },
  images: {
    "taron/01": {
      v: "ab12",
      widths: [400, 828, 1200, 1600],
      attribution: false,
    },
    "phantasialand/background": {
      v: "cd34",
      widths: [828, 400, 1024],
      attribution: false,
    },
    "small/one": { v: "ef56", widths: [400, 768], attribution: false },
    "credited/one": { v: "aa11", widths: [400, 1200], attribution: true },
    "odd/../one": { v: "aa11", widths: [1200], attribution: false },
  },
};
const BASE = "https://media.park.fan";

describe("pickPushImage", () => {
  it("finds a ride, a park and a cover id, at the first width from 1000 px up", () => {
    expect(
      pickPushImage(INDEX, "europe/germany/bruehl/phantasialand/taron", BASE),
    ).toBe("https://media.park.fan/taron/01/ab12-1200.webp");
    expect(
      pickPushImage(INDEX, "europe/germany/bruehl/phantasialand", BASE),
    ).toBe("https://media.park.fan/phantasialand/background/cd34-1024.webp");
    expect(pickPushImage(INDEX, "taron/01", BASE)).toMatch(/ab12-1200\.webp$/);
  });

  it("takes the largest width of a photo that has none that wide", () => {
    expect(pickPushImage(INDEX, "small/one", BASE)).toMatch(/ef56-768\.webp$/);
  });

  it("sends no photo that needs its credit shown, and none it does not know", () => {
    expect(pickPushImage(INDEX, "credited/one", BASE)).toBeNull();
    expect(pickPushImage(INDEX, "europe/x/y/z", BASE)).toBeNull();
    expect(pickPushImage({}, "taron/01", BASE)).toBeNull();
    // `attribution` missing is not "needs none".
    expect(
      pickPushImage({ images: { a: { v: "x1", widths: [1200] } } }, "a", BASE),
    ).toBeNull();
  });

  it("refuses an id that is not a path of the bucket", () => {
    expect(pickPushImage(INDEX, "odd/../one", BASE)).toBeNull();
  });
});

describe("pushImageUrl", () => {
  const fetchMock = jest.fn();
  const realFetch = global.fetch;
  beforeEach(() => {
    resetPushMediaCache();
    fetchMock.mockReset();
    global.fetch = fetchMock as unknown as typeof fetch;
    delete process.env.PUSH_IMAGES;
  });
  afterAll(() => {
    global.fetch = realFetch;
  });

  it("reads the index once for many sends", async () => {
    fetchMock.mockResolvedValue({ ok: true, json: async () => INDEX });
    expect(await pushImageUrl("taron/01", 0)).toMatch(/ab12-1200\.webp$/);
    expect(await pushImageUrl("small/one", 60_000)).toMatch(/ef56-768/);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("sends without a photo when the index cannot be read, and tries again later", async () => {
    fetchMock.mockRejectedValueOnce(new Error("down"));
    expect(await pushImageUrl("taron/01", 0)).toBeNull();
    expect(await pushImageUrl("taron/01", 60_000)).toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    fetchMock.mockResolvedValue({ ok: true, json: async () => INDEX });
    expect(await pushImageUrl("taron/01", 6 * 60_000)).not.toBeNull();
  });

  it("is switched off with PUSH_IMAGES=false", async () => {
    process.env.PUSH_IMAGES = "false";
    expect(await pushImageUrl("taron/01", 0)).toBeNull();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

describe("photoKeyOf", () => {
  it("is the geo path of a park or ride URL", () => {
    expect(photoKeyOf("/parks/europe/germany/rust/europa-park#shows")).toBe(
      "europe/germany/rust/europa-park",
    );
    expect(photoKeyOf("/")).toBeUndefined();
  });
});
