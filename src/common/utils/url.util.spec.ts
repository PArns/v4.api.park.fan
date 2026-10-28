import { withoutTrailingSlashes } from "./url.util";

describe("withoutTrailingSlashes", () => {
  it("drops one or many trailing slashes and nothing else", () => {
    expect(withoutTrailingSlashes("/de/parks/")).toBe("/de/parks");
    expect(withoutTrailingSlashes("https://api.park.fan///")).toBe(
      "https://api.park.fan",
    );
    expect(withoutTrailingSlashes("/de//parks")).toBe("/de//parks");
    expect(withoutTrailingSlashes("/")).toBe("");
  });

  it("stays linear on a long run of slashes", () => {
    const slashes = "/".repeat(200_000);
    const started = Date.now();
    expect(withoutTrailingSlashes(`${slashes}x`)).toBe(`${slashes}x`);
    expect(withoutTrailingSlashes(`x${slashes}`)).toBe("x");
    expect(Date.now() - started).toBeLessThan(500);
  });
});
