import { FallbackBuffer } from "./site-analytics-fallback-buffer";

/**
 * What is pinned: counts add up per window and field, a request is held all
 * or nothing, the buffer never holds more fields than it was given, and
 * taking empties it.
 */
describe("FallbackBuffer", () => {
  it("adds counts per window and field, and empties out when taken", () => {
    const buffer = new FallbackBuffer(10);
    expect(
      buffer.add(
        "w1",
        new Map([
          ["a", 1],
          ["b", 2],
        ]),
      ),
    ).toBe(true);
    expect(buffer.add("w1", new Map([["a", 3]]))).toBe(true);
    expect(buffer.add("w2", new Map([["a", 1]]))).toBe(true);
    expect(buffer.size).toBe(3);
    expect(buffer.take()).toEqual([
      [
        "w1",
        new Map([
          ["a", 4],
          ["b", 2],
        ]),
      ],
      ["w2", new Map([["a", 1]])],
    ]);
    expect(buffer.size).toBe(0);
    expect(buffer.take()).toEqual([]);
  });

  it("refuses a request whose new fields would not fit, and keeps none of it", () => {
    const buffer = new FallbackBuffer(3);
    expect(
      buffer.add(
        "w1",
        new Map([
          ["a", 1],
          ["b", 1],
        ]),
      ),
    ).toBe(true);
    expect(
      buffer.add(
        "w1",
        new Map([
          ["c", 1],
          ["d", 1],
        ]),
      ),
    ).toBe(false);
    // A field it already holds costs nothing.
    expect(
      buffer.add(
        "w1",
        new Map([
          ["a", 5],
          ["c", 1],
        ]),
      ),
    ).toBe(true);
    expect(buffer.size).toBe(3);
    expect(buffer.take()).toEqual([
      [
        "w1",
        new Map([
          ["a", 6],
          ["b", 1],
          ["c", 1],
        ]),
      ],
    ]);
  });
});
