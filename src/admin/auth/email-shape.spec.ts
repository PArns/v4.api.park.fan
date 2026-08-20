import { looksLikeEmailAddress } from "./admin-auth.service";

/**
 * The shape check that replaced a regex.
 *
 * `/^[^@\s]+@[^@\s]+\.[^@\s]+$/` is a backtracking trap — the two adjacent
 * `[^@\s]+` around the dot make matching super-linear in the number of dots —
 * so the rejection path became the expensive one. These tests pin both the
 * behaviour it replaced and the input that made it worth replacing.
 */
describe("looksLikeEmailAddress", () => {
  it.each([
    "you@park.fan",
    "first.last@sub.example.co.uk",
    "a@b.c",
    "with+tag@park.fan",
    "hyphen-ated@park-fan.de",
  ])("accepts %s", (value) => {
    expect(looksLikeEmailAddress(value)).toBe(true);
  });

  it.each([
    ["empty", ""],
    ["no @", "youpark.fan"],
    ["two @", "you@other@park.fan"],
    ["nothing before the @", "@park.fan"],
    ["nothing after the @", "you@"],
    ["no dot in the domain", "you@parkfan"],
    ["a domain that starts with the dot", "you@.fan"],
    ["a domain that ends with the dot", "you@park."],
    ["a space in the local part", "you and me@park.fan"],
    ["a tab in the domain", "you@park\t.fan"],
  ])("rejects %s", (_label, value) => {
    expect(looksLikeEmailAddress(value)).toBe(false);
  });

  it("rejects anything past the column's ceiling", () => {
    expect(looksLikeEmailAddress(`${"a".repeat(320)}@park.fan`)).toBe(false);
  });

  it("rejects the pathological input in linear time", () => {
    // The shape CodeQL flagged: a prefix that can never match, followed by
    // thousands of dots. Against the old regex this backtracked; the assertion
    // that matters here is the clock, not the verdict.
    const hostile = `a@!@!.${"!.".repeat(20_000)}`;
    const started = process.hrtime.bigint();
    expect(looksLikeEmailAddress(hostile)).toBe(false);
    const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6;
    expect(elapsedMs).toBeLessThan(50);
  });
});
