import { isBelowMinimum, parseVersion } from "./app-version.util";

const min = (minVersion: string | null, minBuild: number | null = null) => ({
  minVersion,
  minBuild,
});

describe("isBelowMinimum", () => {
  it("blocks nothing without a minimum", () => {
    expect(isBelowMinimum({ version: "0.1", build: 1 }, min(null, 9))).toBe(
      false,
    );
  });
  it("compares the parts as numbers", () => {
    expect(isBelowMinimum({ version: "1.9.0", build: 1 }, min("1.10"))).toBe(
      true,
    );
    expect(isBelowMinimum({ version: "1.10", build: 1 }, min("1.9.9"))).toBe(
      false,
    );
    expect(isBelowMinimum({ version: "1.0", build: 1 }, min("1.0.0"))).toBe(
      false,
    );
  });
  it("looks at the build only on the same version", () => {
    expect(
      isBelowMinimum({ version: "1.0.1", build: 3 }, min("1.0.1", 4)),
    ).toBe(true);
    expect(
      isBelowMinimum({ version: "1.0.1", build: 4 }, min("1.0.1", 4)),
    ).toBe(false);
    expect(
      isBelowMinimum({ version: "1.1.0", build: 1 }, min("1.0.1", 4)),
    ).toBe(false);
    expect(
      isBelowMinimum({ version: "1.0.0", build: 99 }, min("1.0.1", 4)),
    ).toBe(true);
  });
  it("does not block what it cannot read", () => {
    expect(isBelowMinimum({ version: "beta", build: 1 }, min("1.0"))).toBe(
      false,
    );
    expect(parseVersion("1.2.3.4")).toBeNull();
  });
});
