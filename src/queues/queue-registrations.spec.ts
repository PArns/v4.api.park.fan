import { readdirSync, readFileSync } from "fs";
import { join } from "path";
import { BULL_QUEUE_REGISTRATIONS, bullQueue } from "./queue-registrations";

const SRC = join(__dirname, "..");

function moduleFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) return moduleFiles(path);
    return entry.name.endsWith(".module.ts") ? [path] : [];
  });
}

describe("queue registrations", () => {
  it("hands out the registration with its settings", () => {
    expect(bullQueue("facilities").settings?.maxStalledCount).toBe(3);
    expect(bullQueue("pcn-shadow").settings?.maxStalledCount).toBe(3);
    expect(bullQueue("children-metadata")).toBe(
      BULL_QUEUE_REGISTRATIONS.find((q) => q.name === "children-metadata"),
    );
  });

  it("refuses a queue that is not registered", () => {
    expect(() => bullQueue("no-such-queue")).toThrow("no-such-queue");
  });

  // A registration by name alone builds a queue on Bull's defaults, and the
  // processor may be bound to that one (PAR-1458).
  it("no module registers a queue with options of its own", () => {
    const arg = String.raw`(?:bullQueue\("[a-z0-9-]+"\)|\.\.\.BULL_QUEUE_REGISTRATIONS)`;
    // Any argument that is neither of the two, after however many that are.
    const foreign = new RegExp(
      String.raw`registerQueue\((?:\s*${arg},?)*\s*(?![),]|${arg})\S`,
    );
    const files = moduleFiles(SRC);
    expect(files.length).toBeGreaterThan(8);
    expect(foreign.test('registerQueue(bullQueue("a"), { name: "b" })')).toBe(
      true,
    );
    expect(
      files.filter((file) => foreign.test(readFileSync(file, "utf8"))),
    ).toEqual([]);
  });
});
