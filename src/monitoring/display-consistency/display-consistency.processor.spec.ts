import { Logger } from "@nestjs/common";
import type { Job } from "bull";
import { DisplayConsistencyProcessor } from "./display-consistency.processor";
import type {
  DisplayConsistencyService,
  DisplayConsistencySummary,
} from "./display-consistency.service";

const park = (
  name: string,
  mismatches: number,
  failures: number,
): DisplayConsistencySummary["parks"][number] => ({
  id: name,
  name,
  mismatches,
  failures,
});

/** The run's log line for a summary, and at which level. */
async function logged(parks: DisplayConsistencySummary["parks"]) {
  const summary: DisplayConsistencySummary = {
    checkedAt: "2026-11-17T23:40:00.000Z",
    days: 9,
    mismatches: parks.reduce((sum, p) => sum + p.mismatches, 0),
    failures: parks.reduce((sum, p) => sum + p.failures, 0),
    parks,
  };
  const log = jest.spyOn(Logger.prototype, "log").mockImplementation();
  const warn = jest.spyOn(Logger.prototype, "warn").mockImplementation();
  const processor = new DisplayConsistencyProcessor({
    runDaily: jest.fn().mockResolvedValue(summary),
  } as unknown as DisplayConsistencyService);
  await processor.handle({} as Job);
  const result = {
    log: log.mock.calls.map(([line]) => String(line)),
    warn: warn.mock.calls.map(([line]) => String(line)),
  };
  log.mockRestore();
  warn.mockRestore();
  return result;
}

describe("DisplayConsistencyProcessor", () => {
  it("logs the quiet line only when every park was read whole and agrees", async () => {
    const { log, warn } = await logged([park("Efteling", 0, 0)]);
    expect(warn).toEqual([]);
    expect(log).toEqual([
      expect.stringContaining("✅ Display consistency: 1 parks"),
    ]);
  });

  it("names the parks with mismatches", async () => {
    const { log, warn } = await logged([
      park("Phantasialand", 2, 0),
      park("Efteling", 0, 0),
    ]);
    expect(log).toEqual([]);
    expect(warn).toEqual([
      expect.stringContaining(
        "2 mismatches in 1 and 0 unreadable surfaces in 0 of 2 parks",
      ),
    ]);
    expect(warn[0]).toContain("Phantasialand 2 mismatches.");
  });

  it("warns about a run in which every surface failed, though nothing was compared", async () => {
    const { log, warn } = await logged([
      park("Phantasialand", 0, 2),
      park("Efteling", 1, 1),
    ]);
    expect(log).toEqual([]);
    expect(warn).toEqual([
      expect.stringContaining(
        "1 mismatches in 1 and 3 unreadable surfaces in 2 of 2 parks",
      ),
    ]);
    expect(warn[0]).toContain(
      "Phantasialand 2 unreadable, Efteling 1 mismatches 1 unreadable.",
    );
  });

  it("warns when no park was checked", async () => {
    const { log, warn } = await logged([]);
    expect(log).toEqual([]);
    expect(warn).toEqual([expect.stringContaining("no park checked")]);
  });
});
