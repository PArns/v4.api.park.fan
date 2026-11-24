import { ScheduleEntry } from "../../parks/entities/schedule-entry.entity";
import { isParkOpen } from "./status-calculator.util";

const entry = (
  scheduleType: string,
  openingTime: Date | null = null,
  closingTime: Date | null = null,
): ScheduleEntry =>
  ({ scheduleType, openingTime, closingTime }) as unknown as ScheduleEntry;

/** What a feed keeps reporting on a shut day: rides OPERATING at a placeholder 5 minutes. */
const placeholderRides = [
  { status: "OPERATING", waitTime: 5, lastUpdated: new Date() },
  { status: "OPERATING", waitTime: 5, lastUpdated: new Date() },
];

describe("isParkOpen", () => {
  it("keeps a park shut on days it published as CLOSED, whatever its rides report", () => {
    const shutDays = [entry("CLOSED"), entry("CLOSED"), entry("CLOSED")];
    expect(isParkOpen(shutDays, placeholderRides)).toBe(false);
  });

  it("opens a park inside an OPERATING window", () => {
    const now = Date.now();
    const open = entry(
      "OPERATING",
      new Date(now - 60 * 60 * 1000),
      new Date(now + 60 * 60 * 1000),
    );
    expect(isParkOpen([open])).toBe(true);
  });

  it("opens a park inside an OPERATING window even with CLOSED days around it", () => {
    const now = Date.now();
    const open = entry(
      "OPERATING",
      new Date(now - 60 * 60 * 1000),
      new Date(now + 60 * 60 * 1000),
    );
    expect(isParkOpen([entry("CLOSED"), open], [])).toBe(true);
  });

  it("does not let gap-fill UNKNOWN or event-only rows silence the rides", () => {
    for (const type of ["UNKNOWN", "EXTRA_HOURS", "TICKETED_EVENT", "INFO"]) {
      expect(isParkOpen([entry(type)], placeholderRides)).toBe(true);
    }
  });

  it("falls back to the rides only for a park with no schedule at all", () => {
    expect(isParkOpen([], placeholderRides)).toBe(true);
    expect(isParkOpen([], [])).toBe(false);
  });
});
