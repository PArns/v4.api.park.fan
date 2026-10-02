import {
  dayStatusRank,
  formatTodaySchedule,
  pickDayStatusEntry,
} from "./schedule.util";

/**
 * Event rows (TICKETED_EVENT, PRIVATE_EVENT, EXTRA_HOURS, MAINTENANCE, INFO)
 * stay beside a day's OPERATING/CLOSED/UNKNOWN row since PAR-276. The day's
 * rows come back in enum order, which puts the event types before CLOSED and
 * UNKNOWN, so "the first row" is the event on a closed day (PAR-640).
 */
describe("schedule.util — the row that states a day's status", () => {
  const row = (scheduleType: string, open?: string, close?: string) => ({
    scheduleType,
    openingTime: open ? new Date(open) : null,
    closingTime: close ? new Date(close) : null,
  });

  it("ranks OPERATING, CLOSED, UNKNOWN, then every event type", () => {
    expect(dayStatusRank("OPERATING")).toBeLessThan(dayStatusRank("CLOSED"));
    expect(dayStatusRank("CLOSED")).toBeLessThan(dayStatusRank("UNKNOWN"));
    for (const t of [
      "TICKETED_EVENT",
      "PRIVATE_EVENT",
      "EXTRA_HOURS",
      "MAINTENANCE",
      "INFO",
    ]) {
      expect(dayStatusRank("UNKNOWN")).toBeLessThan(dayStatusRank(t));
    }
  });

  describe("pickDayStatusEntry", () => {
    it("picks CLOSED over an event row listed before it", () => {
      expect(
        pickDayStatusEntry([row("TICKETED_EVENT"), row("CLOSED")])
          ?.scheduleType,
      ).toBe("CLOSED");
    });

    it("picks OPERATING over an event row and over CLOSED", () => {
      expect(
        pickDayStatusEntry([
          row("EXTRA_HOURS"),
          row("CLOSED"),
          row("OPERATING"),
        ])?.scheduleType,
      ).toBe("OPERATING");
    });

    it("keeps the first row of a day with only event rows", () => {
      const first = row("TICKETED_EVENT");
      expect(pickDayStatusEntry([first, row("INFO")])).toBe(first);
    });

    it("returns undefined for no rows", () => {
      expect(pickDayStatusEntry([])).toBeUndefined();
      expect(pickDayStatusEntry(null)).toBeUndefined();
    });
  });

  // formatTodaySchedule is the todaySchedule reader of LocationService,
  // FavoritesService, DiscoveryService and ParkEnrichmentService.
  describe("formatTodaySchedule", () => {
    it("reports CLOSED on a closed day that carries an evening event", () => {
      expect(
        formatTodaySchedule([
          row("TICKETED_EVENT", "2026-10-31T18:00:00Z", "2026-10-31T23:00:00Z"),
          row("CLOSED"),
        ]),
      ).toEqual({ openingTime: "", closingTime: "", scheduleType: "CLOSED" });
    });

    it("reports the opening hours, not the event's, on an OPERATING day", () => {
      expect(
        formatTodaySchedule([
          row("OPERATING", "2026-10-31T09:00:00Z", "2026-10-31T17:00:00Z"),
          row("TICKETED_EVENT", "2026-10-31T18:00:00Z", "2026-10-31T23:00:00Z"),
        ]),
      ).toEqual({
        openingTime: "2026-10-31T09:00:00.000Z",
        closingTime: "2026-10-31T17:00:00.000Z",
        scheduleType: "OPERATING",
      });
    });
  });
});
