import {
  retractClosedParkOverrides,
  scheduleSaysClosedNow,
} from "./schedule-closed-now.util";

describe("scheduleSaysClosedNow (PAR-1236)", () => {
  const now = new Date("2026-11-05T21:00:00Z");

  it("is closed on a day the schedule calls CLOSED", () => {
    expect(scheduleSaysClosedNow([{ scheduleType: "CLOSED" }], now)).toBe(true);
  });

  it("is open inside an OPERATING window and within the hour around it", () => {
    const day = {
      scheduleType: "OPERATING",
      openingTime: "2026-11-05T10:00:00Z",
      closingTime: "2026-11-05T20:30:00Z",
    };
    expect(scheduleSaysClosedNow([day], now)).toBe(false);
    expect(scheduleSaysClosedNow([day], new Date("2026-11-05T21:31:00Z"))).toBe(
      true,
    );
  });

  it("cannot say when there is no decisive row or no times", () => {
    expect(scheduleSaysClosedNow([], now)).toBe(false);
    expect(scheduleSaysClosedNow([{ scheduleType: "UNKNOWN" }], now)).toBe(
      false,
    );
    expect(scheduleSaysClosedNow([{ scheduleType: "OPERATING" }], now)).toBe(
      false,
    );
  });
});

describe("retractClosedParkOverrides (PAR-1236)", () => {
  const now = new Date("2026-11-05T21:00:00Z");

  it("takes back the override, a queue-times-only wait and a months-old wiki entry; keeps a fresh wiki reading", () => {
    const entities: Array<{
      status: string;
      rawStatus?: string;
      lastUpdated?: string;
      source?: string;
    }> = [
      // Kennywood: wiki CLOSED, a frozen queue-times wait turned it OPERATING
      {
        status: "OPERATING",
        rawStatus: "CLOSED",
        lastUpdated: "2026-11-05T20:50:00Z",
        source: "themeparks-wiki",
      },
      // Energylandia: queue-times alone, a fresh timestamp on last weekend's wait
      {
        status: "OPERATING",
        lastUpdated: "2026-11-05T20:50:00Z",
        source: "queue-times",
      },
      // LEGOLAND Billund: the wiki's entry last changed in April
      {
        status: "OPERATING",
        lastUpdated: "2026-04-06T13:54:04Z",
        source: "themeparks-wiki",
      },
      // the wiki measured it minutes ago: a park running despite the schedule
      {
        status: "OPERATING",
        lastUpdated: "2026-11-05T20:55:00Z",
        source: "themeparks-wiki",
      },
      { status: "CLOSED", source: "queue-times" },
    ];

    expect(retractClosedParkOverrides(entities, now)).toBe(3);
    expect(entities.map((e) => e.status)).toEqual([
      "CLOSED",
      "CLOSED",
      "CLOSED",
      "OPERATING",
      "CLOSED",
    ]);
    expect(entities[0]).not.toHaveProperty("rawStatus");
  });
});
