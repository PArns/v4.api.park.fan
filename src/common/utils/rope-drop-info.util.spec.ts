import { buildRopeDropInfo } from "./rope-drop-info.util";
import { RopeDropStored } from "../types/rope-drop.type";

const stored: RopeDropStored = {
  worth: false,
  strength: null,
  confidence: "high",
  busyPeak: 150,
  openWait: 100,
  savings: 50,
  rideByMinutesAfterOpen: 0,
  bestSlotMinutesAfterOpen: 825,
  bestSlotWait: 25,
  endOfDayWorth: true,
  endOfDaySavings: 125,
  byDaytype: {
    weekend: { openWait: 100, busyPeak: 153, savings: 53 },
    weekday: { openWait: 100, busyPeak: 150, savings: 50 },
  },
};

// A 10:00–21:00 ET day (14:00–01:00 UTC).
const OPENING = "2026-06-11T14:00:00.000Z";
const CLOSING = "2026-06-12T01:00:00.000Z";

describe("buildRopeDropInfo", () => {
  it("resolves offsets against the opening instant", () => {
    const info = buildRopeDropInfo(stored, OPENING);
    expect(info.rideByUtc).toBe("2026-06-11T14:00:00.000Z"); // +0 min
    expect(info.bestSlotUtc).toBe("2026-06-12T03:45:00.000Z"); // +825 min, unclamped
  });

  it("returns null instants without an opening time", () => {
    const info = buildRopeDropInfo(stored, null);
    expect(info.rideByUtc).toBeNull();
    expect(info.bestSlotUtc).toBeNull();
  });

  it("clamps instants past closing to closing minus the guard buffer", () => {
    // Offset 825 from longer historical days resolves to 03:45 UTC — 2h45 past
    // this day's close. The clamp pins it to 00:30 UTC (closing − 30 min).
    const info = buildRopeDropInfo(stored, OPENING, CLOSING);
    expect(info.bestSlotUtc).toBe("2026-06-12T00:30:00.000Z");
    // rideBy (+0 min) is inside the day — untouched.
    expect(info.rideByUtc).toBe("2026-06-11T14:00:00.000Z");
  });

  it("leaves instants inside the operating day unclamped", () => {
    const early = { ...stored, bestSlotMinutesAfterOpen: 585 }; // 19:45 ET
    const info = buildRopeDropInfo(early, OPENING, CLOSING);
    expect(info.bestSlotUtc).toBe("2026-06-11T23:45:00.000Z");
  });

  it("never clamps below the opening on degenerate short days", () => {
    // Closing 15 min after opening: clampMax = max(base, close − 30) = base.
    const shortClose = "2026-06-11T14:15:00.000Z";
    const info = buildRopeDropInfo(stored, OPENING, shortClose);
    expect(info.bestSlotUtc).toBe(OPENING);
  });

  it("ignores an absent closing time", () => {
    const info = buildRopeDropInfo(stored, OPENING, null);
    expect(info.bestSlotUtc).toBe("2026-06-12T03:45:00.000Z");
  });
});
