import {
  buildPlanDayFacts,
  isDigestDue,
  nextBaseline,
  planChanges,
  planDaysToWatch,
  URGENT_GAP_MS,
  WAIT_GAP_MS,
  type FactsInput,
  type PlanDayFacts,
  type PlanDayToWatch,
} from "./plan-change";
import { writePlanChangeMessage } from "./push-messages";
import { pushKindOf } from "./push-config";

// 2026-10-17 12:00 in Europe/Berlin (CEST, UTC+2).
const NOW = Date.parse("2026-10-17T10:00:00Z");
const TZ = "Europe/Berlin";
const at = (hhmm: string, date = "2026-10-17") =>
  new Date(`${date}T${hhmm}:00+02:00`);

const TARON = {
  id: "e1",
  startMinute: 14 * 60,
  attractionSlug: "taron",
  attractionName: "Taron",
};
const SHOW = {
  id: "e2",
  startMinute: 15 * 60,
  showSlug: "feuerwerk",
  custom: { label: "Feuerwerk" },
};

function day(patch: Partial<PlanDayToWatch> = {}): PlanDayToWatch {
  return {
    slug: "phantasialand",
    name: "Phantasialand",
    timezone: TZ,
    geo: { continent: "europe", country: "germany", city: "bruehl" },
    date: "2026-10-17",
    isToday: true,
    daysAhead: 0,
    nowMinute: 12 * 60,
    entries: [TARON, SHOW],
    ...patch,
  };
}

const OPEN = [
  {
    scheduleType: "OPERATING",
    openingTime: at("09:00"),
    closingTime: at("18:00"),
  },
];

function input(patch: Partial<FactsInput> = {}): FactsInput {
  return {
    day: day(),
    nowMs: NOW,
    schedule: OPEN,
    planDay: {
      shows: [{ showSlug: "feuerwerk", times: ["15:00"], source: "scheduled" }],
      rides: [
        {
          attractionSlug: "taron",
          attractionId: "a-taron",
          hours: [{ hour: 14, wait: 30 }],
          slots: [{ minute: 14 * 60, wait: 30 }],
        },
      ],
    },
    standby: new Map([
      [
        "a-taron",
        { status: "OPERATING", isHeartbeat: false, timestampMs: NOW - 60_000 },
      ],
    ]),
    previous: null,
    ...patch,
  };
}

const facts = (patch: Partial<FactsInput> = {}) =>
  buildPlanDayFacts(input(patch));
const changes = (base: PlanDayFacts | null, cur: PlanDayFacts) =>
  planChanges(base, cur, day().entries);

describe("planDaysToWatch", () => {
  const payload = {
    version: 3,
    parks: {
      phantasialand: {
        slug: "phantasialand",
        name: "Phantasialand",
        timezone: TZ,
        geo: { continent: "europe", country: "germany", city: "bruehl" },
        days: {
          "2026-10-16": { date: "2026-10-16", entries: [TARON] },
          "2026-10-17": { date: "2026-10-17", entries: [TARON] },
          "2026-10-20": { date: "2026-10-20", entries: [TARON] },
          "2026-10-21": { date: "2026-10-21", entries: [] },
          "2027-06-01": { date: "2027-06-01", entries: [TARON] },
        },
      },
      nogeo: { slug: "nogeo", timezone: TZ, days: {} },
    },
  };

  it("watches the park's today and later days that hold something", () => {
    const days = planDaysToWatch(payload, NOW);
    expect(days.map((d) => [d.date, d.isToday, d.daysAhead])).toEqual([
      ["2026-10-17", true, 0],
      ["2026-10-20", false, 3],
    ]);
    expect(days[0].nowMinute).toBe(12 * 60);
  });

  it("reads nothing from a malformed payload", () => {
    expect(planDaysToWatch(null, NOW)).toEqual([]);
    expect(planDaysToWatch({ parks: [] }, NOW)).toEqual([]);
  });
});

describe("rule (a): hours", () => {
  const base = facts();

  it("reads the day's hours on the park's clock", () => {
    expect(base.hours).toBe("09:00–18:00");
  });

  it("announces a moved opening or closing", () => {
    const cur = facts({
      schedule: [{ ...OPEN[0], closingTime: at("17:00") }],
    });
    expect(changes(base, cur)).toContainEqual({
      type: "hours",
      from: "09:00–18:00",
      to: "09:00–17:00",
    });
  });

  it("announces a day that turns CLOSED", () => {
    const cur = facts({
      schedule: [
        { scheduleType: "CLOSED", openingTime: null, closingTime: null },
      ],
    });
    expect(cur.hours).toBe("closed");
    expect(changes(base, cur)).toContainEqual({ type: "closed" });
  });

  it("says nothing when the hours become unknown or were unknown", () => {
    const unknown = facts({ schedule: [] });
    expect(unknown.hours).toBeUndefined();
    expect(changes(base, unknown).some((c) => c.type === "hours")).toBe(false);
    expect(changes(unknown, base).some((c) => c.type === "hours")).toBe(false);
  });
});

describe("rule (b): shows", () => {
  const moved = (times: string[], source = "scheduled") =>
    facts({
      planDay: {
        ...input().planDay!,
        shows: [{ showSlug: "feuerwerk", times, source }],
      },
    });

  it("is quiet while the show runs at the planned time", () => {
    expect(changes(null, facts())).toEqual([]);
  });

  it("announces the nearest time when the planned one is gone", () => {
    expect(changes(null, moved(["12:30", "15:30", "20:00"]))).toEqual([
      { type: "show-moved", name: "Feuerwerk", to: "15:30" },
    ]);
  });

  it("announces a move back to the planned time after a move was announced", () => {
    const base = moved(["15:30"]);
    expect(changes(base, facts())).toEqual([
      { type: "show-moved", name: "Feuerwerk", to: "15:00" },
    ]);
  });

  it("reads a projection as no information", () => {
    expect(moved(["16:00"], "projected").shows).toEqual({});
  });

  it("calls a scheduled show that drops out of a scheduled day cancelled", () => {
    const base = facts();
    const cur = facts({
      previous: base,
      planDay: {
        ...input().planDay!,
        shows: [{ showSlug: "parade", times: ["13:00"], source: "scheduled" }],
      },
    });
    expect(changes(base, cur)).toEqual([
      { type: "show-cancelled", name: "Feuerwerk" },
    ]);
  });

  it("does not call a show cancelled it never saw scheduled", () => {
    const cur = facts({
      planDay: {
        ...input().planDay!,
        shows: [{ showSlug: "parade", times: ["13:00"], source: "scheduled" }],
      },
    });
    expect(cur.shows).toEqual({});
  });

  it("only reads shows on the day itself", () => {
    expect(
      facts({ day: day({ isToday: false, daysAhead: 1, date: "2026-10-18" }) })
        .shows,
    ).toEqual({});
  });
});

describe("rule (c): rides today", () => {
  const reading = (status: string, extra: object = {}) =>
    new Map([
      [
        "a-taron",
        { status, isHeartbeat: false, timestampMs: NOW - 60_000, ...extra },
      ],
    ]);

  it("announces a planned ride going down and coming back", () => {
    const open = facts();
    const down = facts({ standby: reading("DOWN") });
    expect(open.rides).toEqual({ e1: "open" });
    expect(changes(open, down)).toContainEqual({
      type: "ride-down",
      name: "Taron",
    });
    expect(changes(down, open)).toContainEqual({
      type: "ride-up",
      name: "Taron",
    });
  });

  it("does not trust a stale or heartbeat OPERATING row", () => {
    expect(
      facts({ standby: reading("OPERATING", { isHeartbeat: true }) }).rides,
    ).toEqual({});
    expect(
      facts({
        standby: reading("OPERATING", { timestampMs: NOW - 20 * 60_000 }),
      }).rides,
    ).toEqual({});
  });

  it("is silent while the park is closed and for a ride already ridden or passed", () => {
    const closed = facts({
      schedule: [{ ...OPEN[0], openingTime: at("13:00") }],
      standby: reading("DOWN"),
    });
    expect(closed.rides).toEqual({});
    const done = facts({
      day: day({ entries: [{ ...TARON, done: true }] }),
      standby: reading("DOWN"),
    });
    expect(done.rides).toEqual({});
    const passed = facts({
      day: day({ nowMinute: 14 * 60 + 30 }),
      standby: reading("DOWN"),
    });
    expect(passed.rides).toEqual({});
  });

  it("first sight of a down ride is the baseline, not news", () => {
    expect(changes(null, facts({ standby: reading("DOWN") }))).toEqual([]);
  });

  it("does not announce a ride opening in the morning as running again", () => {
    // Overnight heartbeats carry CLOSED into the opening minutes.
    const closed = facts({ standby: reading("CLOSED", { isHeartbeat: true }) });
    const base = nextBaseline(null, closed, day().entries, false);
    expect(changes(base, facts())).toEqual([]);
    // Seen open, then down: that one does come back up.
    const seen = nextBaseline(base, facts(), day().entries, false);
    const down = facts({ standby: reading("DOWN") });
    const told = nextBaseline(seen, down, day().entries, true);
    expect(changes(told, facts())).toContainEqual({
      type: "ride-up",
      name: "Taron",
    });
  });
});

describe("rule (d): forecast waits", () => {
  const withWait = (wait: number, patch: Partial<FactsInput> = {}) =>
    facts({
      planDay: {
        ...input().planDay!,
        rides: [
          {
            attractionSlug: "taron",
            attractionId: "a-taron",
            hours: [{ hour: 14, wait }],
            slots: [
              { minute: 13 * 60 + 45, wait: 5 },
              { minute: 14 * 60, wait },
            ],
          },
        ],
      },
      ...patch,
    });

  it("reads the slot the ride is planned in", () => {
    expect(withWait(30).waits).toEqual({ e1: 30 });
  });

  it("announces a move of at least 15 min and 30 %", () => {
    expect(changes(withWait(30), withWait(45))).toContainEqual({
      type: "wait",
      name: "Taron",
      at: "14:00",
      from: 30,
      to: 45,
    });
  });

  it("keeps quiet below either margin", () => {
    expect(changes(withWait(30), withWait(44))).toEqual([]); // 14 min
    expect(changes(withWait(60), withWait(75))).toEqual([]); // 25 %
  });

  it("reads no forecast more than a week out", () => {
    const far = day({ isToday: false, daysAhead: 8, date: "2026-10-25" });
    expect(withWait(30, { day: far }).waits).toEqual({});
  });
});

describe("dedupe and pacing", () => {
  it("sends a first change at once, then waits out the gap", () => {
    const urgent = [{ type: "closed" as const }];
    const wait = [
      { type: "wait" as const, name: "x", at: "14:00", from: 10, to: 40 },
    ];
    expect(isDigestDue(urgent, null, NOW)).toBe(true);
    expect(isDigestDue(urgent, NOW - URGENT_GAP_MS + 1, NOW)).toBe(false);
    expect(isDigestDue(urgent, NOW - URGENT_GAP_MS, NOW)).toBe(true);
    expect(isDigestDue(wait, NOW - URGENT_GAP_MS, NOW)).toBe(false);
    expect(isDigestDue(wait, NOW - WAIT_GAP_MS, NOW)).toBe(true);
    expect(isDigestDue([], null, NOW)).toBe(false);
  });

  it("does not announce the same change twice", () => {
    const base = facts();
    const cur = facts({ schedule: [{ ...OPEN[0], closingTime: at("17:00") }] });
    const after = nextBaseline(base, cur, day().entries, true);
    expect(changes(after, cur)).toEqual([]);
  });

  it("keeps a held-back change pending, and a blip that reverts never sends", () => {
    const base = facts();
    const down = facts({
      standby: new Map([
        ["a-taron", { status: "DOWN", isHeartbeat: false, timestampMs: NOW }],
      ]),
    });
    const held = nextBaseline(base, down, day().entries, false);
    expect(changes(held, down)).toContainEqual({
      type: "ride-down",
      name: "Taron",
    });
    expect(changes(held, base)).toEqual([]);
  });

  it("adds new entries to the baseline silently and drops removed ones", () => {
    const base = nextBaseline(null, facts(), day().entries, false);
    expect(base.waits).toEqual({ e1: 30 });
    expect(
      nextBaseline(base, { shows: {}, rides: {}, waits: {} }, [SHOW], false)
        .waits,
    ).toEqual({});
  });
});

describe("the digest", () => {
  const copy = {
    dedupeKey: "plan-change:phantasialand:2026-10-17",
    parkName: "Phantasialand",
    date: "2026-10-17",
    isToday: false,
    url: "/trip-planner?park=europe%2Fgermany%2Fbruehl%2Fphantasialand&date=2026-10-17",
    changes: [
      { type: "hours" as const, from: "09:00–18:00", to: "10:00–18:00" },
      { type: "show-moved" as const, name: "Feuerwerk", to: "15:30" },
      { type: "ride-down" as const, name: "Taron" },
      { type: "wait" as const, name: "Taron", at: "14:00", from: 30, to: 50 },
    ],
  };

  it("names the park and day, three lines and the rest counted, in the subscriber's language", () => {
    const de = writePlanChangeMessage(copy, "de-AT");
    expect(de.title).toBe("Dein Plan: Phantasialand, Sa., 17.10.");
    expect(de.body).toBe(
      "Öffnungszeiten jetzt 10:00–18:00 Uhr (vorher 09:00–18:00)\nFeuerwerk jetzt um 15:30 Uhr\nTaron ist gerade außer Betrieb\nund 1 weitere Änderung",
    );
    expect(writePlanChangeMessage({ ...copy, isToday: true }, "en").title).toBe(
      "Your plan: Phantasialand, today",
    );
    expect(writePlanChangeMessage(copy, "xx").title).toMatch(/^Your plan/);
  });

  it("writes every language", () => {
    for (const locale of ["de", "en", "nl", "fr", "es", "it"]) {
      const m = writePlanChangeMessage(copy, locale);
      expect(m.body.split("\n")).toHaveLength(4);
      expect(m.body).not.toContain("undefined");
    }
  });

  it("is the plan-change kind, which a device must have chosen", () => {
    const m = writePlanChangeMessage(copy, "de");
    expect(m.kind).toBe("plan-change");
    expect(pushKindOf({ tag: m.tag })).toBe("plan-change");
  });
});
