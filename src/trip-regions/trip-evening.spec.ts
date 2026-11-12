import { fromZonedTime } from "date-fns-tz";
import { addIsoDays } from "../common/utils/date.util";
import { ORLANDO } from "./regions";
import { buildLegs, hopPairs } from "./travel";
import { isoInZone, type TripParkDayInternal } from "./trip-calendar.util";
import type { PlannedDay } from "./trip-optimizer";
import type { InternalRegionCalendar } from "./trip-region.assembly";
import {
  eveningShowsWanted,
  eveningTriggers,
  planEvenings,
  showsKey,
  type EveningShows,
  type TicketKind,
} from "./trip-evening";
import { normalizePlanQuery, planInput } from "./trip-plan.assembly";

const TZ = ORLANDO.timezone;
const FROM = "2026-12-01";
const TO = "2026-12-05";
const DAY = "2026-12-03";
const LEGS = buildLegs(ORLANDO);

const MK = "magic-kingdom-park";
const EPCOT = "epcot";
const HS = "disneys-hollywood-studios";
const AK = "disneys-animal-kingdom-theme-park";
const USF = "universal-studios-florida";
const IOA = "universal-islands-of-adventure";
const EPIC = "universal-epic-universe";

const at = (date: string, hhmm: string): string =>
  isoInZone(fromZonedTime(`${date}T${hhmm}:00`, TZ), TZ);

function open(
  opens: string,
  closes: string,
  over: Partial<TripParkDayInternal> = {},
): TripParkDayInternal {
  return {
    status: "OPERATING",
    hoursPublished: true,
    opensAt: at(DAY, opens),
    closesAt: at(DAY, closes),
    crowdLevel: "moderate",
    basis: "forecast",
    ...over,
  };
}

const PARTY: Partial<TripParkDayInternal> = {
  evening: {
    kind: "ticketed_event",
    opensAt: at(DAY, "19:00"),
    closesAt: at(DAY, "23:59"),
  },
};

/** A normal December day at Walt Disney World, with one park changed. */
function disneyDay(
  over: Record<string, TripParkDayInternal> = {},
): Record<string, TripParkDayInternal> {
  return {
    [MK]: open("09:00", "22:00"),
    [EPCOT]: open("09:00", "21:00"),
    [HS]: open("09:00", "21:00"),
    [AK]: open("08:00", "19:00"),
    ...over,
  };
}

function calendar(
  parks: Record<string, TripParkDayInternal>,
): InternalRegionCalendar {
  return {
    meta: {
      region: "orlando",
      timezone: TZ,
      from: FROM,
      to: TO,
      computedAt: "t",
      publishedHoursUntil: {},
    },
    parks: Object.keys(parks),
    days: [{ date: DAY, isHoliday: false, isSchoolVacation: false, parks }],
    seasons: [],
  };
}

const parkDay = (slug: string, date = DAY): PlannedDay => ({
  date,
  kind: "park",
  parks: [{ slug, part: "full" }],
  reasons: [],
  warnings: [],
});

/** The night shows production projects for a December Thursday. */
function shows(
  over: Record<string, Array<[string, string[]]>> = {},
): EveningShows {
  const lists: Record<string, Array<[string, string[]]>> = {
    [MK]: [["happily-ever-after", ["21:00"]]],
    [EPCOT]: [["luminous-the-symphony-of-us", ["21:00"]]],
    [HS]: [
      ["fantasmic", ["18:30", "20:00"]],
      ["wonderful-world-of-animation", ["20:45"]],
    ],
    ...over,
  };
  return new Map(
    Object.entries(lists).map(([park, list]) => [
      showsKey(park, DAY),
      list.map(([showSlug, times]) => ({
        showSlug,
        times,
        source: "projected" as const,
      })),
    ]),
  );
}

function evening(
  park: string,
  parks: Record<string, TripParkDayInternal>,
  ticket: TicketKind,
  showMap: EveningShows = shows(),
) {
  return planEvenings({
    region: ORLANDO,
    calendar: calendar(parks),
    legs: LEGS,
    days: [parkDay(park)],
    trip: { from: FROM, to: TO },
    ticket,
    shows: showMap,
  }).get(DAY);
}

describe("evening triggers", () => {
  it("takes a park closing before 20:00, or two hours before the resort's latest", () => {
    const trip = { from: FROM, to: TO };
    const cal = calendar(disneyDay({ [AK]: open("08:00", "20:00") }));
    expect(eveningTriggers(ORLANDO, cal, [parkDay(AK)], trip)).toEqual([
      { date: DAY, park: AK },
    ]);
    expect(eveningTriggers(ORLANDO, cal, [parkDay(EPCOT)], trip)).toEqual([]);
  });

  it("never fires on rest, arrival or departure days, nor the trip's ends", () => {
    const cal = calendar(disneyDay({ [AK]: open("08:00", "18:00") }));
    const trip = { from: FROM, to: TO };
    const rest: PlannedDay = { ...parkDay(AK), kind: "rest", parks: [] };
    expect(eveningTriggers(ORLANDO, cal, [rest], trip)).toEqual([]);
    expect(
      eveningTriggers(ORLANDO, cal, [parkDay(AK)], { from: DAY, to: TO }),
    ).toEqual([]);
    expect(
      eveningTriggers(ORLANDO, cal, [parkDay(AK)], { from: FROM, to: DAY }),
    ).toEqual([]);
  });

  it("asks for the night shows of the day's resort only", () => {
    const cal = calendar(disneyDay({ [AK]: open("08:00", "18:00") }));
    const wanted = eveningShowsWanted(ORLANDO, cal, [{ date: DAY, park: AK }]);
    expect(wanted.map((w) => w.park).sort()).toEqual([EPCOT, MK, HS].sort());
  });
});

describe("evening after Animal Kingdom closing at 18:00", () => {
  const parks = disneyDay({ [AK]: open("08:00", "18:00") });

  it("with a hopper: EPCOT or Hollywood Studios with a night show after arrival", () => {
    const e = evening(AK, parks, "hopper");
    expect(e?.kind).toBe("hop");
    expect([EPCOT, HS]).toContain(e?.park);
    expect(e?.reasons).toEqual(
      expect.arrayContaining(["park_closes_early", "open_later", "night_show"]),
    );
    const show = e?.highlights?.find((h) => h.kind !== "dining");
    expect(show?.source).toBe("projected");
    expect((show?.at ?? "") >= (e?.from ?? "")).toBe(true);
    expect(e?.travel?.minutes).toBeLessThanOrEqual(35);
    expect(e?.alternatives?.length).toBeLessThanOrEqual(2);
    expect(e?.alternatives?.some((a) => a.kind === "venue")).toBe(true);
  });

  it("without one: Disney Springs, free and open later", () => {
    const e = evening(AK, parks, "base");
    expect(e).toMatchObject({ kind: "venue", venue: "disney-springs" });
    expect(e?.reasons).toEqual(
      expect.arrayContaining(["park_closes_early", "free_entry", "open_later"]),
    );
    expect(e?.from).toBe(
      at(DAY, `18:${String(e?.travel?.minutes).padStart(2, "0")}`),
    );
    expect(e?.to).toBe(at(DAY, "23:00"));
  });

  it("names a show only where the shows data has a time for it", () => {
    const e = evening(AK, parks, "hopper", new Map());
    expect(e?.kind).toBe("hop");
    expect(e?.highlights).toBeUndefined();
    expect(e?.reasons).not.toContain("night_show");
  });
});

describe("fireworks views", () => {
  it("BoardWalk when EPCOT's fireworks start soon after a short day elsewhere", () => {
    const parks = disneyDay({ [HS]: open("09:00", "19:00") });
    const e = evening(HS, parks, "base");
    expect(e).toMatchObject({ kind: "venue", venue: "disney-boardwalk" });
    expect(e?.highlights?.[0]).toMatchObject({
      kind: "fireworks_view",
      name: "Luminous The Symphony of Us",
      at: at(DAY, "21:00"),
    });
    expect(e?.reasons).toContain("fireworks_view");
  });

  it("never the Polynesian on a party night, whatever the projection says", () => {
    const parks = disneyDay({
      [MK]: open("09:00", "18:00", PARTY),
      [AK]: open("08:00", "18:00"),
    });
    const showMap = shows({ [MK]: [["happily-ever-after", ["19:30"]]] });
    const e = evening(AK, parks, "base", showMap);
    const all = [e, ...(e?.alternatives ?? [])];
    expect(all.some((o) => o?.venue === "polynesian-beach")).toBe(false);

    const normal = evening(
      AK,
      disneyDay({ [AK]: open("08:00", "18:00") }),
      "base",
      showMap,
    );
    expect(normal).toMatchObject({ venue: "polynesian-beach" });
  });
});

describe("a Magic Kingdom party night", () => {
  const parks = disneyDay({ [MK]: open("09:00", "18:00", PARTY) });

  it("counts as a day ending early and gets an evening", () => {
    const hop = evening(MK, parks, "hopper");
    expect(hop?.kind).toBe("hop");
    expect(hop?.park).not.toBe(MK);
    expect(hop?.reasons[0]).toBe("park_closes_early");
    const base = evening(MK, parks, "base");
    expect(base).toMatchObject({ kind: "venue", venue: "disney-springs" });
  });

  it("is never a hop park for day guests", () => {
    const e = evening(
      AK,
      disneyDay({
        [MK]: open("09:00", "18:00", PARTY),
        [AK]: open("08:00", "17:00"),
      }),
      "hopper",
    );
    const all = [e, ...(e?.alternatives ?? [])];
    expect(all.some((o) => o?.park === MK)).toBe(false);
  });
});

describe("Universal", () => {
  const parks: Record<string, TripParkDayInternal> = {
    [USF]: open("09:00", "19:00"),
    [IOA]: open("09:00", "21:00", { crowdLevel: "high" }),
    [EPIC]: open("09:00", "21:00", { crowdLevel: "very_low" }),
  };

  it("hops between Universal Studios and Islands of Adventure, never to Epic", () => {
    const e = evening(USF, parks, "hopper");
    expect(e).toMatchObject({ kind: "hop", park: IOA });
    const all = [e, ...(e?.alternatives ?? [])];
    expect(all.some((o) => o?.park === EPIC)).toBe(false);
    expect(all.some((o) => o?.venue === "universal-citywalk")).toBe(true);
  });

  it("sends an Epic day guest to CityWalk, hopper or not", () => {
    const epic = { ...parks, [EPIC]: open("09:00", "18:00") };
    expect(evening(EPIC, epic, "hopper")).toMatchObject({
      kind: "venue",
      venue: "universal-citywalk",
    });
  });

  it("pairs Epic with nobody on a hop day", () => {
    expect(hopPairs(ORLANDO, LEGS, [USF, IOA, EPIC])).toEqual([[USF, IOA]]);
  });
});

describe("the hopper ticket in the plan query", () => {
  const query = {
    from: FROM,
    to: addIsoDays(FROM, 1),
    parks: `${MK},${EPCOT},${HS}`,
    hop: "1",
  };

  it("ignores hop=1 without ticket=hopper, with a warning and no pairing", () => {
    expect(() =>
      normalizePlanQuery(ORLANDO, [MK, EPCOT, HS], query, "2026-11-12"),
    ).toThrow("More park visits than trip days");
    const q = normalizePlanQuery(
      ORLANDO,
      [MK, EPCOT, HS],
      { ...query, parks: `${MK},${EPCOT}` },
      "2026-11-12",
    );
    expect(q).toMatchObject({
      ticket: "base",
      hop: false,
      warnings: ["hop_needs_hopper"],
    });
    expect(planInput(ORLANDO, q, calendar({}), new Map()).options.hop).toBe(
      false,
    );
  });

  it("takes hop=1 with ticket=hopper", () => {
    const q = normalizePlanQuery(
      ORLANDO,
      [MK, EPCOT, HS],
      { ...query, ticket: "hopper" },
      "2026-11-12",
    );
    expect(q).toMatchObject({ ticket: "hopper", hop: true, warnings: [] });
    expect(q.evenings).toBe(true);
  });
});
