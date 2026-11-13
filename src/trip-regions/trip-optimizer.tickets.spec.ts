import { addIsoDays } from "../common/utils/date.util";
import { ORLANDO } from "./regions";
import {
  optimizeTrip,
  type OptimizedPlan,
  type PlanCell,
  type PlanInput,
  type PlanOptions,
  type PlanPark,
  type PlanTicket,
} from "./trip-optimizer";

const START = "2027-02-01"; // a Monday
const MK = "magic-kingdom-park";
const EPCOT = "epcot";
const HS = "disneys-hollywood-studios";
const AK = "disneys-animal-kingdom-theme-park";
const TL = "disneys-typhoon-lagoon-water-park";
const USF = "universal-studios-florida";
const IOA = "universal-islands-of-adventure";
const EPIC = "universal-epic-universe";
const SW = "seaworld-orlando";
const DC = "discovery-cove-orlando";

const OPTIONS: PlanOptions = {
  arrival: "park",
  departure: "park",
  blocks: false,
  blockGroups: ["walt-disney-world", "universal-orlando", "seaworld-orlando"],
  fireworks: "any",
  rest: { mode: "none" },
  hop: false,
  hopPairs: [],
  perkRules: [],
  locks: [],
};

const config = new Map(ORLANDO.parks.map((p) => [p.slug, p]));
const park = (slug: string, visits = 1): PlanPark => ({
  slug,
  group: config.get(slug)?.group ?? "test",
  type: config.get(slug)?.type ?? "theme_park",
  visits,
});

/** `waits[slug][i]`: a figure, `null` for CLOSED; 50 where absent. */
function input(
  n: number,
  parks: PlanPark[],
  waits: Record<string, Array<number | null>>,
  tickets: PlanTicket[],
  options: Partial<PlanOptions> = {},
): PlanInput {
  const dates = Array.from({ length: n }, (_, i) => addIsoDays(START, i));
  const cells: PlanInput["cells"] = {};
  for (const p of parks) {
    cells[p.slug] = {};
    dates.forEach((d, i) => {
      const v = waits[p.slug]?.[i];
      const cell: PlanCell =
        v === null
          ? { status: "CLOSED", basis: "none" }
          : {
              status: "OPERATING",
              headlinerWait: v ?? 50,
              basis: "forecast",
              dayHours: 12,
            };
      cells[p.slug][d] = cell;
    });
  }
  return {
    days: dates.map((date) => ({
      date,
      weekday: new Date(`${date}T12:00:00Z`).getUTCDay(),
      isHoliday: false,
      isSchoolVacation: false,
    })),
    parks,
    cells,
    options: { ...OPTIONS, ...options },
    tickets,
  };
}

const ticket = (t: Partial<PlanTicket> & { id: string }): PlanTicket => ({
  parks: [],
  days: Infinity,
  window: Infinity,
  caps: [],
  hopPairs: [],
  ...t,
});

const day = (i: number) => addIsoDays(START, i);
const datesOf = (plan: OptimizedPlan, slug: string): string[] =>
  plan.parks.find((p) => p.slug === slug)?.dates ?? [];
const indexOf = (date: string) =>
  Math.round(
    (Date.parse(`${date}T00:00:00Z`) - Date.parse(`${START}T00:00:00Z`)) /
      86_400_000,
  );

/** Cheap (20) on the given days, dear (90) elsewhere. */
const cheapOn = (n: number, cheap: number[]): number[] =>
  Array.from({ length: n }, (_, i) => (cheap.includes(i) ? 20 : 90));

describe("the optimizer with owned tickets", () => {
  it("keeps a fixed start's days inside its window", () => {
    const parks = [MK, EPCOT, HS, AK].map((s) => park(s));
    const waits = Object.fromEntries(
      parks.map((p) => [p.slug, cheapOn(12, [0, 1, 9, 10, 11])]),
    );
    const free = optimizeTrip(input(12, parks, waits, []));
    const freeDays = parks.flatMap((p) => datesOf(free, p.slug)).map(indexOf);
    expect(freeDays.some((i) => i < 2 || i > 8)).toBe(true);

    const fourDay = ticket({
      id: "t1",
      parks: [MK, EPCOT, HS, AK],
      days: 4,
      window: 7,
      start: day(2),
    });
    const plan = optimizeTrip(input(12, parks, waits, [fourDay]));
    const used = parks.flatMap((p) => datesOf(plan, p.slug)).map(indexOf);
    expect(used).toHaveLength(4);
    for (const i of used) {
      expect(i).toBeGreaterThanOrEqual(2);
      expect(i).toBeLessThanOrEqual(8);
    }
    expect(plan.relaxed).not.toContain("ticket_window_relaxed");
    expect(plan.tickets).toEqual([
      {
        id: "t1",
        start: day(2),
        end: day(8),
        usedDays: 4,
        allowanceVisits: 0,
      },
    ]);
    const ticketDays = plan.days
      .filter((d) => d.ticket === "t1")
      .map((d) => d.ticketDay);
    expect(ticketDays).toEqual([1, 2, 3, 4]);
  });

  it("picks the best start for a ticket without one and says it chose", () => {
    const parks = [MK, EPCOT, HS, AK].map((s) => park(s));
    // The cheap days 6..9 fit one 7-day window; 0 and 13 lie apart from them.
    const waits = Object.fromEntries(
      parks.map((p) => [p.slug, cheapOn(14, [0, 6, 7, 8, 9, 13])]),
    );
    const fourDay = ticket({
      id: "t1",
      parks: [MK, EPCOT, HS, AK],
      days: 4,
      window: 7,
    });
    const plan = optimizeTrip(input(14, parks, waits, [fourDay]));
    const used = parks
      .flatMap((p) => datesOf(plan, p.slug))
      .map(indexOf)
      .sort((a, b) => a - b);
    expect(used).toEqual([6, 7, 8, 9]);
    expect(plan.tickets[0]).toMatchObject({
      start: day(6),
      startChosen: true,
      end: day(12),
      usedDays: 4,
    });
    expect(plan.exact).toBe(true);
  });

  it("lets two wide windows interleave their resorts where blocks would not", () => {
    const disney = [MK, EPCOT, HS, AK];
    const universal = [USF, IOA, EPIC];
    const parks = [...disney, ...universal].map((s) => park(s));
    // Disney is cheap on even days, Universal on odd ones.
    const waits: Record<string, number[]> = {};
    for (const s of disney) {
      waits[s] = Array.from({ length: 14 }, (_, i) => (i % 2 === 0 ? 20 : 90));
    }
    for (const s of universal) {
      waits[s] = Array.from({ length: 14 }, (_, i) => (i % 2 === 1 ? 20 : 90));
    }
    const blocked = optimizeTrip(input(14, parks, waits, [], { blocks: true }));
    const owned = optimizeTrip(
      input(
        14,
        parks,
        waits,
        [
          ticket({
            id: "t1",
            parks: disney,
            days: 14,
            window: 18,
            start: day(0),
          }),
          ticket({
            id: "t2",
            parks: universal,
            days: 14,
            window: 14,
            start: day(0),
          }),
        ],
        { blocks: true },
      ),
    );
    const spread = (plan: OptimizedPlan, slugs: string[]) =>
      slugs.flatMap((s) => datesOf(plan, s)).map(indexOf);
    const disneyDays = spread(owned, disney);
    const universalDays = spread(owned, universal);
    expect(disneyDays.every((i) => i % 2 === 0)).toBe(true);
    expect(universalDays.every((i) => i % 2 === 1)).toBe(true);
    // With blocks and no tickets no Universal day falls between Disney's.
    const blockDisney = spread(blocked, disney);
    expect(
      spread(blocked, universal).every(
        (i) => i < Math.min(...blockDisney) || i > Math.max(...blockDisney),
      ),
    ).toBe(true);
    expect(owned.days.filter((d) => d.ticket === "t1")).toHaveLength(4);
    expect(owned.days.filter((d) => d.ticket === "t2")).toHaveLength(3);
  });

  it("keeps a cap: Epic Universe on one day of a 1-Day + 1-Day Epic", () => {
    const parks = [park(USF), park(EPIC, 2)];
    const oneAndEpic = ticket({
      id: "t1",
      parks: [USF, IOA, EPIC],
      days: 2,
      window: 4,
      start: day(0),
      caps: [
        { parks: [EPIC], max: 1 },
        { parks: [USF, IOA], max: 1 },
      ],
    });
    const plan = optimizeTrip(input(6, parks, {}, [oneAndEpic]));
    expect(datesOf(plan, EPIC)).toHaveLength(1);
    expect(datesOf(plan, USF)).toHaveLength(1);
    expect(plan.unplaced).toEqual([{ slug: EPIC, reason: "no_ticket_day" }]);
    expect(plan.tickets[0].usedDays).toBe(2);
  });

  it("makes a hop day only on a ticket that hops between the two parks", () => {
    const parks = [MK, EPCOT, HS, AK].map((s) => park(s, s === MK ? 2 : 1));
    const base = ticket({
      id: "t1",
      parks: [MK, EPCOT, HS, AK],
      days: 4,
      window: 7,
      start: day(0),
    });
    const noHopper = optimizeTrip(input(8, parks, {}, [base], { hop: true }));
    expect(noHopper.unplaced).toEqual([{ slug: MK, reason: "no_ticket_day" }]);
    expect(noHopper.days.some((d) => d.kind === "hop")).toBe(false);

    const hopper = {
      ...base,
      hopPairs: [[MK, EPCOT]] as Array<[string, string]>,
    };
    const withHopper = optimizeTrip(
      input(8, parks, {}, [hopper], { hop: true }),
    );
    expect(withHopper.unplaced).toEqual([]);
    const hops = withHopper.days.filter((d) => d.kind === "hop");
    expect(hops).toHaveLength(1);
    expect(hops[0].parks.map((v) => v.slug).sort()).toEqual([EPCOT, MK]);
    expect(withHopper.tickets[0].usedDays).toBe(4);

    // The hopper's pairs, but hop=0: the visitor did not ask for hop days.
    const asked = optimizeTrip(input(8, parks, {}, [hopper]));
    expect(asked.unplaced).toEqual([{ slug: MK, reason: "no_ticket_day" }]);
  });

  it("puts a visit outside the window only when nothing inside fits, and says so", () => {
    const parks = [park(MK), park(EPCOT)];
    // EPCOT is closed on every day of the window.
    const waits = {
      [EPCOT]: [null, null, null, null, 30, 30, 30, 30],
    };
    const plan = optimizeTrip(
      input(8, parks, waits, [
        ticket({
          id: "t1",
          parks: [MK, EPCOT],
          days: 2,
          window: 4,
          start: day(0),
        }),
      ]),
    );
    expect(plan.relaxed).toContain("ticket_window_relaxed");
    const epcotDay = plan.days.find((d) => d.parks[0]?.slug === EPCOT);
    expect(epcotDay?.warnings).toContain("outside_ticket_window");
    expect(indexOf(epcotDay?.date as string)).toBeGreaterThanOrEqual(4);
    const mkDay = plan.days.find((d) => d.parks[0]?.slug === MK);
    expect(mkDay?.warnings).not.toContain("outside_ticket_window");
    expect(indexOf(mkDay?.date as string)).toBeLessThanOrEqual(3);
  });

  it("counts a water park on the allowance, not as a ticket day", () => {
    const parks = [park(MK), park(EPCOT), park(TL)];
    const plan = optimizeTrip(
      input(5, parks, {}, [
        ticket({
          id: "t1",
          parks: [MK, EPCOT, HS, AK],
          allowance: { parks: [TL], visits: 2 },
          days: 2,
          window: 5,
          start: day(0),
        }),
      ]),
    );
    expect(plan.unplaced).toEqual([]);
    const water = plan.days.find((d) => d.parks[0]?.slug === TL);
    expect(water).toMatchObject({ ticket: "t1", ticketAllowance: true });
    expect(water?.ticketDay).toBeUndefined();
    expect(plan.tickets[0]).toMatchObject({ usedDays: 2, allowanceVisits: 1 });
  });

  it("plans a park no owned ticket covers as before, without a ticket", () => {
    const parks = [park(MK), park(USF)];
    const plan = optimizeTrip(
      input(4, parks, {}, [
        ticket({ id: "t1", parks: [MK], days: 1, window: 1, start: day(1) }),
      ]),
    );
    expect(datesOf(plan, MK)).toEqual([day(1)]);
    const usf = plan.days.find((d) => d.parks[0]?.slug === USF);
    expect(usf?.ticket).toBeUndefined();
  });

  it("puts Discovery Cove on its reserved day and the sister park in one span with it", () => {
    const parks = [park(DC), park(SW)];
    const waits = { [SW]: cheapOn(20, [1, 19]) };
    const plan = optimizeTrip(
      input(20, parks, waits, [
        ticket({
          id: "t1",
          parks: [DC, SW],
          days: Infinity,
          window: 14,
          reservedDayParks: [DC],
          reservedDay: day(5),
          caps: [{ parks: [DC], max: 1 }],
        }),
      ]),
    );
    expect(datesOf(plan, DC)).toEqual([day(5)]);
    // Day 19 is cheaper but 15 days from the reserved day; day 1 lies in a span with it.
    expect(datesOf(plan, SW)).toEqual([day(1)]);
  });
});

describe("an owned ticket with no start on sale in the trip", () => {
  it("puts its days outside its window and says so", () => {
    const parks = [park(USF)];
    const plan = optimizeTrip(
      input(4, parks, {}, [
        ticket({
          id: "t1",
          parks: [USF, IOA, EPIC],
          days: 14,
          window: 14,
          startOptions: [],
        }),
      ]),
    );
    expect(plan.relaxed).toContain("ticket_window_relaxed");
    const usf = plan.days.find((d) => d.parks[0]?.slug === USF);
    expect(usf?.warnings).toContain("outside_ticket_window");
  });
});
