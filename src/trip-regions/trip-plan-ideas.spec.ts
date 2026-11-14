import { regionCalendar, regionPlan } from "./__fixtures__/region-days";
import { PARIS, ORLANDO } from "./regions";
import {
  MAX_IDEA_PLANS,
  MAX_IDEAS,
  MIN_IDEA_MINUTES,
  planIdeas,
} from "./trip-plan-ideas";
import type { TripPlanQueryDto } from "./dto/trip-region-query.dto";
import type { TripParkDayInternal } from "./trip-calendar.util";

const TODAY = "2027-01-04";
const DLP = "disneyland-park";
const DAW = "disney-adventure-world";
const ASTERIX = "parc-asterix";

/** Last year's Saturday in May: 55 and 65 minutes at Disney, 40 at Astérix. */
const WAITS: Record<string, number> = { [DLP]: 55, [DAW]: 65, [ASTERIX]: 40 };
const mayWaits = (slug: string, _date: string, cell: TripParkDayInternal) => {
  cell.headlinerWait = WAITS[slug];
  cell.waitExact = WAITS[slug];
};

function ideasFor(query: Record<string, string>) {
  const region = PARIS;
  const known = region.parks.map((p) => p.slug);
  const days =
    (Date.parse(`${query.to}T00:00:00Z`) -
      Date.parse(`${query.from}T00:00:00Z`)) /
      86_400_000 +
    1;
  return planIdeas({
    region,
    known,
    query: query as unknown as TripPlanQueryDto,
    today: TODAY,
    calendar: regionCalendar(region, query.from, days, mayWaits),
    computedAt: "t",
  });
}

describe("the plan's ideas", () => {
  it("brings back the resort a weekend leaves out with a park on a travel day, as a query /plan answers", () => {
    const query = { from: "2027-05-14", to: "2027-05-16" };
    const answer = ideasFor(query);
    expect(answer.meta.plans).toBeLessThanOrEqual(MAX_IDEA_PLANS);
    expect(answer.ideas.length).toBeGreaterThan(0);
    expect(answer.ideas.length).toBeLessThanOrEqual(MAX_IDEAS);
    const [first] = answer.ideas;
    expect(first.kind).toBe("travel_day");
    expect(first.places).toEqual([DLP]);
    expect(first.headlinerWait.after).toBe(55);
    // Applied as is, the query gives the plan the idea priced.
    const applied = regionPlan(
      PARIS,
      { ...query, ...Object.fromEntries(new URLSearchParams(first.query)) },
      TODAY,
      mayWaits,
    );
    expect(applied.totals.headlinerWait).toBe(first.totals.after);
    expect(applied.days.some((d) => d.parks.some((p) => p.slug === DLP))).toBe(
      true,
    );
    // Every idea places a park or saves the minimum, best first.
    for (const idea of answer.ideas) {
      expect(
        (idea.places?.length ?? 0) > 0 || idea.savedMinutes >= MIN_IDEA_MINUTES,
      ).toBe(true);
    }
    const rank = answer.ideas.map((i) => [
      i.places?.length ?? 0,
      i.savedMinutes,
    ]);
    expect(rank).toEqual([...rank].sort((a, b) => b[0] - a[0] || b[1] - a[1]));
  });

  it("keeps the visitor's pins and answers, and has nothing to add to a plan that is already best", () => {
    const answer = ideasFor({
      from: "2027-05-14",
      to: "2027-05-16",
      arrival: "park",
      departure: "park",
      lock: `${ASTERIX}@2027-05-16`,
    });
    for (const idea of answer.ideas) {
      const set = new URLSearchParams(idea.query);
      if (set.has("lock")) {
        expect(set.get("lock")?.split(",")).toContain(`${ASTERIX}@2027-05-16`);
      }
      expect(set.has("arrival") || set.has("departure")).toBe(false);
    }
    // The same waits every day: no move or swap saves anything.
    expect(
      answer.ideas.filter((i) => i.kind === "move" || i.kind === "swap"),
    ).toEqual([]);
  });

  it("offers Orlando's hopper where a Disney park is left out", () => {
    const from = "2027-03-01";
    const answer = planIdeas({
      region: ORLANDO,
      known: ORLANDO.parks.map((p) => p.slug),
      query: {
        from,
        to: "2027-03-03",
        parks: "magic-kingdom-park,epcot,disneys-hollywood-studios",
        arrival: "park",
      } as unknown as TripPlanQueryDto,
      today: TODAY,
      calendar: regionCalendar(ORLANDO, from, 3),
      computedAt: "t",
    });
    const hopper = answer.ideas.find((i) => i.kind === "hopper");
    expect(hopper?.query).toBe("hopper=walt-disney-world&hop=1");
    expect(hopper?.places?.length).toBe(1);
  });
});
