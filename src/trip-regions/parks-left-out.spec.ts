import { regionCalendar, regionPlan } from "./__fixtures__/region-days";
import { ORLANDO, PARIS } from "./regions";
import { normalizePlanQuery, pinnedVisits } from "./trip-plan.assembly";
import { findTripDates, holidaysEndedBefore } from "./trip-when";

/**
 * A trip with more chosen parks than park days (PAR-1309): the plan fills
 * what fits and names the parks it left out (`parksLeftOut`), on `/plan` and
 * on each `/when` window alike; only pinned visits that cannot fit are a 400.
 * Contract: docs/frontend/trip-regions.md#the-plan-get-v1trip-regionsorlandoplan.
 */

const TODAY = "2027-01-04";
const HEADLINERS = ORLANDO.parks.filter((p) => p.headliner).map((p) => p.slug);

describe("parks left out", () => {
  it("plans Orlando's 7 default parks in a 7-day trip on its 5 park days and names the other 2", () => {
    const answer = regionPlan(
      ORLANDO,
      { from: "2027-01-23", to: "2027-01-29" },
      TODAY,
    );
    const planned = answer.days.flatMap((d) => d.parks.map((p) => p.slug));
    expect(answer.days.filter((d) => d.kind === "park")).toHaveLength(5);
    expect(new Set(planned).size).toBe(5);
    const leftOut = (
      answer as unknown as {
        parksLeftOut: Array<{ park: string; reason: string }>;
      }
    ).parksLeftOut;
    expect(leftOut).toHaveLength(2);
    for (const p of leftOut) {
      expect(p.reason).toBe("no_day_left");
      expect(planned).not.toContain(p.park);
      expect(HEADLINERS).toContain(p.park);
    }
    // In the region's order, so a sentence reads the way the hub lists them.
    const order = leftOut.map((p) => HEADLINERS.indexOf(p.park));
    expect([...order].sort((a, b) => a - b)).toEqual(order);
    expect([...planned, ...leftOut.map((p) => p.park)].sort()).toEqual(
      [...HEADLINERS].sort(),
    );
  });

  it("plans more parks without a count than the trip has days, where it used to refuse", () => {
    const q = {
      from: "2027-01-23",
      to: "2027-01-25",
      parks: HEADLINERS.join(","),
    };
    expect(() =>
      normalizePlanQuery(ORLANDO, HEADLINERS, q, TODAY),
    ).not.toThrow();
    const answer = regionPlan(ORLANDO, q, TODAY);
    const leftOut = (answer as unknown as { parksLeftOut: unknown[] })
      .parksLeftOut;
    // Three days, a free arrival and departure: one park day, six parks out.
    expect(answer.days.filter((d) => d.kind === "park")).toHaveLength(1);
    expect(leftOut).toHaveLength(6);
    // A count of one is no pin either.
    expect(() =>
      normalizePlanQuery(
        ORLANDO,
        HEADLINERS,
        { ...q, parks: HEADLINERS.map((s) => `${s}:1`).join(",") },
        TODAY,
      ),
    ).not.toThrow();
  });

  it("refuses only pinned visits the trip cannot hold", () => {
    expect(
      pinnedVisits([
        { visits: 2, explicit: true },
        { visits: 1, explicit: true },
        { visits: 2, explicit: false },
      ]),
    ).toBe(2);
    expect(() =>
      normalizePlanQuery(
        ORLANDO,
        HEADLINERS,
        {
          from: "2027-01-23",
          to: "2027-01-25",
          parks: "magic-kingdom-park:2,epcot:2",
        },
        TODAY,
      ),
    ).toThrow("More park visits than trip days");
    // Pinned visits within the trip's days plan, a repeat visit dropped first.
    const answer = regionPlan(
      ORLANDO,
      {
        from: "2027-01-23",
        to: "2027-01-26",
        parks: "magic-kingdom-park:2,epcot:2",
        split: "fixed",
      },
      TODAY,
    );
    const planned = answer.days.flatMap((d) => d.parks.map((p) => p.slug));
    expect(new Set(planned)).toEqual(new Set(["magic-kingdom-park", "epcot"]));
    expect(
      (answer as unknown as { parksLeftOut: unknown[] }).parksLeftOut,
    ).toEqual([]);
    expect(answer.unplaced).toHaveLength(2);
  });

  it("gives each /when window the same field, from the same plan", () => {
    const from = "2027-01-09";
    const calendar = regionCalendar(ORLANDO, from, 40);
    const result = findTripDates({
      region: ORLANDO,
      calendar,
      query: {
        from,
        to: calendar.meta.to,
        days: 7,
        limit: 3,
        gap: 3,
        plan: normalizePlanQuery(
          ORLANDO,
          HEADLINERS,
          { from, to: "2027-01-15" },
          TODAY,
        ),
      },
    });
    expect(result.windows.length).toBeGreaterThan(0);
    for (const w of result.windows) {
      const leftOut = w.plan.parksLeftOut ?? [];
      expect(leftOut).toHaveLength(2);
      const planned = w.plan.days.flatMap((d) => d.parks);
      expect([...planned, ...leftOut.map((p) => p.park)].sort()).toEqual(
        [...HEADLINERS].sort(),
      );
      // The plan for the window's dates leaves out the same parks.
      const plan = regionPlan(
        ORLANDO,
        { from: w.from, to: w.to },
        TODAY,
        undefined,
        calendar,
      );
      expect(
        (plan as unknown as { parksLeftOut: unknown[] }).parksLeftOut,
      ).toEqual(leftOut);
    }
  });

  it("names a park out of a region with bases by its own reason", () => {
    const answer = regionPlan(
      PARIS,
      {
        from: "2027-05-10",
        to: "2027-05-13",
        parks: "disneyland-park,disney-adventure-world,parc-asterix",
        split: "fixed",
      },
      TODAY,
    );
    const leftOut = (
      answer as unknown as {
        parksLeftOut: Array<{ park: string; reason: string }>;
      }
    ).parksLeftOut;
    expect(leftOut).toHaveLength(1);
    expect(leftOut[0].reason).toBe("no_day_left");
  });
});

describe("after_holidays", () => {
  /** Orlando's January 2027: the winter break to the 4th, Martin Luther King Jr. Day's weekend 16–18. */
  const january = Array.from({ length: 31 }, (_, i) => {
    const day = i + 1;
    return { isSchoolVacation: day <= 4 || (day >= 16 && day <= 18) };
  });
  const at = (day: number) => day - 1;

  it("follows a break of five days or more, never a long weekend", () => {
    // 23 January: the long weekend ended five days before, and is no holidays.
    expect(holidaysEndedBefore(january, at(23))).toBe(false);
    expect(holidaysEndedBefore(january, at(19))).toBe(false);
  });

  it("counts a break the range starts inside: a search from 1 January", () => {
    expect(holidaysEndedBefore(january, at(5))).toBe(true);
    expect(holidaysEndedBefore(january, at(11))).toBe(true);
    expect(holidaysEndedBefore(january, at(12))).toBe(false);
  });

  it("counts a week-long break that ended within seven days", () => {
    const spring = Array.from({ length: 20 }, (_, i) => ({
      isSchoolVacation: i >= 2 && i <= 10,
    }));
    // The break's last day is index 10: seven days after it, then none.
    expect(holidaysEndedBefore(spring, 11)).toBe(true);
    expect(holidaysEndedBefore(spring, 17)).toBe(true);
    expect(holidaysEndedBefore(spring, 18)).toBe(false);
  });
});
