import {
  regionCalendar,
  regionPlan,
  regionRecords,
} from "./__fixtures__/region-days";
import { PARIS, regionOfPark, SOUTHERN_CALIFORNIA } from "./regions";
import { buildLegs, findLeg, hopPairs } from "./travel";
import { dayCostParts, heatOf } from "./trip-optimizer";
import { normalizePlanQuery } from "./trip-plan.assembly";
import { describeRegion } from "./trip-region.assembly";
import { findTripDates } from "./trip-when";

/**
 * Southern California as configured (PAR-1303): no hotel perk anywhere,
 * Universal Studios Hollywood as a day trip, the walk between the two
 * Disneyland gates, Magic Mountain's heat and no storm season, a plan and a
 * `/when` on its own days. The config itself is checked whole in
 * `region-config.spec.ts`.
 */

const REGION = SOUTHERN_CALIFORNIA;
const TODAY = "2027-01-04";
const DL = "disneyland-park";
const DCA = "disney-california-adventure-park";
const USH = "universal-studios-hollywood";

describe("Southern California", () => {
  it("serves hotel kinds without perks, US markets and weather without a storm season", () => {
    const region = describeRegion(REGION, regionRecords(REGION));
    expect(region.hotels).toEqual([
      { key: "disney", perks: [] },
      { key: "offsite", perks: [] },
    ]);
    expect(region.ticketMarkets).toEqual({
      home: "us",
      codes: ["us", "uk", "intl"],
    });
    expect(region.weather).toEqual({ climatePark: DL });
    expect(region.travel.source).toBe("osrm");
    expect(region).not.toHaveProperty("bases");
    expect(region.groups.find((g) => g.key === USH)?.kind).toBe("day_trip");
  });

  it("tells Anaheim's Disneyland Park from Paris' by the path", () => {
    expect(
      regionOfPark({
        continentSlug: "north-america",
        countrySlug: "united-states",
        citySlug: "anaheim",
        slug: DL,
      }),
    ).toBe(REGION);
    expect(
      regionOfPark({
        continentSlug: "europe",
        countrySlug: "france",
        citySlug: "paris",
        slug: DL,
      }),
    ).toBe(PARIS);
  });

  it("walks between the two Disneyland gates, which one hopper ticket hops between", () => {
    const legs = buildLegs(REGION);
    expect(findLeg(legs, DL, DCA)).toMatchObject({
      mode: "walk",
      minutes: 5,
      source: "curated",
    });
    expect(findLeg(legs, DL, USH)).toMatchObject({ mode: "drive" });
    expect(hopPairs(REGION, legs, [DL, DCA, USH])).toEqual([[DL, DCA]]);
    // The drive to either park ends at Mickey & Friends; the tram follows.
    expect(findLeg(legs, DL, "knotts-berry-farm")).toMatchObject({
      via: "tram",
      transferMinutes: 15,
    });
  });

  it("plans the headliners with no perk on any day, even where the feed has an early slot", () => {
    const answer = regionPlan(
      REGION,
      {
        from: "2027-02-01",
        to: "2027-02-06",
        hotel: "disney",
        area: "disneyland_resort",
        split: "fixed",
      },
      TODAY,
      (slug, date, cell) => {
        if (slug === DL || slug === USH) {
          cell.early = {
            kind: slug === DL ? "early_entry" : "early_access_ticket",
            opensAt: `${date}T08:00:00-08:00`,
            closesAt: `${date}T09:00:00-08:00`,
          };
        }
      },
    );
    const parkDays = answer.days.filter((d) => d.kind === "park");
    expect(parkDays.flatMap((d) => d.parks.map((p) => p.slug)).sort()).toEqual(
      [DCA, DL, USH].sort(),
    );
    for (const day of parkDays) {
      expect(day.perks ?? []).toEqual([]);
      expect(day.warnings).not.toContain("hurricane_season");
    }
    const suggested = answer.meta.ticketSuggestion?.map((s) => s.product);
    expect(suggested).toEqual(
      expect.arrayContaining(["dlr-multi-day", "ush-1-day"]),
    );
  });

  it("refuses another region's hotel kind", () => {
    expect(() =>
      normalizePlanQuery(
        REGION,
        REGION.parks.map((p) => p.slug),
        { from: "2027-02-01", to: "2027-02-05", hotel: "disney_deluxe" },
        TODAY,
      ),
    ).toThrow("hotel must be one of disney, offsite");
  });

  it("prices Magic Mountain's summer heat, no storm, and Disneyland's mild normal not at all", () => {
    const august = (tempMax: number) => ({
      tempMax,
      basis: "climate_normal" as const,
    });
    expect(heatOf(august(34.6), REGION.weather.heat)?.factor).toBeGreaterThan(
      1,
    );
    expect(heatOf(august(30.3), REGION.weather.heat)).toBeNull();
    const cell = {
      status: "OPERATING" as const,
      headlinerWait: 40,
      exactWait: 40,
      basis: "forecast" as const,
      dayHours: 12,
    };
    expect(
      dayCostParts(cell, {
        type: "theme_park",
        date: "2027-09-10",
        weather: REGION.weather,
      }),
    ).toEqual({ cost: 40, heat: 0, storm: 0, hurricane: 0 });
  });

  it("finds the windows of a /when without a hurricane season, keeping the US school-break days", () => {
    const from = "2027-08-02";
    const calendar = regionCalendar(REGION, from, 75);
    const days = 4;
    const result = findTripDates({
      region: REGION,
      calendar,
      query: {
        from,
        to: calendar.meta.to,
        days,
        limit: 5,
        gap: 3,
        plan: normalizePlanQuery(
          REGION,
          REGION.parks.map((p) => p.slug),
          {
            from,
            to: "2027-08-05",
            parks: `${DL},${DCA}`,
            hotel: "disney",
          },
          TODAY,
        ),
      },
    });
    expect(result.windows.length).toBeGreaterThan(0);
    for (const w of result.windows) {
      expect(w.warnings).not.toContain("hurricane_season");
      expect(w.warnings).not.toContain("hurricane_peak");
      if (w.penalties) expect(w.penalties).not.toHaveProperty("hurricane");
    }
    for (const m of result.months) {
      expect(m).not.toHaveProperty("hurricaneSeason");
      expect(m).toHaveProperty("usSchoolBreakDays");
    }
  });
});
