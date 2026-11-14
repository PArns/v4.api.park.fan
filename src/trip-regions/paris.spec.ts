import {
  regionCalendar,
  regionPlan,
  regionRecords,
  type CellTweak,
} from "./__fixtures__/region-days";
import { PARIS, type RegionConfig } from "./regions";
import { seedTickets } from "./tickets";
import { buildLegs, findLeg, hopPairs } from "./travel";
import { dayCostParts, heatOf } from "./trip-optimizer";
import { normalizePlanQuery } from "./trip-plan.assembly";
import { describeRegion } from "./trip-region.assembly";
import { findTripDates } from "./trip-when";

/**
 * Paris as configured (PAR-1296): Disneyland Paris with Extra Magic Time for
 * its hotel guests, Parc Astérix a resort of its own with its hotels' Early
 * Entry, three bases with no route line, RER A from central Paris, no storm
 * season, a plan and a `/when` on its own days. The config itself is checked
 * whole in `region-config.spec.ts`.
 */

const REGION = PARIS;
const TODAY = "2027-01-04";
const DLP = "disneyland-park";
const DAW = "disney-adventure-world";
const ASTERIX = "parc-asterix";

/** Extra Magic Time before every Disney day, as the feed carries it since September 2026. */
const extraMagicTime: CellTweak = (slug, date, cell) => {
  if (slug === DLP || slug === DAW) {
    cell.early = {
      kind: "extra_magic_time",
      opensAt: `${date}T08:00:00+02:00`,
      closesAt: `${date}T09:00:00+02:00`,
    };
  }
};

describe("Paris", () => {
  it("serves its hotel kinds with their perks, its bases and no storm season", () => {
    const region = describeRegion(REGION, regionRecords(REGION));
    expect(region.hotels).toEqual([
      { key: "disney", perks: ["extra_magic_time"] },
      { key: "parc_asterix", perks: ["early_entry"] },
      { key: "offsite", perks: [] },
    ]);
    expect(region.groups.map((g) => [g.key, g.kind])).toEqual([
      ["disneyland-paris", "resort"],
      ["parc-asterix", "resort"],
    ]);
    expect(region.weather).toEqual({ climatePark: DLP });
    expect(region.ticketMarkets).toEqual({ home: "eu", codes: ["eu", "intl"] });
    expect(region.bases?.map((b) => b.key)).toEqual([
      "disneyland-paris",
      "parc-asterix",
      "paris",
    ]);
    expect(region.bases?.every((b) => b.order === undefined)).toBe(true);
    expect(region.parks.filter((p) => p.headliner).map((p) => p.slug)).toEqual([
      DLP,
      DAW,
      ASTERIX,
    ]);
  });

  it("names every base a gateway serves, so a client pins a start only where there is one", () => {
    const region = describeRegion(REGION, regionRecords(REGION));
    const gateways = new Map(
      (region.bases ?? []).flatMap((b) =>
        (b.gateways ?? []).map((g) => [g.code, g.bases] as const),
      ),
    );
    expect(Object.fromEntries(gateways)).toEqual({
      XED: ["disneyland-paris"],
      CDG: ["disneyland-paris", "parc-asterix", "paris"],
      ORY: ["paris"],
    });
  });

  it("walks between the Disney gates, takes RER A from Paris, and hops only at Disney", () => {
    const legs = buildLegs(REGION);
    expect(findLeg(legs, DLP, DAW)).toMatchObject({
      mode: "walk",
      minutes: 5,
      source: "curated",
    });
    expect(findLeg(legs, "hotel:paris_center", DLP)).toMatchObject({
      mode: "rail",
      name: "RER A",
      minutes: 50,
      source: "curated",
    });
    expect(findLeg(legs, DLP, ASTERIX)).toMatchObject({
      mode: "drive",
      via: "walk",
      transferMinutes: 15,
    });
    expect(hopPairs(REGION, legs, [DLP, DAW, ASTERIX])).toEqual([[DLP, DAW]]);
  });

  it("prices heat against Paris' cooler thresholds and never a storm", () => {
    expect(
      heatOf({ tempMax: 31, basis: "forecast" }, REGION.weather.heat)?.factor,
    ).toBeCloseTo(1.08);
    expect(
      heatOf(
        { tempMax: 31, basis: "forecast" },
        { ...REGION.weather.heat, forecastFromC: 35 },
      ),
    ).toBeNull();
    expect(
      dayCostParts(
        {
          status: "OPERATING",
          headlinerWait: 40,
          exactWait: 40,
          basis: "forecast",
          dayHours: 12,
        },
        { type: "theme_park", date: "2027-09-10", weather: REGION.weather },
      ),
    ).toEqual({ cost: 40, heat: 0, storm: 0, hurricane: 0 });
  });

  it("plans the three headliners across the stays it picks, every park on a day its base reaches", () => {
    const answer = regionPlan(
      REGION,
      { from: "2027-05-10", to: "2027-05-15", split: "fixed" },
      TODAY,
      extraMagicTime,
    );
    expect(answer.unplaced).toEqual([]);
    const placed = answer.days.flatMap((d) => d.parks.map((p) => p.slug));
    expect([...placed].sort()).toEqual([ASTERIX, DAW, DLP].sort());
    const stays = answer.meta.stays ?? [];
    expect(stays.reduce((n, s) => n + (s.nights as number), 0)).toBe(5);
    const reach = new Map(
      (REGION.bases ?? []).map((b) => [
        b.key,
        new Set(b.parks.map((p) => p.slug)),
      ]),
    );
    for (const day of answer.days) {
      expect(day.base).toBeDefined();
      for (const p of day.parks) {
        expect(reach.get(day.base as string)?.has(p.slug)).toBe(true);
      }
    }
  });

  it("gives a Disney hotel stay Extra Magic Time and Parc Astérix's hotels none without the feed's slot", () => {
    const answer = regionPlan(
      REGION,
      {
        from: "2027-05-10",
        to: "2027-05-15",
        parks: `${DLP},${DAW},${ASTERIX}`,
        bases: "disneyland-paris:3@disney_lake,parc-asterix:2@parc_asterix",
        split: "fixed",
      },
      TODAY,
      extraMagicTime,
    );
    expect(answer.unplaced).toEqual([]);
    const disney = answer.days.filter((d) =>
      d.parks.some((p) => p.slug === DLP || p.slug === DAW),
    );
    expect(disney.length).toBeGreaterThan(0);
    for (const day of disney) {
      expect(day.base).toBe("disneyland-paris");
      expect(day.perks).toEqual(["extra_magic_time"]);
    }
    const asterix = answer.days.find((d) =>
      d.parks.some((p) => p.slug === ASTERIX),
    );
    expect(asterix?.perks ?? []).toEqual([]);
  });

  it("counts a stay's hotel perks only where the visitor names its area, never by the base's default area", () => {
    const query = {
      from: "2027-05-10",
      to: "2027-05-13",
      parks: `${DLP},${DAW}`,
      split: "fixed",
    };
    const unnamed = regionPlan(
      REGION,
      { ...query, bases: "disneyland-paris:3" },
      TODAY,
      extraMagicTime,
    );
    const named = regionPlan(
      REGION,
      { ...query, bases: "disneyland-paris:3@disney_lake" },
      TODAY,
      extraMagicTime,
    );
    const disneyDays = (answer: typeof named) =>
      answer.days.filter((d) => d.kind === "park");
    expect(disneyDays(unnamed).length).toBe(2);
    // The base's default area is a Disney hotel's, but the guest may sleep
    // in Val d'Europe: no perk is claimed for a hotel nobody named.
    for (const day of disneyDays(unnamed)) expect(day.perks ?? []).toEqual([]);
    for (const day of disneyDays(named)) {
      expect(day.perks).toEqual(["extra_magic_time"]);
    }
  });

  it("gives a tie between bases to the one closer to the trip's parks, never the one listed first", () => {
    // Central Paris within the free day-trip time of Disneyland Park, and
    // listed first: both bases then cost a Disney trip the same.
    const paris = REGION.bases?.find((b) => b.key === "paris");
    const tied: RegionConfig = {
      ...REGION,
      bases: [
        {
          ...paris!,
          parks: paris!.parks.map((p) =>
            p.slug === DLP || p.slug === DAW ? { ...p, minutes: 45 } : p,
          ),
        },
        ...(REGION.bases ?? []).filter((b) => b.key !== "paris"),
      ],
    };
    const query = {
      from: "2027-05-10",
      to: "2027-05-13",
      parks: `${DLP},${DAW}`,
      split: "fixed",
    };
    const answer = regionPlan(tied, query, TODAY);
    expect(answer.meta.stays?.map((s) => s.base)).toEqual(["disneyland-paris"]);
    // A trip to Parc Astérix alone ties its own hotels with central Paris.
    const asterix = regionPlan(tied, { ...query, parks: ASTERIX }, TODAY);
    expect(asterix.meta.stays?.map((s) => s.base)).toEqual(["parc-asterix"]);
    // The real region: a Disney trip never lands in central Paris.
    expect(
      regionPlan(REGION, query, TODAY).meta.stays?.map((s) => s.base),
    ).toEqual(["disneyland-paris"]);
  });

  it("takes a stay's area with its base, never hotel= or area=", () => {
    expect(() =>
      normalizePlanQuery(
        REGION,
        REGION.parks.map((p) => p.slug),
        { from: "2027-05-10", to: "2027-05-15", hotel: "disney" },
        TODAY,
      ),
    ).toThrow("bases=<base>@<area>");
    expect(() =>
      normalizePlanQuery(
        REGION,
        REGION.parks.map((p) => p.slug),
        {
          from: "2027-05-10",
          to: "2027-05-15",
          bases: "disneyland-paris:5@parc_asterix",
        },
        TODAY,
      ),
    ).toThrow("parc_asterix is not a hotel area of Disneyland Paris");
  });

  it("suggests a Disney ticket and a Parc Astérix ticket, each of its own resort", () => {
    const answer = regionPlan(
      REGION,
      {
        from: "2027-05-10",
        to: "2027-05-14",
        parks: `${DLP},${DAW},${ASTERIX}`,
        split: "fixed",
      },
      TODAY,
    );
    const suggestions = answer.meta.ticketSuggestion ?? [];
    expect(
      Object.fromEntries(suggestions.map((s) => [s.resort, s.product])),
    ).toEqual({
      "disneyland-paris": "dlp-multi-day",
      "parc-asterix": "asterix-1-day",
    });
    for (const product of seedTickets(REGION)) {
      expect(["disneyland-paris", "parc-asterix"]).toContain(product.resort);
    }
  });

  it("finds the windows of a /when with their stays and no storm warning", () => {
    const from = "2027-06-07";
    const calendar = regionCalendar(REGION, from, 40, extraMagicTime);
    const result = findTripDates({
      region: REGION,
      calendar,
      query: {
        from,
        to: calendar.meta.to,
        days: 4,
        limit: 3,
        gap: 3,
        plan: normalizePlanQuery(
          REGION,
          REGION.parks.map((p) => p.slug),
          { from, to: "2027-06-10", parks: `${DLP},${DAW}` },
          TODAY,
        ),
      },
    });
    expect(result.windows.length).toBeGreaterThan(0);
    for (const w of result.windows) {
      expect(w.warnings.filter((x) => /hurricane|typhoon/.test(x))).toEqual([]);
      expect(
        (w.plan as { stays?: unknown[] } | undefined)?.stays?.length,
      ).toBeGreaterThan(0);
    }
    for (const m of result.months) {
      expect(m).not.toHaveProperty("usSchoolBreakDays");
    }
  });
});
