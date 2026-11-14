import {
  boundSeasonOf,
  isCurrentlyInSeason,
  nextSeasonBindingDay,
  resolveCuratedFacts,
  type ResolvedCuratedFacts,
} from "./curated-attraction-facts.util";
import type { SeasonBinding } from "../../common/types/season-binding.type";

describe("resolveCuratedFacts", () => {
  describe("name", () => {
    it("prefers the curated name", () => {
      expect(
        resolveCuratedFacts({ name: "Taron", curatedName: "TARON" }).name,
      ).toBe("TARON");
    });

    it("falls back to the synced name", () => {
      expect(resolveCuratedFacts({ name: "Taron" }).name).toBe("Taron");
    });

    it("ignores a curated name that is only whitespace", () => {
      // An editor clearing the field sends "" or "  "; that means "no
      // override", not "this ride has no name".
      expect(
        resolveCuratedFacts({ name: "Taron", curatedName: "   " }).name,
      ).toBe("Taron");
    });

    it("trims a curated name", () => {
      expect(
        resolveCuratedFacts({ name: "Taron", curatedName: " Taron " }).name,
      ).toBe("Taron");
    });
  });

  describe("minimum height", () => {
    it("prefers the correction", () => {
      expect(
        resolveCuratedFacts({ minimumHeight: 100, curatedMinimumHeight: 120 })
          .minimumHeight,
      ).toBe(120);
    });

    it("reads a curated 0 as 'no minimum at all'", () => {
      // Winni Splash: the wiki publishes 100, the park's own conditions say
      // children under 1.00 m may play when accompanied — no minimum.
      expect(
        resolveCuratedFacts({ minimumHeight: 100, curatedMinimumHeight: 0 })
          .minimumHeight,
      ).toBeNull();
    });

    it("keeps the synced value when nothing is curated", () => {
      expect(resolveCuratedFacts({ minimumHeight: 100 }).minimumHeight).toBe(
        100,
      );
    });

    it("never emits a unit next to a null height", () => {
      const facts = resolveCuratedFacts({
        minimumHeight: 100,
        curatedMinimumHeight: 0,
        minimumHeightUnit: "in",
      });
      expect(facts.minimumHeight).toBeNull();
      expect(facts.minimumHeightUnit).toBeNull();
    });

    it("defaults a curated height with no recorded unit to centimetres", () => {
      const facts = resolveCuratedFacts({ curatedMinimumHeight: 130 });
      expect(facts).toMatchObject({
        minimumHeight: 130,
        minimumHeightUnit: "cm",
      });
    });

    it("keeps the published unit when the height came from the sync", () => {
      expect(
        resolveCuratedFacts({ minimumHeight: 132, minimumHeightUnit: "in" })
          .minimumHeightUnit,
      ).toBe("in");
    });
  });

  describe("maximum height", () => {
    it("follows the same 0-means-none convention", () => {
      expect(
        resolveCuratedFacts({ maximumHeight: 140, curatedMaximumHeight: 0 })
          .maximumHeight,
      ).toBeNull();
      expect(
        resolveCuratedFacts({ maximumHeight: 140, curatedMaximumHeight: 120 })
          .maximumHeight,
      ).toBe(120);
    });
  });

  describe("may get wet", () => {
    it("prefers the correction, including a curated false", () => {
      // Genting SkyWorlds' shot tower is flagged as a water ride upstream and
      // is not one — so a curated `false` has to beat a synced `true`.
      expect(
        resolveCuratedFacts({ mayGetWet: true, curatedMayGetWet: false })
          .mayGetWet,
      ).toBe(false);
    });

    it("stays null when neither side knows", () => {
      expect(resolveCuratedFacts({}).mayGetWet).toBeNull();
    });
  });

  describe("seasonality", () => {
    it("passes the detector's verdict through untouched", () => {
      const facts = resolveCuratedFacts({
        isSeasonal: true,
        seasonMonths: [4, 5, 6, 7, 8, 9],
      });
      expect(facts).toMatchObject({
        isSeasonal: true,
        seasonMonths: [4, 5, 6, 7, 8, 9],
        seasonalityCurated: false,
      });
    });

    it("lets a curated false overrule the detector", () => {
      // A ride closed for a year-long refurbishment looks exactly like a
      // seasonal one to a behavioural detector.
      const facts = resolveCuratedFacts({
        isSeasonal: true,
        seasonMonths: [1, 2, 3],
        curatedIsSeasonal: false,
      });
      expect(facts.isSeasonal).toBe(false);
      expect(facts.seasonalityCurated).toBe(true);
    });

    it("takes the months down with a curated false", () => {
      // Serving seasonMonths on a ride the API just called not-seasonal is the
      // half-applied override this pairing exists to prevent.
      const facts = resolveCuratedFacts({
        isSeasonal: true,
        seasonMonths: [1, 2, 3],
        curatedIsSeasonal: false,
      });
      expect(facts.seasonMonths).toBeNull();
    });

    it("lets curated months stand in for months the detector could not derive", () => {
      // The detector writes no months below 330 observed days, on purpose.
      const facts = resolveCuratedFacts({
        isSeasonal: true,
        seasonMonths: null,
        curatedSeasonMonths: [7, 8],
      });
      expect(facts).toMatchObject({
        isSeasonal: true,
        seasonMonths: [7, 8],
        seasonalityCurated: true,
      });
    });

    it("infers seasonality from curated months alone", () => {
      const facts = resolveCuratedFacts({ curatedSeasonMonths: [10] });
      expect(facts.isSeasonal).toBe(true);
      expect(facts.seasonMonths).toEqual([10]);
    });

    it("lets a curated true stand without months", () => {
      const facts = resolveCuratedFacts({
        isSeasonal: false,
        curatedIsSeasonal: true,
      });
      expect(facts).toMatchObject({
        isSeasonal: true,
        seasonMonths: null,
        seasonalityCurated: true,
      });
    });

    it("prefers curated months over the detector's", () => {
      const facts = resolveCuratedFacts({
        isSeasonal: true,
        seasonMonths: [1, 2, 3, 4, 12],
        curatedSeasonMonths: [4, 5, 6],
      });
      expect(facts.seasonMonths).toEqual([4, 5, 6]);
    });

    it("treats an empty curated array as no override", () => {
      const facts = resolveCuratedFacts({
        isSeasonal: true,
        seasonMonths: [7],
        curatedSeasonMonths: [],
      });
      expect(facts.seasonMonths).toEqual([7]);
      expect(facts.seasonalityCurated).toBe(false);
    });
  });

  describe("isCurrentlyInSeason", () => {
    const july = new Date("2026-07-15T12:00:00Z");
    const facts = (
      partial: Partial<
        Pick<
          ResolvedCuratedFacts,
          "isSeasonal" | "seasonMonths" | "seasonOutSince"
        >
      >,
    ) => ({
      isSeasonal: false,
      seasonMonths: null,
      seasonOutSince: null,
      ...partial,
    });

    it("answers for a seasonal ride with known months", () => {
      expect(
        isCurrentlyInSeason(
          facts({ isSeasonal: true, seasonMonths: [7, 8] }),
          "UTC",
          july,
        ),
      ).toBe(true);
      expect(
        isCurrentlyInSeason(
          facts({ isSeasonal: true, seasonMonths: [1, 2] }),
          "UTC",
          july,
        ),
      ).toBe(false);
    });

    it("is null for a non-seasonal ride", () => {
      expect(isCurrentlyInSeason(facts({}), "UTC", july)).toBeNull();
    });

    it("is null — not false — when nothing at all is known about when", () => {
      // "Seasonal, but we do not know when" must not collapse into "closed",
      // which would hide a ride we have simply not understood yet.
      expect(
        isCurrentlyInSeason(facts({ isSeasonal: true }), "UTC", july),
      ).toBeNull();
      expect(
        isCurrentlyInSeason(
          facts({ isSeasonal: true, seasonMonths: [] }),
          "UTC",
          july,
        ),
      ).toBeNull();
    });

    it("says 'not now' when the detector recorded when it last ran", () => {
      // The months need 330 days of watching and the recording is 239 days
      // old, so a seasonal ride carries a flag and no months — and an ice rink
      // stayed on the August ride list because null means "do not hide".
      // `seasonOutSince` is the evidence that already exists: fully closed on
      // days the park was open, last seen running in January.
      expect(
        isCurrentlyInSeason(
          facts({ isSeasonal: true, seasonOutSince: "2026-01-25" }),
          "UTC",
          july,
        ),
      ).toBe(false);
    });

    it("reads the month in the park's timezone, not the server's", () => {
      // 2026-07-31 23:30 UTC: still July in New York (19:30), already August
      // in Tokyo (08:30 on the 1st).
      const monthEnd = new Date("2026-07-31T23:30:00Z");
      const august = facts({ isSeasonal: true, seasonMonths: [8] });
      expect(isCurrentlyInSeason(august, "America/New_York", monthEnd)).toBe(
        false,
      );
      expect(isCurrentlyInSeason(august, "Asia/Tokyo", monthEnd)).toBe(true);
      const july = facts({ isSeasonal: true, seasonMonths: [7] });
      expect(isCurrentlyInSeason(july, "America/New_York", monthEnd)).toBe(
        true,
      );
      expect(isCurrentlyInSeason(july, "Asia/Tokyo", monthEnd)).toBe(false);
    });

    it("still prefers the months when there are any", () => {
      expect(
        isCurrentlyInSeason(
          facts({
            isSeasonal: true,
            seasonMonths: [7],
            seasonOutSince: "2026-01-25",
          }),
          "UTC",
          july,
        ),
      ).toBe(true);
    });
  });

  describe("a ride bound to a park season", () => {
    const wintertraum: SeasonBinding = {
      id: "s1",
      kind: "winter",
      name: "Wintertraum",
      status: "expected",
      startDate: "2026-11-20",
      endDate: "2027-01-18",
      dates: null,
    };
    const tz = "Europe/Berlin";
    const on = (day: string) => new Date(`${day}T12:00:00Z`);

    it("is seasonal, in season only on the season's days", () => {
      const facts = resolveCuratedFacts({
        isSeasonal: false,
        seasonBindings: [wintertraum],
      });
      expect(facts.isSeasonal).toBe(true);
      expect(isCurrentlyInSeason(facts, tz, on("2026-11-14"))).toBe(false);
      expect(isCurrentlyInSeason(facts, tz, on("2026-12-24"))).toBe(true);
      expect(boundSeasonOf(facts, tz, on("2026-11-14"))).toEqual({
        boundSeason: wintertraum,
      });
    });

    it("outranks the detector's months and its out-of-season note", () => {
      const facts = resolveCuratedFacts({
        isSeasonal: true,
        seasonMonths: [11],
        seasonOutSince: "2026-03-01",
        seasonBindings: [wintertraum],
      });
      expect(facts.seasonMonths).toBeNull();
      expect(facts.seasonOutSince).toBeNull();
      expect(isCurrentlyInSeason(facts, tz, on("2026-11-14"))).toBe(false);
    });

    it("gives way to curated months and to a curated false", () => {
      const months = resolveCuratedFacts({
        curatedSeasonMonths: [11],
        seasonBindings: [wintertraum],
      });
      expect(months.seasonBindings).toEqual([]);
      expect(isCurrentlyInSeason(months, tz, on("2026-11-14"))).toBe(true);
      const notSeasonal = resolveCuratedFacts({
        curatedIsSeasonal: false,
        seasonBindings: [wintertraum],
      });
      expect(notSeasonal.isSeasonal).toBe(false);
      expect(boundSeasonOf(notSeasonal, tz)).toEqual({});
    });

    it("reads a season's list of days, and names the next one", () => {
      const nights = {
        ...wintertraum,
        kind: "halloween" as const,
        startDate: "2026-10-03",
        endDate: "2026-10-31",
        dates: ["2026-10-03", "2026-10-10"],
      };
      const facts = resolveCuratedFacts({ seasonBindings: [nights] });
      expect(isCurrentlyInSeason(facts, tz, on("2026-10-03"))).toBe(true);
      expect(isCurrentlyInSeason(facts, tz, on("2026-10-06"))).toBe(false);
      expect(nextSeasonBindingDay(nights, "2026-10-06")).toBe("2026-10-10");
      expect(nextSeasonBindingDay(nights, "2026-10-10")).toBeNull();
    });
  });
});
