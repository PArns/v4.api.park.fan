import { regionPlan, type CellTweak } from "./__fixtures__/region-days";
import { JAPAN, PARIS } from "./regions";

/**
 * Stay plans that tie: two with the same number of moves whose cost differs
 * by less than ORDER_MATTERS_MINUTES per park day go to the bases closer to
 * the trip's parks (docs/frontend/trip-regions.md#bases-a-trip-across-several-cities).
 * Japan's trade-off between a move and a long day trip is pinned as it was
 * before the rule; regions without bases are pinned by their own specs
 * (`orlando-identity.spec.ts`, `southern-california.spec.ts`).
 */

const TODAY = "2027-01-04";

/** A plan as one line per day: date, kind, parks, the night's base. */
function itinerary(answer: ReturnType<typeof regionPlan>): string[] {
  return answer.days.map(
    (d) =>
      `${d.date.slice(5)} ${d.kind}${d.parks.length > 0 ? ` ${d.parks.map((p) => p.slug).join("+")}` : ""} @${String(d.base)}`,
  );
}

const stays = (answer: ReturnType<typeof regionPlan>) =>
  answer.meta.stays?.map((s) => `${String(s.base)}:${String(s.nights)}`);

describe("stay plans that tie", () => {
  it("keeps a Disneyland Park and Parc Astérix trip at the Disney hotels when central Paris is cheaper by a hair", () => {
    // From central Paris Disneyland Park is 5 minutes past the free day
    // trip, from the Disney hotels Parc Astérix is: the plans differ only in
    // which park's day pays for those minutes, under a minute in all.
    const waits: CellTweak = (slug, _date, cell) => {
      const wait =
        slug === "disneyland-park"
          ? 30
          : slug === "parc-asterix"
            ? 45
            : cell.waitExact;
      cell.waitExact = wait;
      cell.headlinerWait = wait;
    };
    const answer = regionPlan(
      PARIS,
      {
        from: "2027-05-07",
        to: "2027-05-10",
        parks: "disneyland-park,parc-asterix",
        split: "fixed",
      },
      TODAY,
      waits,
    );
    expect(stays(answer)).toEqual(["disneyland-paris:3"]);
    expect(itinerary(answer)).toEqual([
      "05-07 arrival @disneyland-paris",
      "05-08 park disneyland-park @disneyland-paris",
      "05-09 park parc-asterix @disneyland-paris",
      "05-10 departure @disneyland-paris",
    ]);
    expect(answer.meta.exact).toBe(true);
    // The visitor who names central Paris still gets it.
    const named = regionPlan(
      PARIS,
      {
        from: "2027-05-07",
        to: "2027-05-10",
        parks: "disneyland-park,parc-asterix",
        bases: "paris:3",
        split: "fixed",
      },
      TODAY,
      waits,
    );
    expect(stays(named)).toEqual(["paris:3"]);
  });

  describe("Japan, as before the rule", () => {
    const plan = (q: Record<string, string>) =>
      regionPlan(JAPAN, { split: "fixed", ...q }, TODAY);

    it("keeps Fuji-Q a day trip from Tokyo rather than move for a night by the lake", () => {
      const once = plan({
        from: "2027-04-10",
        to: "2027-04-13",
        parks: "fuji-q-highland",
        startBase: "tokyo",
        endBase: "tokyo",
      });
      expect(stays(once)).toEqual(["tokyo:3"]);
      expect(itinerary(once)).toEqual([
        "04-10 arrival @tokyo",
        "04-11 park fuji-q-highland @tokyo",
        "04-12 rest @tokyo",
        "04-13 departure @tokyo",
      ]);
      const twice = plan({
        from: "2027-04-10",
        to: "2027-04-16",
        parks: "tokyo-disneyland,tokyo-disneysea,fuji-q-highland:2",
        startBase: "tokyo",
        endBase: "tokyo",
      });
      expect(stays(twice)).toEqual(["tokyo:6"]);
      expect(itinerary(twice)).toEqual([
        "04-10 arrival @tokyo",
        "04-11 park fuji-q-highland @tokyo",
        "04-12 park tokyo-disneysea @tokyo",
        "04-13 rest @tokyo",
        "04-14 park fuji-q-highland @tokyo",
        "04-15 park tokyo-disneyland @tokyo",
        "04-16 departure @tokyo",
      ]);
    });

    it("plans the five parks from Tokyo to Osaka with the same stays and days", () => {
      const answer = plan({
        from: "2027-04-10",
        to: "2027-04-19",
        parks:
          "tokyo-disneyland,tokyo-disneysea,fuji-q-highland,legoland-japan,universal-studios-japan",
        startBase: "tokyo",
        endBase: "osaka",
      });
      expect(stays(answer)).toEqual(["tokyo:4", "osaka:5"]);
      expect(itinerary(answer)).toEqual([
        "04-10 arrival @tokyo",
        "04-11 park fuji-q-highland @tokyo",
        "04-12 park tokyo-disneysea @tokyo",
        "04-13 park tokyo-disneyland @tokyo",
        "04-14 travel @osaka",
        "04-15 park legoland-japan @osaka",
        "04-16 rest @osaka",
        "04-17 park universal-studios-japan @osaka",
        "04-18 rest @osaka",
        "04-19 departure @osaka",
      ]);
      expect(answer.meta.exact).toBe(true);
    });

    it("keeps LEGOLAND Japan a day trip from Osaka", () => {
      const answer = plan({
        from: "2027-04-10",
        to: "2027-04-14",
        parks: "universal-studios-japan,legoland-japan",
        startBase: "osaka",
        endBase: "osaka",
      });
      expect(stays(answer)).toEqual(["osaka:4"]);
      expect(itinerary(answer)).toEqual([
        "04-10 arrival @osaka",
        "04-11 rest @osaka",
        "04-12 park universal-studios-japan @osaka",
        "04-13 park legoland-japan @osaka",
        "04-14 departure @osaka",
      ]);
    });
  });
});
