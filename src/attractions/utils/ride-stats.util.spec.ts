import { mergeRideStats, resolveRideStatsAttribution } from "./ride-stats.util";
import type {
  RideMeasurements,
  RideStats,
} from "../entities/attraction-ride-profile.entity";

const curated = (over: Partial<RideMeasurements> = {}): RideMeasurements => ({
  topSpeedKmh: null,
  heightM: null,
  lengthM: null,
  durationSeconds: null,
  ...over,
});

const imported = (over: Partial<RideStats> = {}): RideStats => ({
  topSpeedKmh: null,
  heightM: null,
  lengthM: null,
  durationSeconds: null,
  source: "wikidata",
  sourceId: "Q319081",
  ...over,
});

describe("mergeRideStats", () => {
  it("returns null when neither writer has anything", () => {
    expect(mergeRideStats(null, null)).toBeNull();
  });

  it("serves the imported record untouched when nothing is curated", () => {
    const stats = mergeRideStats(
      null,
      imported({ topSpeedKmh: 80, heightM: 26 }),
    );

    expect(stats).toMatchObject({
      topSpeedKmh: 80,
      heightM: 26,
      source: "wikidata",
      sourceId: "Q319081",
    });
  });

  it("drops the Wikidata citation when no imported value survives", () => {
    const stats = mergeRideStats(curated({ topSpeedKmh: 70 }), null);

    expect(stats).toMatchObject({
      topSpeedKmh: 70,
      source: "curated",
      sourceId: null,
    });
  });

  it("lets a curated value win over the imported one", () => {
    // Wikidata has Lynet's length entered as its height. The curated value is
    // the reason the ride does not read as 540 metres tall.
    const stats = mergeRideStats(
      curated({ heightM: 20 }),
      imported({ heightM: 540, lengthM: 20 }),
    );

    expect(stats?.heightM).toBe(20);
  });

  it("keeps imported fields the curation is silent about", () => {
    const stats = mergeRideStats(
      curated({ topSpeedKmh: 70 }),
      imported({ durationSeconds: 112 }),
    );

    expect(stats).toMatchObject({
      topSpeedKmh: 70,
      durationSeconds: 112,
      source: "mixed",
      sourceId: "Q319081",
    });
  });

  it("treats a curated zero as a value, not as absence", () => {
    // Nothing measures zero here today, but `?? ` on a number field is how a
    // legitimate 0 silently becomes the other writer's value.
    const stats = mergeRideStats(
      curated({ durationSeconds: 0 }),
      imported({ durationSeconds: 90 }),
    );

    expect(stats?.durationSeconds).toBe(0);
  });

  it("reports curated-only when the import states nothing at all", () => {
    const stats = mergeRideStats(
      curated({ topSpeedKmh: 95, heightM: 40 }),
      imported(),
    );

    expect(stats).toMatchObject({ source: "curated", sourceId: null });
  });
});

describe("resolveRideStatsAttribution", () => {
  it("credits Wikidata and links the entity the numbers are stated on", () => {
    const stats = mergeRideStats(null, imported({ topSpeedKmh: 80 }));

    expect(resolveRideStatsAttribution(stats)).toEqual({
      label: "Wikidata",
      url: "https://www.wikidata.org/wiki/Q319081",
    });
  });

  it("credits nobody when every surviving number is hand-curated", () => {
    // The case that shipped a broken citation: `source` alone said "curated"
    // while the client still rendered a link, and there was no id to put in it.
    const stats = mergeRideStats(curated({ topSpeedKmh: 70 }), null);

    expect(resolveRideStatsAttribution(stats)).toBeNull();
  });

  it("still credits Wikidata on a mixed record — an imported value survived", () => {
    const stats = mergeRideStats(
      curated({ topSpeedKmh: 70 }),
      imported({ durationSeconds: 112 }),
    );

    expect(stats?.source).toBe("mixed");
    expect(resolveRideStatsAttribution(stats)).toMatchObject({
      label: "Wikidata",
    });
  });

  it("credits nobody when the import was outvoted on every field", () => {
    // `source` is "curated" here and so is every served number, even though the
    // ride does have an entity. Crediting it would name a source none of the
    // displayed values came from.
    const stats = mergeRideStats(
      curated({ topSpeedKmh: 70, heightM: 20 }),
      imported({ topSpeedKmh: 65, heightM: 18 }),
    );

    expect(stats?.sourceId).toBeNull();
    expect(resolveRideStatsAttribution(stats)).toBeNull();
  });

  it("has nothing to credit on a ride with no measurements", () => {
    expect(resolveRideStatsAttribution(null)).toBeNull();
  });
});
