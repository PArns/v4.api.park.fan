import {
  buildStatsQuery,
  parseStatsResults,
  type SparqlResults,
} from "./wikidata.parser";

/** A SPARQL binding, shaped the way the query service returns one. */
const row = (fields: Record<string, string | undefined>) =>
  Object.fromEntries(
    Object.entries(fields)
      .filter(([, value]) => value !== undefined)
      .map(([key, value]) => [key, { value: value as string }]),
  );

describe("buildStatsQuery", () => {
  it("reads every measurement through the normalised value", () => {
    // `psn:` makes Wikidata convert to SI before we see it, so a ride entered
    // in mph and one entered in km/h arrive the same. That is the whole reason
    // this file has no unit table to get wrong.
    const query = buildStatsQuery([3117]);

    expect(query).toContain("psn:P2052");
    expect(query).toContain("psn:P2048");
    expect(query).toContain("psn:P2043");
    expect(query).toContain("psn:P2047");
    expect(query).not.toContain("wdt:P2052");
  });

  it("puts every id in the VALUES block", () => {
    expect(buildStatsQuery([3117, 17412])).toContain(
      'VALUES ?rcdb { "3117" "17412" }',
    );
  });

  it("drops anything that is not a plain id", () => {
    // These come from our own database, but they are being pasted into a query
    // string — so digits only, no exceptions.
    const query = buildStatsQuery(["3117", '" } INJECTED {"', "12x", 42]);

    expect(query).toContain('"3117"');
    expect(query).toContain('"42"');
    expect(query).not.toContain("INJECTED");
    expect(query).not.toContain("12x");
  });
});

describe("parseStatsResults", () => {
  it("converts SI speed to km/h without a rounding tail", () => {
    // Black Mamba is published as 80 km/h; Wikidata normalises that to
    // 22.222222222222222224 m/s, and ×3.6 must come home as 80 rather than
    // 80.00000000000001.
    const results: SparqlResults = {
      results: {
        bindings: [
          row({
            rcdb: "3117",
            ride: "http://www.wikidata.org/entity/Q319081",
            speedSi: "22.222222222222222224",
            heightSi: "26",
            lengthSi: "768",
          }),
        ],
      },
    };

    const stats = parseStatsResults(results).get("3117")!;

    expect(stats.topSpeedKmh).toBe(80);
    expect(stats.heightM).toBe(26);
    expect(stats.lengthM).toBe(768);
    expect(stats.durationSeconds).toBeNull();
    expect(stats.entityId).toBe("Q319081");
  });

  it("skips a ride Wikidata has an entry for but no numbers on", () => {
    // Storing a row of nulls would mark the ride imported and stop a later run
    // from picking it up once somebody fills the values in.
    const results: SparqlResults = {
      results: {
        bindings: [
          row({ rcdb: "527", ride: "http://www.wikidata.org/entity/Q389146" }),
        ],
      },
    };

    expect(parseStatsResults(results).size).toBe(0);
  });

  it("keeps the first statement when a ride has several", () => {
    // Merging them would invent a ride that is one entry's height and another's
    // speed — a set of numbers no real coaster ever had.
    const results: SparqlResults = {
      results: {
        bindings: [
          row({
            rcdb: "1",
            ride: "http://www.wikidata.org/entity/Q1",
            speedSi: "20",
            heightSi: "30",
          }),
          row({
            rcdb: "1",
            ride: "http://www.wikidata.org/entity/Q1",
            speedSi: "25",
            heightSi: "40",
          }),
        ],
      },
    };

    const stats = parseStatsResults(results).get("1")!;

    expect(stats.topSpeedKmh).toBe(72); // 20 m/s, not 25
    expect(stats.heightM).toBe(30);
  });

  it("returns nothing rather than throwing on an empty or odd result", () => {
    expect(parseStatsResults({}).size).toBe(0);
    expect(parseStatsResults({ results: {} }).size).toBe(0);
    expect(
      parseStatsResults({ results: { bindings: [row({ rcdb: "9" })] } }).size,
    ).toBe(0);
  });
});
