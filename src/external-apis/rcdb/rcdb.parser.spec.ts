import { readFileSync } from "fs";
import { join } from "path";
import { parseRideStats } from "./rcdb.parser";

/**
 * Fixtures are the stat tables lifted off real RCDB pages — the only part of
 * the page this parser looks at. Four rides that between them cover what the
 * tables do: a European ride (metric numbers shown in imperial), an American
 * one (genuinely imperial), a record holder whose speed has to survive the
 * round trip, and a manufacturer's model page that is not a ride at all.
 */
const fixture = (name: string) =>
  readFileSync(join(__dirname, "__fixtures__", `${name}.html`), "utf8");

describe("parseRideStats", () => {
  it("recovers the metric figures a European ride was built to", () => {
    // Black Mamba is published as 768 m long, 27 m drop, 80 km/h. RCDB shows
    // 2519.7 ft / 88.6 ft / 49.7 mph — converting back has to land on the
    // round numbers, not 767.9 m and 79.98 km/h.
    const stats = parseRideStats(fixture("black-mamba"))!;

    expect(stats.lengthM).toBe(768);
    expect(stats.dropM).toBe(27);
    expect(stats.topSpeedKmh).toBe(80);
    expect(stats.inversions).toBe(4);
    expect(stats.gForce).toBe(4);
  });

  it("keeps the converted value for a ride actually built in mph", () => {
    // 47 mph is 75.6 km/h. Snapping that to 76 would invent a figure the ride
    // was never built to — the tolerance exists for RCDB's rounding, not to
    // make numbers tidy.
    const stats = parseRideStats(fixture("flashback"))!;

    expect(stats.topSpeedKmh).toBe(75.6);
    expect(stats.durationSeconds).toBe(108); // 1:48
    expect(stats.capacityPerHour).toBe(760);
    expect(stats.trainManufacturer).toBe("Vekoma");
    expect(stats.designer).toBe("Peter Clerx");
  });

  it("survives the round trip on a record holder", () => {
    // Falcon's Flight is a 250 km/h ride that RCDB shows as 155.3 mph, which
    // converts to 249.93 — the widest gap the speed tolerance has to close.
    const stats = parseRideStats(fixture("falcons-flight"))!;

    expect(stats.topSpeedKmh).toBe(250);
    expect(stats.heightM).toBeGreaterThan(160);
  });

  it("reads riders per train out of the arrangement sentence", () => {
    expect(parseRideStats(fixture("black-mamba"))!.ridersPerTrain).toBe(32);
    // Single-car trains phrase it as "riders per car" — same number, and a
    // rider count is what the field is for.
    expect(parseRideStats(fixture("gotham"))!.ridersPerTrain).toBe(4);
  });

  it("returns null for a manufacturer's model page", () => {
    // rcdb.com/12374.htm is Mack's "Custom - Mega Coaster": a Details table
    // and no track at all. Writing a row of nulls for it would claim we looked
    // up a ride, and a wrong rcdbId in the seed would go unnoticed.
    expect(parseRideStats(fixture("model-page"))).toBeNull();
  });

  it("returns null rather than throwing on junk", () => {
    expect(parseRideStats("")).toBeNull();
    expect(parseRideStats("<html><body>404</body></html>")).toBeNull();
  });

  it("leaves no markup behind, however malformed the input", () => {
    // Entity-encoded tags only become tags once decoded, and one strip pass is
    // not a strip: `<<b>b>` leaves `b>` behind. This is someone else's HTML and
    // nothing promises it is well-formed, so entities are decoded first, the
    // strip runs to a fixed point, and any angle bracket that survives is not
    // text either.
    const html = [
      "<h3>Tracks</h3><table class=stat-tbl><tbody>",
      "<tr><th>Speed<td><span class=float>49.7</span> mph",
      "<tr><th>Designer<td>&#60;i&#62;Werner Stengel&#60;/i&#62;",
      "<tr><th>Restraints<td><<b>b>Lap bar",
      "</table>",
    ].join("");

    const stats = parseRideStats(html)!;

    expect(stats.designer).toBe("Werner Stengel");
    expect(stats.restraints).not.toMatch(/[<>]/);
    expect(stats.topSpeedKmh).toBe(80);
  });
});
