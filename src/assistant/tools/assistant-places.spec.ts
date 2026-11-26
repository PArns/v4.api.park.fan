import { McpToolError } from "../../mcp/mcp-tool";
import {
  byWalkFrom,
  findPlace,
  parkPlaces,
  walkBetween,
  type ParkPlace,
} from "./assistant-places";

const place = (
  id: string,
  name: string,
  kind: ParkPlace["kind"] = "ride",
  latitude: number | null = 50.8,
  longitude: number | null = 6.88,
): ParkPlace => ({ id, name, kind, latitude, longitude });

describe("parkPlaces", () => {
  it("reads decimal strings and drops placeholders and nonsense", () => {
    const places = parkPlaces({
      attractions: [
        { id: "a", name: "Taron", latitude: "50.79", longitude: "6.88" },
      ],
      shows: [{ id: "s", name: "Show", latitude: 0, longitude: 0 }],
      restaurants: [{ id: "r", name: "Wok", latitude: "x", longitude: null }],
    });
    expect(places).toEqual([
      place("a", "Taron", "ride", 50.79, 6.88),
      place("s", "Show", "show", null, null),
      place("r", "Wok", "restaurant", null, null),
    ]);
  });
});

describe("findPlace", () => {
  const places = [
    place("1", "Chiapas - DIE Wasserbahn"),
    place("2", "Winja's Fear"),
    place("3", "Winja's Force"),
    place("4", "Taron"),
  ];

  it("takes the exact name over a partial one", () => {
    expect(findPlace(places, "taron", "P")).toMatchObject({ id: "4" });
  });

  it("finds a ride by part of its name", () => {
    expect(findPlace(places, "Chiapas", "P")).toMatchObject({ id: "1" });
  });

  it("hands back several matches to pick from", () => {
    expect(findPlace(places, "winja", "P")).toMatchObject({
      ambiguous: true,
      places: [{ id: "2" }, { id: "3" }],
    });
  });

  it("throws for the model when nothing matches", () => {
    expect(() => findPlace(places, "Space Mountain", "P")).toThrow(
      McpToolError,
    );
  });
});

describe("walkBetween", () => {
  it("rounds to 10 m and walks at the day planner's pace", () => {
    const a = place("a", "A", "ride", 50.8, 6.88);
    const b = place("b", "B", "ride", 50.801, 6.88);
    // 111 m straight, 1.6 × 111 / 67 m a minute.
    expect(walkBetween(a, b)).toEqual({ metres: 110, walkMinutes: 3 });
  });

  it("has no walk without both positions", () => {
    expect(
      walkBetween(place("a", "A"), place("b", "B", "ride", null, null)),
    ).toBeNull();
  });
});

describe("byWalkFrom", () => {
  it("puts the nearest first, the unplaced last and leaves the anchor out", () => {
    const anchor = place("a", "A", "restaurant", 50.8, 6.88);
    const all = [
      anchor,
      place("far", "Far", "restaurant", 50.802, 6.88),
      place("none", "None", "restaurant", null, null),
      place("near", "Near", "restaurant", 50.8005, 6.88),
    ];
    const rows = byWalkFrom(anchor, all, (p) => p);
    expect(rows.map((r) => r.item.id)).toEqual(["near", "far", "none"]);
  });
});
