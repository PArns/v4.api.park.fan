import { insideRings, outlineRings } from "./outline.geometry";
import type { OverpassElement } from "./overpass.parser";

const g = (...points: [number, number][]) =>
  points.map(([lon, lat]) => ({ lon, lat }));

describe("outlineRings", () => {
  it("reads a closed way as one ring", () => {
    const way: OverpassElement = {
      type: "way",
      id: 1,
      geometry: g([0, 0], [2, 0], [2, 2], [0, 2], [0, 0]),
    };
    expect(outlineRings(way)).toEqual([
      [
        [0, 0],
        [2, 0],
        [2, 2],
        [0, 2],
        [0, 0],
      ],
    ]);
  });

  it("joins a relation's outer ways, whichever way round they are drawn", () => {
    const relation: OverpassElement = {
      type: "relation",
      id: 1,
      members: [
        { type: "way", role: "outer", geometry: g([0, 0], [2, 0], [2, 2]) },
        // Drawn backwards: ends where the first one ends.
        { type: "way", role: "outer", geometry: g([0, 0], [0, 2], [2, 2]) },
        { type: "way", role: "inner", geometry: g([1, 1], [1, 1.5], [1, 1]) },
        { type: "node", role: "label" },
      ],
    };
    const rings = outlineRings(relation);
    expect(rings).toHaveLength(1);
    expect(rings[0]).toHaveLength(5);
    expect(insideRings(1, 1, rings)).toBe(true);
  });

  it("gives no ring for an outline that does not close or has no geometry", () => {
    expect(
      outlineRings({
        type: "way",
        id: 1,
        geometry: g([0, 0], [2, 0], [2, 2]),
      }),
    ).toEqual([]);
    expect(outlineRings({ type: "relation", id: 1 })).toEqual([]);
    expect(outlineRings({ type: "way", id: 1 })).toEqual([]);
  });
});

describe("insideRings", () => {
  const square: [number, number][] = [
    [0, 0],
    [2, 0],
    [2, 2],
    [0, 2],
    [0, 0],
  ];
  const island: [number, number][] = [
    [5, 5],
    [6, 5],
    [6, 6],
    [5, 6],
    [5, 5],
  ];

  it("tells inside from outside, in any of several rings", () => {
    expect(insideRings(1, 1, [square])).toBe(true);
    expect(insideRings(3, 1, [square])).toBe(false);
    expect(insideRings(5.5, 5.5, [square, island])).toBe(true);
    expect(insideRings(1, 1, [])).toBe(false);
  });
});
