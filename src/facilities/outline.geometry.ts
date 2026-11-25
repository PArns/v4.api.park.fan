import type { OutlineRings } from "./facility.types";
import type { OverpassElement } from "./overpass.parser";

type Point = [number, number];

/**
 * The closed outer rings of a way or multipolygon relation fetched with
 * `out geom`. A relation's outer member ways are joined end to end; a chain
 * that does not close is left out, so a broken outline yields no ring and the
 * import refuses it instead of testing points against half a park.
 */
export function outlineRings(element: OverpassElement): OutlineRings {
  const line = (g: { lat: number; lon: number }[]): Point[] =>
    g.map((p) => [p.lon, p.lat]);
  const same = (a: Point, b: Point) => a[0] === b[0] && a[1] === b[1];
  const segments: Point[][] =
    element.type === "way"
      ? [line(element.geometry ?? [])]
      : (element.members ?? [])
          .filter((m) => m.type === "way" && m.role !== "inner" && m.geometry)
          .map((m) => line(m.geometry!));
  const rings: OutlineRings = [];
  while (segments.length > 0) {
    const ring = segments.shift()!;
    let grew = true;
    while (grew && ring.length > 0 && !same(ring[0], ring[ring.length - 1])) {
      grew = false;
      for (let i = 0; i < segments.length; i++) {
        const next = segments[i];
        const end = ring[ring.length - 1];
        const forward = same(next[0], end);
        if (forward || same(next[next.length - 1], end)) {
          ring.push(...(forward ? next : [...next].reverse()).slice(1));
          segments.splice(i, 1);
          grew = true;
          break;
        }
      }
    }
    if (ring.length >= 4 && same(ring[0], ring[ring.length - 1])) {
      rings.push(ring);
    }
  }
  return rings;
}

/** Whether a point lies inside any of the rings (ray casting). */
export function insideRings(
  longitude: number,
  latitude: number,
  rings: OutlineRings,
): boolean {
  return rings.some((ring) => {
    let inside = false;
    for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
      const [x1, y1] = ring[i];
      const [x2, y2] = ring[j];
      if (
        y1 > latitude !== y2 > latitude &&
        longitude < ((x2 - x1) * (latitude - y1)) / (y2 - y1) + x1
      ) {
        inside = !inside;
      }
    }
    return inside;
  });
}
