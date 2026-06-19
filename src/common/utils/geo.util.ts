/**
 * Minimal geometry helpers for matching a point (a park's lon/lat) to a
 * warning area. No external deps: a bbox pre-filter plus ray-casting
 * point-in-polygon for GeoJSON Polygon / MultiPolygon.
 */

export type Bbox = [number, number, number, number]; // [west, south, east, north]

/** Whether (lon, lat) falls inside the bbox (inclusive). */
export function bboxContains(
  bbox: Bbox | undefined | null,
  lon: number,
  lat: number,
): boolean {
  if (!bbox) return false;
  const [w, s, e, n] = bbox;
  return lon >= w && lon <= e && lat >= s && lat <= n;
}

/** Ray-casting test for a point in a single linear ring ([ [lon,lat], … ]). */
function pointInRing(lon: number, lat: number, ring: number[][]): boolean {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0],
      yi = ring[i][1];
    const xj = ring[j][0],
      yj = ring[j][1];
    const intersects =
      yi > lat !== yj > lat && lon < ((xj - xi) * (lat - yi)) / (yj - yi) + xi;
    if (intersects) inside = !inside;
  }
  return inside;
}

/** Point inside a Polygon (outer ring minus holes). */
function pointInSinglePolygon(
  lon: number,
  lat: number,
  rings: number[][][],
): boolean {
  if (rings.length === 0) return false;
  if (!pointInRing(lon, lat, rings[0])) return false; // outside outer ring
  for (let h = 1; h < rings.length; h++) {
    if (pointInRing(lon, lat, rings[h])) return false; // inside a hole
  }
  return true;
}

/**
 * Point-in-polygon for a GeoJSON geometry. Accepts Polygon
 * (`number[][][]`) and MultiPolygon (`number[][][][]`); returns false for
 * anything else or missing coords.
 */
export function pointInPolygon(
  lon: number,
  lat: number,
  geometry: { type?: string; coordinates?: unknown } | null | undefined,
): boolean {
  const coords = geometry?.coordinates as unknown[] | undefined;
  if (!coords || coords.length === 0) return false;
  const type = geometry?.type;

  if (type === "MultiPolygon") {
    return (coords as number[][][][]).some((poly) =>
      pointInSinglePolygon(lon, lat, poly),
    );
  }
  // Default to Polygon shape.
  return pointInSinglePolygon(lon, lat, coords as number[][][]);
}
