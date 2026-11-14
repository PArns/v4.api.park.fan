#!/usr/bin/env node
/**
 * Builds a region's entry of src/trip-regions/travel-matrix.generated.ts: the
 * driving distance and time between every pair of its parks, hotel areas and
 * evening venues (a venue only to and from its own resort's parks), read once
 * from OpenStreetMap through the public OSRM demo server and stored in code.
 * The other regions' entries are kept as they are. A region whose travel is
 * curated (`travel.source: "curated"`, Japan's trains) has no entry: its legs
 * are in `regions.ts`.
 *
 *   node scripts/trip-regions/build-travel-matrix.mjs [region]   # default: orlando
 *
 * Needs Node with TypeScript type stripping (22.18+ by default, 26 in CI), because
 * it imports the coordinates straight from src/trip-regions/regions.ts: the car
 * park a leg ends at is defined once, there.
 *
 * - Free flow: OSRM knows no traffic. The API says so (`basis: "free_flow"`).
 * - Both directions are asked for and averaged, so one leg serves both ways.
 * - km to one decimal, minutes rounded.
 * - One table request for the whole region (the demo server takes up to 100
 *   coordinates). `fetch` first; where it cannot reach the server (a proxy that
 *   only curl is configured for), the same URL goes through `curl`.
 *
 * The OSRM demo server is for light use; this runs by hand when a car park
 * moves or a park joins the region, not in CI.
 */
import { execFileSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..", "..");
const { TRIP_REGIONS } = await import(
  new URL("../../src/trip-regions/regions.ts", import.meta.url).href
);

const { TRAVEL_MATRICES: existing } = await import(
  new URL("../../src/trip-regions/travel-matrix.generated.ts", import.meta.url)
    .href
);

const regionSlug = process.argv[2] ?? "orlando";
const region = TRIP_REGIONS[regionSlug];
if (!region) {
  console.error(`Unknown region "${regionSlug}"`);
  process.exit(1);
}
if (region.travel?.source !== "osrm") {
  console.error(
    `${regionSlug}'s travel is curated in regions.ts, not read from OSRM`,
  );
  process.exit(1);
}
const noCarPark = region.parks.filter((p) => !p.carPark).map((p) => p.slug);
if (noCarPark.length > 0) {
  console.error(`No carPark for ${noCarPark.join(", ")}`);
  process.exit(1);
}

const points = [
  ...region.parks.map((p) => ({
    id: p.slug,
    latitude: p.carPark.latitude,
    longitude: p.carPark.longitude,
  })),
  ...region.hotelAreas.map((h) => ({
    id: `hotel:${h.key}`,
    latitude: h.latitude,
    longitude: h.longitude,
  })),
  ...(region.venues ?? []).map((v) => ({
    id: `venue:${v.key}`,
    group: v.group,
    latitude: v.latitude,
    longitude: v.longitude,
  })),
];
const groupOf = new Map(region.parks.map((p) => [p.slug, p.group]));

const coords = points.map((p) => `${p.longitude},${p.latitude}`).join(";");
const url = `https://router.project-osrm.org/table/v1/driving/${coords}?annotations=duration,distance`;

async function getJson(target) {
  try {
    const res = await fetch(target, {
      headers: { "User-Agent": "parkfan-travel-matrix/1" },
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } catch (err) {
    console.warn(`fetch failed (${err.message}); retrying with curl`);
    const out = execFileSync(
      "curl",
      [
        "-sS",
        "--fail",
        "--max-time",
        "60",
        "-A",
        "parkfan-travel-matrix/1",
        target,
      ],
      { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 },
    );
    return JSON.parse(out);
  }
}

const table = await getJson(url);
if (table.code !== "Ok") {
  console.error(`OSRM answered ${table.code}: ${table.message ?? ""}`);
  process.exit(1);
}

// How far OSRM moved each point to reach a road. A large snap is how the first
// draft put Volcano Bay 16.7 km from Universal Studios: its centroid snapped to
// a service road on the far side of the resort.
const SNAP_WARN_M = 150;
points.forEach((p, i) => {
  const snap = table.sources[i]?.distance ?? 0;
  if (snap > SNAP_WARN_M) {
    console.warn(
      `⚠️  ${p.id} snapped ${snap.toFixed(0)} m to "${table.sources[i].name}"`,
    );
  }
});

const legs = [];
for (let i = 0; i < points.length; i++) {
  for (let j = i + 1; j < points.length; j++) {
    const a = points[i].id;
    const b = points[j].id;
    // Hotel to hotel is no leg anybody plans, and an evening goes to a venue
    // from a park of the venue's own resort only.
    const isPark = (id) => !id.includes(":");
    if (!isPark(a) && !isPark(b)) continue;
    const venue = [points[i], points[j]].find((p) => p.group);
    const park = isPark(a) ? a : b;
    if (venue && groupOf.get(park) !== venue.group) continue;
    const meters = (table.distances[i][j] + table.distances[j][i]) / 2;
    const seconds = (table.durations[i][j] + table.durations[j][i]) / 2;
    legs.push({
      from: a,
      to: b,
      km: Math.round(meters / 100) / 10,
      minutes: Math.round(seconds / 60),
    });
  }
}

const computedOn = new Date().toISOString().slice(0, 10);
const matrices = {
  ...existing,
  [regionSlug]: {
    region: regionSlug,
    source: "osrm",
    computedOn,
    attribution: "© OpenStreetMap contributors",
    basis: "free_flow",
    legs,
  },
};
const entry = (slug, m) => `  ${JSON.stringify(slug)}: {
    region: ${JSON.stringify(m.region)},
    source: "osrm",
    computedOn: ${JSON.stringify(m.computedOn)},
    attribution: "© OpenStreetMap contributors",
    basis: "free_flow",
    legs: [
${m.legs
  .map(
    (l) =>
      `      { from: ${JSON.stringify(l.from)}, to: ${JSON.stringify(l.to)}, km: ${l.km}, minutes: ${l.minutes} },`,
  )
  .join("\n")}
    ],
  },`;
const body = `// Generated by scripts/trip-regions/build-travel-matrix.mjs — do not edit by hand.
// Driving distance and time between each OSRM region's car parks, hotel areas
// and evening venues (src/trip-regions/regions.ts), from OpenStreetMap through
// OSRM, with no traffic. Both directions averaged; km to one decimal, minutes rounded.

/** One drive, valid both ways. \`from\`/\`to\` are park slugs, \`hotel:<key>\` or \`venue:<key>\`. */
export interface GeneratedLeg {
  from: string;
  to: string;
  km: number;
  minutes: number;
}

export interface GeneratedTravelMatrix {
  region: string;
  source: "osrm";
  computedOn: string;
  attribution: string;
  basis: "free_flow";
  legs: GeneratedLeg[];
}

export const TRAVEL_MATRICES: Record<string, GeneratedTravelMatrix> = {
${Object.keys(matrices)
  .sort()
  .map((slug) => entry(slug, matrices[slug]))
  .join("\n")}
};
`;

const outFile = join(root, "src", "trip-regions", "travel-matrix.generated.ts");
writeFileSync(outFile, body);
// The lint step runs Prettier over src/; format here so the file passes as written.
try {
  execFileSync(
    join(root, "node_modules", ".bin", "prettier"),
    ["--write", outFile],
    {
      stdio: "inherit",
    },
  );
} catch {
  console.warn("prettier not available: run `pnpm format` before committing");
}
console.log(`Wrote ${legs.length} legs for ${regionSlug} to ${outFile}`);
