#!/usr/bin/env node
/**
 * Imports researched park seasons (parties, festivals, closures) through the
 * admin seasons API, the same route the curation editor writes with, so each
 * row is validated, audited and published like a hand-made one.
 *
 *   PARKFAN_ADMIN_TOKEN=… node scripts/trip-regions/import-seasons.mjs seasons.json          # dry run
 *   PARKFAN_ADMIN_TOKEN=… node scripts/trip-regions/import-seasons.mjs seasons.json --apply  # write
 *
 * Input: a JSON array of
 *   { parkSlug, kind, name, startDate, endDate, dates?, status, separateTicket,
 *     priceFrom?, priceCurrency?, opensAt?, closesAt?, url?, sourceUrl?, notes? }
 *
 * - **Dry run by default.** It prints what it would create and what it skips;
 *   only `--apply` writes.
 * - **Skips what is already on file:** a season of the same park, kind and name
 *   whose dates overlap the row's.
 * - **Never invents an end.** A row without `endDate` (an announced closure
 *   with no reopening date) is reported and skipped; `park_seasons.end_date` is
 *   NOT NULL, and a made-up end would read as a reopening.
 * - **Auth:** `PARKFAN_ADMIN_TOKEN` holds an admin session token of an editor
 *   or owner (from `POST /v1/admin/auth/login`), sent as `Authorization:
 *   Bearer`. The token is never printed. An API token (`pfa_…`) is refused by
 *   these routes.
 * - **Target:** `PARKFAN_API_URL`, default `http://localhost:3000`. Pointing it
 *   at production is a production write: read
 *   docs/rules/automated-retirement-and-identity.md#7 first, dry-run, then apply.
 *
 * A park is found by its geographic path in src/trip-regions/regions.ts when
 * the region lists it (slugs repeat across cities), else by an exact slug match
 * in the admin park search, which must be unique.
 */
import { readFileSync } from "node:fs";

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const file = args.find((a) => !a.startsWith("--"));
if (!file) {
  console.error("Usage: import-seasons.mjs <seasons.json> [--apply]");
  process.exit(2);
}

const base = (process.env.PARKFAN_API_URL ?? "http://localhost:3000").replace(/\/$/, "");
const token = process.env.PARKFAN_ADMIN_TOKEN;
if (!token) {
  console.error("Set PARKFAN_ADMIN_TOKEN to an admin session token (editor or owner).");
  process.exit(2);
}

const { TRIP_REGIONS } = await import(
  new URL("../../src/trip-regions/regions.ts", import.meta.url).href
);

const KINDS = new Set([
  "halloween",
  "christmas",
  "summer_nights",
  "special_event",
  "opening",
  "closure",
  "maintenance",
]);
const STATUSES = new Set(["confirmed", "announced", "expected", "cancelled"]);
const DATE = /^\d{4}-\d{2}-\d{2}$/;
/** The admin route's own rule: 00:00–23:59. A midnight close is 00:00, read as the next day. */
const TIME = /^([01]\d|2[0-3]):[0-5]\d$/;

async function api(method, path, body) {
  const res = await fetch(`${base}/v1/${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = text;
  }
  if (!res.ok) {
    const message = typeof json === "object" && json ? json.message ?? json.error : json;
    throw new Error(`${method} ${path}: HTTP ${res.status} ${JSON.stringify(message)}`);
  }
  return json;
}

/** The park id for a slug: the region's geographic path first, else a unique admin match. */
const parkIds = new Map();
async function parkIdOf(slug) {
  if (parkIds.has(slug)) return parkIds.get(slug);
  let id = null;
  for (const region of Object.values(TRIP_REGIONS)) {
    const config = region.parks.find((p) => p.slug === slug);
    if (!config) continue;
    const park = await api(
      "GET",
      `parks/${config.continent}/${config.country}/${config.city}/${config.slug}`,
    );
    id = park?.id ?? null;
    break;
  }
  if (!id) {
    const found = await api("GET", `admin/content/parks?q=${encodeURIComponent(slug)}&limit=50`);
    const rows = (found?.parks ?? found?.data ?? found ?? []).filter((p) => p.slug === slug);
    if (rows.length !== 1) {
      throw new Error(`${slug}: ${rows.length} parks carry this slug; add it to a region or fix the file`);
    }
    id = rows[0].id;
  }
  parkIds.set(slug, id);
  return id;
}

/** Every season on file, once, for the duplicate check. */
async function existingSeasons() {
  const all = [];
  for (let offset = 0; ; offset += 500) {
    const page = await api("GET", `admin/content/seasons?limit=500&offset=${offset}`);
    all.push(...(page?.seasons ?? []));
    if (!page || all.length >= page.total || (page.seasons ?? []).length === 0) break;
  }
  return all;
}

const norm = (s) => (s ?? "").trim().toLowerCase();

/** What is wrong with a row before the API sees it, or null. */
function problem(row) {
  if (!row.parkSlug) return "no parkSlug";
  if (!KINDS.has(row.kind)) return `kind "${row.kind}" is not one of ${[...KINDS].join(", ")}`;
  if (!row.startDate || !DATE.test(row.startDate)) return "startDate is not YYYY-MM-DD";
  if (row.endDate == null) return "no endDate: the end is not announced, so the row waits";
  if (!DATE.test(row.endDate)) return "endDate is not YYYY-MM-DD";
  if (row.endDate < row.startDate) return "endDate is before startDate";
  if (row.status && !STATUSES.has(row.status)) return `status "${row.status}" is unknown`;
  for (const f of ["opensAt", "closesAt"]) {
    if (row[f] === "24:00") return `${f} is 24:00: write midnight as 00:00`;
    if (row[f] != null && !TIME.test(row[f])) return `${f} is not HH:MM`;
  }
  if (row.dates != null && !Array.isArray(row.dates)) return "dates is not a list";
  for (const d of row.dates ?? []) {
    if (!DATE.test(d) || d < row.startDate || d > row.endDate) {
      return `date ${d} is outside ${row.startDate} – ${row.endDate}`;
    }
  }
  return null;
}

/** The admin route's body; `notes` is the row's `note`. */
function body(row) {
  const out = {
    kind: row.kind,
    name: row.name ?? null,
    startDate: row.startDate,
    endDate: row.endDate,
    dates: row.dates && row.dates.length > 0 ? row.dates : null,
    status: row.status ?? "announced",
    separateTicket: row.separateTicket === true,
    priceFrom: row.priceFrom ?? null,
    priceCurrency: row.priceCurrency ?? null,
    opensAt: row.opensAt ?? null,
    closesAt: row.closesAt ?? null,
    url: row.url ?? null,
    sourceUrl: row.sourceUrl ?? null,
    note: row.notes ?? null,
  };
  return out;
}

const rows = JSON.parse(readFileSync(file, "utf8"));
if (!Array.isArray(rows)) {
  console.error("The file must hold a JSON array of seasons.");
  process.exit(2);
}

console.log(`${apply ? "APPLY" : "DRY RUN"} against ${base}: ${rows.length} rows from ${file}`);
const onFile = await existingSeasons();
const counts = { create: 0, exists: 0, invalid: 0, failed: 0 };

for (const [i, row] of rows.entries()) {
  const label = `#${i + 1} ${row.parkSlug} ${row.kind} "${row.name ?? ""}" ${row.startDate} – ${row.endDate ?? "?"}`;
  const issue = problem(row);
  if (issue) {
    counts.invalid++;
    console.log(`  skip   ${label}: ${issue}`);
    continue;
  }
  let parkId;
  try {
    parkId = await parkIdOf(row.parkSlug);
  } catch (err) {
    counts.invalid++;
    console.log(`  skip   ${label}: ${err.message}`);
    continue;
  }
  const duplicate = onFile.find(
    (s) =>
      (s.parkId ?? s.park?.id) === parkId &&
      s.kind === row.kind &&
      norm(s.name) === norm(row.name) &&
      s.startDate <= row.endDate &&
      s.endDate >= row.startDate,
  );
  if (duplicate) {
    counts.exists++;
    console.log(`  exists ${label} (season ${duplicate.id})`);
    continue;
  }
  if (!apply) {
    counts.create++;
    console.log(`  create ${label}`);
    continue;
  }
  try {
    const created = await api("POST", `admin/content/parks/${parkId}/seasons`, body(row));
    onFile.push({ ...created, parkId });
    counts.create++;
    console.log(`  created ${label} → ${created.id}`);
  } catch (err) {
    counts.failed++;
    console.log(`  FAILED ${label}: ${err.message}`);
  }
}

console.log(
  `${apply ? "Created" : "Would create"} ${counts.create}, already on file ${counts.exists}, ` +
    `skipped ${counts.invalid}${apply ? `, failed ${counts.failed}` : ""}.`,
);
if (counts.failed > 0) process.exit(1);
