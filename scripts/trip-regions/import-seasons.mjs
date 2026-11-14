#!/usr/bin/env node
/**
 * Imports researched park seasons (parties, festivals, closures) through the
 * admin MCP server's `import_trip_seasons`, which checks, audits and publishes
 * each row as the admin's season form does. The matching, the checks and the
 * report are the server's (`src/admin/content/season-import.ts`); this only
 * sends a file and prints the answer. Claude does the same through the
 * connector (docs/admin/mcp.md#writing).
 *
 *   PARKFAN_API_TOKEN=pfa_… node scripts/trip-regions/import-seasons.mjs seasons.json          # dry run
 *   PARKFAN_API_TOKEN=pfa_… node scripts/trip-regions/import-seasons.mjs seasons.json --apply  # write
 *
 * Input: a JSON array of
 *   { region?, parkSlug, kind, name, startDate, endDate, dates?, status,
 *     separateTicket, priceFrom?, priceCurrency?, opensAt?, closesAt?, url?,
 *     sourceUrl?, notes? }
 *
 * - **Dry run by default.** It prints what the server would create and skip;
 *   only `--apply` writes.
 * - **Skips what is already on file:** a season of the same park, kind and name
 *   whose dates overlap the row's, and a row that repeats an earlier one.
 * - **Never invents an end.** A row without `endDate` is reported and skipped.
 * - **Auth:** `PARKFAN_API_TOKEN` holds a personal access token with the
 *   `admin:write` scope, of an editor or owner (the admin account page, "API
 *   access / MCP"), sent as `Authorization: Bearer`. It is never printed.
 * - **Target:** `PARKFAN_API_URL`, default `http://localhost:3000`. Pointing it
 *   at production is a production write: read
 *   docs/rules/automated-retirement-and-identity.md#7 first, dry-run, then apply.
 */
import { readFileSync } from "node:fs";

/** What one call takes; the server says the same. */
const MAX_ROWS = 100;

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const file = args.find((a) => !a.startsWith("--"));
if (!file) {
  console.error("Usage: import-seasons.mjs <seasons.json> [--apply]");
  process.exit(2);
}

const base = (process.env.PARKFAN_API_URL ?? "http://localhost:3000").replace(/\/$/, "");
const token = process.env.PARKFAN_API_TOKEN;
if (!token) {
  console.error("Set PARKFAN_API_TOKEN to a personal access token with admin:write.");
  process.exit(2);
}

const rows = JSON.parse(readFileSync(file, "utf8"));
if (!Array.isArray(rows)) {
  console.error("The file must hold a JSON array of seasons.");
  process.exit(2);
}
if (rows.length > MAX_ROWS) {
  console.error(`The file has ${rows.length} rows; one import takes ${MAX_ROWS}. Split it.`);
  process.exit(2);
}

console.log(`${apply ? "APPLY" : "DRY RUN"} against ${base}: ${rows.length} rows from ${file}`);

const res = await fetch(`${base}/v1/mcp`, {
  method: "POST",
  headers: {
    Authorization: `Bearer ${token}`,
    Accept: "application/json, text/event-stream",
    "Content-Type": "application/json",
  },
  body: JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "tools/call",
    params: { name: "import_trip_seasons", arguments: { rows, dryRun: !apply } },
  }),
});
const answer = await res.json().catch(() => null);
const result = answer?.result;
const text = result?.content?.[0]?.text;
if (!res.ok || !result || result.isError) {
  const why = text ?? answer?.error?.message ?? answer?.error_description ?? `HTTP ${res.status}`;
  console.error(`The import was refused: ${why}`);
  process.exit(1);
}

const report = JSON.parse(text);
for (const row of report.rows) {
  const id = row.seasonId ? ` (season ${row.seasonId})` : "";
  const reason = row.reason ? `: ${row.reason}` : "";
  console.log(`  ${row.outcome.padEnd(7)} ${row.label}${id}${reason}`);
}
const { create, exists, invalid, failed } = report.counts;
console.log(
  `${apply ? "Created" : "Would create"} ${create}, already on file ${exists}, ` +
    `skipped ${invalid}${apply ? `, failed ${failed}` : ""}.`,
);
if (failed > 0) process.exit(1);
