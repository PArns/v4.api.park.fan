/**
 * A local stand-in for the trip-region routes and the day optimizer before
 * they are deployed, on production data. The frontend points its API base at
 * http://localhost:3001 and gets:
 *
 * - `GET /v1/trip-regions/:region`, `/calendar`, `/best-days`, `/plan`
 * - `POST /v1/parks/:continent/:country/:city/:park/plan/optimize`
 *
 * answered by the same modules the API runs (`trip-region.assembly`,
 * `trip-best-days`, `trip-plan.assembly`, `trip-optimizer`, the day
 * optimizer's `engine`), fed from production's public routes (`/calendar`,
 * `/schedule`, `/seasons`, `/plan/day`) instead of the database and Redis.
 * Every other path is passed through to production unchanged. The shim adds
 * fetching and wiring and no rule of its own, so it cannot drift.
 *
 *   pnpm trip-regions:dev                     # port 3001
 *   DEV_SEASONS_FILE=seasons.json pnpm trip-regions:dev
 *
 * - **Key:** the forecast routes need a data key; this sends none itself. In
 *   the Claude Code container the HTTPS proxy injects one. Elsewhere set
 *   `DEV_API_KEY` (sent as `X-API-Key`, never printed).
 * - **Proxy:** Node's fetch ignores `HTTPS_PROXY` unless `NODE_USE_ENV_PROXY=1`;
 *   the shim re-starts itself with it when a proxy is configured.
 * - **Seasons:** production has no Orlando seasons yet. `DEV_SEASONS_FILE`
 *   adds the rows of a researcher's JSON (the shape
 *   `import-seasons.mjs` reads), skipping a row without an end date and one
 *   production already has.
 * - **Cache:** each fetched park answer is kept 15 minutes in memory.
 * - **Known gaps:** production's `/schedule` carries no `description`, so a
 *   gap-filled CLOSED day past the published hours reads as stated; there is
 *   no `forecastUntil` (it is the warmup's Redis key) and no group name from
 *   the `destinations` table (the curated names stand in); the day optimizer
 *   gets no `ridesInWorks` and its request is not validated.
 */
import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import "reflect-metadata";
import { HttpException } from "@nestjs/common";
import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../../src/common/utils/date.util";
import { findRegion, type RegionConfig } from "../../src/trip-regions/regions";
import {
  assembleRegionCalendar,
  describeRegion,
  lastYearMonthsNeeded,
  MAX_CALENDAR_DAYS,
  publicCalendar,
  referenceRange,
  validateWindow,
  type InternalRegionCalendar,
  type ParkCalendarSource,
  type RegionParkRecord,
  type RegionSeasonSource,
} from "../../src/trip-regions/trip-region.assembly";
import { regionBestDays } from "../../src/trip-regions/trip-best-days";
import {
  lastYearMonthFromDays,
  monthBounds,
  type LastYearMonth,
} from "../../src/trip-regions/last-year-snapshot";
import type { ScheduleRowLike } from "../../src/trip-regions/trip-calendar.util";
import * as plan from "../../src/trip-regions/trip-plan.assembly";
import {
  TripBestDaysQueryDto,
  TripCalendarQueryDto,
  TripPlanQueryDto,
} from "../../src/trip-regions/dto/trip-region-query.dto";
import { runDayOptimizer } from "../../src/planner/day-optimizer/engine";
import { dayClockFor } from "../../src/planner/plan-optimize.service";

const UPSTREAM = (process.env.DEV_UPSTREAM ?? "https://api.park.fan").replace(
  /\/$/,
  "",
);
const PORT = Number(process.env.PORT ?? 3001);
const CACHE_MS = 15 * 60 * 1000;

// Node's fetch reaches the proxy only with NODE_USE_ENV_PROXY; restart with it.
if (
  (process.env.HTTPS_PROXY || process.env.https_proxy) &&
  process.env.NODE_USE_ENV_PROXY !== "1"
) {
  const child = spawn(
    process.execPath,
    process.execArgv.concat(process.argv.slice(1)),
    {
      stdio: "inherit",
      env: { ...process.env, NODE_USE_ENV_PROXY: "1" },
    },
  );
  child.on("exit", (code) => process.exit(code ?? 0));
} else {
  start();
}

class UpstreamError extends Error {
  constructor(
    readonly status: number,
    readonly body: string,
  ) {
    super(`upstream ${status}`);
  }
}

const cache = new Map<string, { at: number; value: Promise<unknown> }>();

/** A production JSON answer, kept 15 minutes. */
function upstreamJson<T>(path: string): Promise<T> {
  const hit = cache.get(path);
  if (hit && Date.now() - hit.at < CACHE_MS) return hit.value as Promise<T>;
  const value = (async () => {
    const res = await fetch(`${UPSTREAM}${path}`, {
      headers: {
        Accept: "application/json",
        ...(process.env.DEV_API_KEY && {
          "X-API-Key": process.env.DEV_API_KEY,
        }),
      },
    });
    const text = await res.text();
    if (!res.ok) throw new UpstreamError(res.status, text);
    return JSON.parse(text) as T;
  })();
  cache.set(path, { at: Date.now(), value });
  value.catch(() => cache.delete(path));
  return value;
}

const parkBase = (c: RegionParkRecord["config"]) =>
  `/v1/parks/${c.continent}/${c.country}/${c.city}/${c.slug}`;

/** The region's parks from production's park payloads (retired ones answer 404 there too). */
async function regionParks(region: RegionConfig): Promise<RegionParkRecord[]> {
  const found = await Promise.all(
    region.parks.map(async (config) => {
      try {
        const park = await upstreamJson<{
          id: string;
          slug: string;
          name: string;
          latitude: number | string;
          longitude: number | string;
          timezone: string;
        }>(parkBase(config));
        const record: RegionParkRecord = {
          config,
          id: park.id,
          slug: park.slug,
          name: park.name,
          path: `/parks/${config.continent}/${config.country}/${config.city}/${config.slug}`,
          latitude: Number(park.latitude),
          longitude: Number(park.longitude),
          timezone: park.timezone,
        };
        return record;
      } catch {
        return null;
      }
    }),
  );
  return found.filter((p): p is RegionParkRecord => p !== null);
}

interface DevSeasonRow {
  parkSlug: string;
  kind: string;
  name?: string | null;
  startDate: string;
  endDate?: string | null;
  dates?: string[] | null;
  status?: string;
  separateTicket?: boolean;
  priceFrom?: number | null;
  priceCurrency?: string | null;
  opensAt?: string | null;
  closesAt?: string | null;
  url?: string | null;
  sourceUrl?: string | null;
}

function devSeasons(): DevSeasonRow[] {
  const file = process.env.DEV_SEASONS_FILE;
  if (!file) return [];
  return JSON.parse(readFileSync(file, "utf8")) as DevSeasonRow[];
}

/** Production's seasons of a park in a window, plus the dev file's rows it lacks. */
async function seasonsFor(
  park: RegionParkRecord,
  from: string,
  to: string,
): Promise<RegionSeasonSource[]> {
  const live = await upstreamJson<{ seasons: RegionSeasonSource[] }>(
    `${parkBase(park.config)}/seasons?from=${from}&to=${to}`,
  ).catch(() => ({ seasons: [] as RegionSeasonSource[] }));
  const out = live.seasons.map((s) => ({ ...s, parkId: park.id }));
  devSeasons().forEach((row, i) => {
    if (row.parkSlug !== park.slug || !row.endDate) return;
    if (row.startDate > to || row.endDate < from) return;
    const known = out.some(
      (s) =>
        s.kind === row.kind &&
        (s.name ?? "").toLowerCase() === (row.name ?? "").toLowerCase() &&
        s.startDate <= (row.endDate as string) &&
        s.endDate >= row.startDate,
    );
    if (known) return;
    out.push({
      id: `dev-season-${i + 1}`,
      parkId: park.id,
      kind: row.kind,
      name: row.name ?? null,
      startDate: row.startDate,
      endDate: row.endDate,
      dates: row.dates && row.dates.length > 0 ? row.dates : null,
      status: row.status ?? "announced",
      separateTicket: row.separateTicket === true,
      priceFrom:
        row.priceFrom != null ? Number(row.priceFrom).toFixed(2) : null,
      priceCurrency: row.priceCurrency ?? null,
      opensAt: row.opensAt ?? null,
      closesAt: row.closesAt ?? null,
      attractionIds: null,
      url: row.url ?? null,
      sourceUrl: row.sourceUrl ?? null,
      confirmedAt: null,
    });
  });
  return out;
}

/** The calendar as `TripRegionService.buildCalendar` assembles it, from production. */
async function regionCalendar(
  region: RegionConfig,
  from: string,
  to: string,
): Promise<InternalRegionCalendar> {
  const today = getCurrentDateInTimezone(region.timezone);
  const parks = await regionParks(region);
  // The window and its reference span, as TripRegionService reads them.
  const ref = referenceRange(from, to);
  const sources: ParkCalendarSource[] = await Promise.all(
    parks.map(async (park) => {
      const base = parkBase(park.config);
      const [calendar, schedule] = await Promise.all([
        upstreamJson<ParkCalendarSource["calendar"]>(
          `${base}/calendar?from=${ref.from}&to=${ref.to}&includeHourly=none`,
        ),
        upstreamJson<{ schedule: ScheduleRowLike[] }>(
          `${base}/schedule?from=${ref.from}&to=${ref.to}`,
        ).catch(() => ({ schedule: [] as ScheduleRowLike[] })),
      ]);
      const lastYear = new Map<string, LastYearMonth>();
      const wanted = calendar.days.filter(
        (d) => d.date >= from && d.date <= to,
      );
      for (const ym of lastYearMonthsNeeded(wanted, today)) {
        const { first, last } = monthBounds(ym);
        const past = await upstreamJson<ParkCalendarSource["calendar"]>(
          `${base}/calendar?from=${first}&to=${last}&includeHourly=none`,
        ).catch(() => null);
        if (past) lastYear.set(ym, lastYearMonthFromDays(past.days, today));
      }
      return { park, calendar, rows: schedule.schedule, lastYear };
    }),
  );
  const seasons = (
    await Promise.all(parks.map((p) => seasonsFor(p, from, to)))
  ).flat();
  return assembleRegionCalendar({
    region,
    from,
    to,
    today,
    computedAt: new Date().toISOString(),
    sources,
    seasons,
    forecastUntil: [],
  });
}

/** Query validation exactly as the API's ValidationPipe runs it. */
async function query<T extends object>(dto: new () => T, url: URL): Promise<T> {
  const instance = plainToInstance(dto, Object.fromEntries(url.searchParams), {
    enableImplicitConversion: true,
  });
  const errors = await validate(instance, {
    whitelist: true,
    forbidNonWhitelisted: true,
  });
  if (errors.length > 0) {
    throw new UpstreamError(
      400,
      JSON.stringify({
        statusCode: 400,
        message: errors.flatMap((e) => Object.values(e.constraints ?? {})),
        error: "Bad Request",
      }),
    );
  }
  return instance;
}

function regionOr404(slug: string): RegionConfig {
  const region = findRegion(slug);
  if (!region) {
    throw new UpstreamError(
      404,
      JSON.stringify({ statusCode: 404, message: `No trip region "${slug}"` }),
    );
  }
  return region;
}

async function readBody(req: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks);
}

/** The routes the shim answers itself; null for everything it passes on. */
async function route(req: IncomingMessage, url: URL): Promise<unknown | null> {
  const trip =
    /^\/v1\/trip-regions\/([^/]+)(?:\/(calendar|best-days|plan))?\/?$/.exec(
      url.pathname,
    );
  if (trip && req.method === "GET") {
    const region = regionOr404(trip[1]);
    const today = getCurrentDateInTimezone(region.timezone);
    switch (trip[2]) {
      case undefined:
        return describeRegion(region, await regionParks(region));
      case "calendar": {
        const q = await query(TripCalendarQueryDto, url);
        const from = q.from ?? today;
        const to = q.to ?? addIsoDays(from, 30);
        validateWindow(from, to, MAX_CALENDAR_DAYS, today);
        return publicCalendar(await regionCalendar(region, from, to));
      }
      case "best-days": {
        const q = await query(TripBestDaysQueryDto, url);
        const from = q.from ?? today;
        const to = q.to ?? addIsoDays(from, 30);
        validateWindow(from, to, MAX_CALENDAR_DAYS, today);
        return regionBestDays(
          region,
          await regionCalendar(region, from, to),
          q.limit ?? 5,
        );
      }
      case "plan": {
        const q = await query(TripPlanQueryDto, url);
        const parks = await regionParks(region);
        const normalized = plan.normalizePlanQuery(
          region,
          parks.map((p) => p.config.slug),
          q,
          today,
        );
        const calendar = await regionCalendar(
          region,
          normalized.from,
          normalized.to,
        );
        const optimized = plan.optimizePlan(region, normalized, calendar);
        // The measured error at each planned visit's lead, as `/plan/day` serves it.
        const leadMae = new Map<number, number | null>();
        for (const lead of plan.plannedLeads(optimized, today)) {
          const date = addIsoDays(today, lead);
          const day = await upstreamJson<{ leadTimeMae?: number | null }>(
            `${parkBase(parks[0].config)}/plan/day?date=${date}`,
          ).catch(() => null);
          leadMae.set(lead, day?.leadTimeMae ?? null);
        }
        return plan.planResponse(region, normalized, calendar, optimized, {
          computedAt: new Date().toISOString(),
          today,
          leadMae,
        });
      }
    }
  }

  const optimize =
    /^\/v1\/parks\/([^/]+)\/([^/]+)\/([^/]+)\/([^/]+)\/plan\/optimize\/?$/.exec(
      url.pathname,
    );
  if (optimize && req.method === "POST") {
    const [, continent, country, city, slug] = optimize;
    const base = `/v1/parks/${continent}/${country}/${city}/${slug}`;
    const body = JSON.parse((await readBody(req)).toString("utf8") || "{}");
    const park = await upstreamJson<{ timezone: string }>(base);
    const clock = dayClockFor(park.timezone, body.date, body.nowMinute);
    const day =
      clock.phase === "past"
        ? null
        : await upstreamJson<Parameters<typeof runDayOptimizer>[0]>(
            `${base}/plan/day?date=${body.date}`,
          );
    return { date: body.date, ...runDayOptimizer(day, body, clock) };
  }
  return null;
}

/** Everything the shim does not answer goes to production as it came. */
async function passThrough(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
): Promise<void> {
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(req.headers)) {
    if (["host", "connection", "content-length"].includes(name)) continue;
    if (typeof value === "string") headers[name] = value;
  }
  const hasBody = req.method !== "GET" && req.method !== "HEAD";
  const upstream = await fetch(`${UPSTREAM}${url.pathname}${url.search}`, {
    method: req.method,
    headers,
    body: hasBody ? new Uint8Array(await readBody(req)) : undefined,
  });
  const out: Record<string, string> = {};
  upstream.headers.forEach((value, name) => {
    // fetch has already decoded the body.
    if (
      !["content-encoding", "content-length", "transfer-encoding"].includes(
        name,
      )
    ) {
      out[name] = value;
    }
  });
  res.writeHead(upstream.status, out);
  res.end(Buffer.from(await upstream.arrayBuffer()));
}

function start(): void {
  createServer((req, res) => {
    const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);
    const started = Date.now();
    void (async () => {
      try {
        const body = await route(req, url);
        if (body === null) {
          await passThrough(req, res, url);
        } else {
          res.writeHead(200, {
            "Content-Type": "application/json; charset=utf-8",
            "Cache-Control": "private, no-store",
          });
          res.end(
            JSON.stringify(body, (_k, v) => (v === null ? undefined : v)),
          );
        }
      } catch (err) {
        if (err instanceof HttpException) {
          const response = err.getResponse();
          res.writeHead(err.getStatus(), {
            "Content-Type": "application/json",
          });
          res.end(
            JSON.stringify(
              typeof response === "string"
                ? { statusCode: err.getStatus(), message: response }
                : response,
            ),
          );
        } else if (err instanceof UpstreamError) {
          res.writeHead(err.status, { "Content-Type": "application/json" });
          res.end(err.body);
        } else {
          // The detail goes to this terminal only; a caller learns nothing about the shim's internals.
          console.error(err);
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(
            JSON.stringify({ statusCode: 500, message: "Internal server error" }),
          );
        }
      } finally {
        console.log(
          `${req.method} ${url.pathname} ${res.statusCode} ${Date.now() - started}ms`,
        );
      }
    })();
  }).listen(PORT, () => {
    console.log(
      `trip-regions dev shim on http://localhost:${PORT} → ${UPSTREAM}`,
    );
  });
}
