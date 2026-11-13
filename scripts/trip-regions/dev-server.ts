/**
 * A local stand-in for the trip-region routes and the day optimizer before
 * they are deployed, on production data. The frontend points its API base at
 * http://localhost:3001 and gets:
 *
 * - `GET /v1/trip-regions/:region`, `/calendar`, `/best-days`, `/plan`,
 *   `/when`, `/school-breaks`, `/tickets`
 * - `POST /v1/parks/:continent/:country/:city/:park/plan/optimize`
 *
 * answered by the same modules the API runs (`trip-region.assembly`,
 * `trip-best-days`, `trip-plan.assembly`, `trip-optimizer`, `trip-when`,
 * `trip-when.assembly`, `trip-school-breaks`, the day optimizer's `engine`),
 * fed from production's public routes (`/calendar`, `/schedule`, `/seasons`,
 * `/plan/day`, each read in spans of at most 90 days) instead of the database
 * and Redis, and the school breaks from OpenHolidays and the curated sets
 * through the sync's own `openHolidaysSchoolRows`.
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
 * - **Tickets:** the shim has no database, so its ticket catalog is the seed
 *   (`tickets.ts` with `ticket-seed-links.ts`, `seedCatalog`), put on the region
 *   explicitly like `TicketCatalogService.withTickets` puts the table's. An
 *   edit made in production's admin is not seen here; production's
 *   `/tickets` route serves it.
 * - **Known gaps:** production's `/schedule` carries no `description`, so a
 *   gap-filled CLOSED day past the published hours reads as stated; there is
 *   no `forecastUntil` (it is the warmup's Redis key) and no group name from
 *   the `destinations` table (the curated names stand in); the day optimizer
 *   gets no `ridesInWorks` and its request is not validated. `/when` reads its
 *   climate normals off Magic Kingdom's calendar days marked `climate_normal`,
 *   so the first two weeks (a forecast there) have none; its school coverage
 *   is what OpenHolidays holds for the next two years, not the database's.
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
  seedCatalog,
  ticketCatalogResponse,
  withCatalog,
} from "../../src/trip-regions/ticket-catalog";
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
  eveningShowsWanted,
  eveningTriggers,
  showsKey,
  type EveningShow,
  type EveningShows,
} from "../../src/trip-regions/trip-evening";
import {
  TripBestDaysQueryDto,
  TripCalendarQueryDto,
  TripPlanQueryDto,
} from "../../src/trip-regions/dto/trip-region-query.dto";
import { runDayOptimizer } from "../../src/planner/day-optimizer/engine";
import { findTripDates } from "../../src/trip-regions/trip-when";
import {
  CLIMATE_REFERENCE_PARK,
  normalizeWhenQuery,
  whenRange,
} from "../../src/trip-regions/trip-when.assembly";
import {
  countryOfCode,
  DEFAULT_SCHOOL_BREAK_COUNTRIES,
  defaultSchoolBreakCountries,
  resolveWithin,
  schoolBreakRegions,
  type SchoolRowLike,
  type TripSchoolBreaks,
} from "../../src/trip-regions/trip-school-breaks";
import {
  TripSchoolBreaksQueryDto,
  TripWhenQueryDto,
} from "../../src/trip-regions/dto/trip-when.dto";
import { openHolidaysSchoolRows } from "../../src/holidays/holidays.service";
import {
  CURATED_HOLIDAY_SETS,
  expandCuratedHolidays,
} from "../../src/holidays/data/curated-holidays";
import {
  buildSchoolCoverage,
  type SchoolCoverage,
} from "../../src/common/utils/holiday.utils";
import type { ClimateNormals } from "../../src/external-apis/weather/climate-normals";
import type { OpenHolidaysEntry } from "../../src/external-apis/open-holidays/open-holidays.types";
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

/**
 * Production's park routes refuse a range over 90 days, and a span of 90 calendar days that
 * crosses a DST change measures longer than that, so the shim stitches 89-day spans.
 */
const UPSTREAM_SPAN_DAYS = 89;

function spans(from: string, to: string): Array<[string, string]> {
  const out: Array<[string, string]> = [];
  for (let f = from; f <= to; f = addIsoDays(f, UPSTREAM_SPAN_DAYS)) {
    const t = addIsoDays(f, UPSTREAM_SPAN_DAYS - 1);
    out.push([f, t < to ? t : to]);
  }
  return out;
}

/** A park's `/calendar` over any range, read in spans. */
async function parkCalendar(
  base: string,
  from: string,
  to: string,
): Promise<ParkCalendarSource["calendar"]> {
  const parts = await Promise.all(
    spans(from, to).map(([f, t]) =>
      upstreamJson<ParkCalendarSource["calendar"]>(
        `${base}/calendar?from=${f}&to=${t}&includeHourly=none`,
      ),
    ),
  );
  return { ...parts[0], days: parts.flatMap((p) => p.days) };
}

/** A park's `/schedule` rows over any range, read in spans. */
async function parkSchedule(
  base: string,
  from: string,
  to: string,
): Promise<ScheduleRowLike[]> {
  const parts = await Promise.all(
    spans(from, to).map(([f, t]) =>
      upstreamJson<{ schedule: ScheduleRowLike[] }>(
        `${base}/schedule?from=${f}&to=${t}`,
      ).catch(() => ({ schedule: [] as ScheduleRowLike[] })),
    ),
  );
  return parts.flatMap((p) => p.schedule);
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
        parkCalendar(base, ref.from, ref.to),
        parkSchedule(base, ref.from, ref.to),
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
      return { park, calendar, rows: schedule, lastYear };
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

/** OpenHolidays, where the holiday sync reads the school breaks from. */
const OPEN_HOLIDAYS = "https://openholidaysapi.org";
/** How far ahead the shim reads school rows: what the coverage dates are read off. */
const SCHOOL_DAYS_AHEAD = 730;

/**
 * A country's school rows as the sync would store them: OpenHolidays through
 * `openHolidaysSchoolRows`, plus the curated sets' school rows.
 */
async function schoolRows(
  country: string,
  today: string,
): Promise<SchoolRowLike[]> {
  const to = addIsoDays(today, SCHOOL_DAYS_AHEAD);
  const key = `${OPEN_HOLIDAYS}/SchoolHolidays?countryIsoCode=${country}&validFrom=${today}&validTo=${to}&languageIsoCode=EN`;
  const hit = cache.get(key);
  let entries: Promise<OpenHolidaysEntry[]>;
  if (hit && Date.now() - hit.at < CACHE_MS) {
    entries = hit.value as Promise<OpenHolidaysEntry[]>;
  } else {
    entries = fetch(key, { headers: { Accept: "application/json" } }).then(
      async (res) =>
        res.ok ? ((await res.json()) as OpenHolidaysEntry[]) : [],
    );
    cache.set(key, { at: Date.now(), value: entries });
    entries.catch(() => cache.delete(key));
  }
  const curated = CURATED_HOLIDAY_SETS.filter((set) => set.country === country)
    .flatMap(expandCuratedHolidays)
    .filter((row) => row.holidayType === "school");
  return [...openHolidaysSchoolRows(await entries, country), ...curated].map(
    (row) => ({
      date: row.date.toISOString().slice(0, 10),
      name: row.name,
      country: row.country,
      region: row.region ?? null,
      holidayType: row.holidayType,
      isNationwide: row.isNationwide,
    }),
  );
}

/** `MAX(date) GROUP BY country, region` over the rows, as the coverage query reads it. */
function coverageOf(rows: SchoolRowLike[]): SchoolCoverage {
  const through = new Map<
    string,
    { country: string; region: string | null; through: string }
  >();
  for (const row of rows) {
    const key = `${row.country}|${row.region ?? ""}`;
    const seen = through.get(key);
    if (!seen || seen.through < row.date) {
      through.set(key, {
        country: row.country,
        region: row.region,
        through: row.date,
      });
    }
  }
  return buildSchoolCoverage([...through.values()]);
}

/** `/school-breaks` for the countries, from OpenHolidays and the curated sets. */
async function schoolBreaksFor(
  countries: string[] | null,
  from: string,
  to: string,
  today: string,
): Promise<TripSchoolBreaks> {
  const asked = countries ?? DEFAULT_SCHOOL_BREAK_COUNTRIES;
  const rows = new Map<string, SchoolRowLike[]>();
  for (const country of asked)
    rows.set(country, await schoolRows(country, today));
  const coverage = coverageOf([...rows.values()].flat());
  const chosen = countries ?? defaultSchoolBreakCountries(coverage, today);
  return schoolBreakRegions(chosen, rows, coverage, from, to);
}

/** The reference park's climate normals, off its calendar days marked `climate_normal`. */
async function climateOf(
  region: RegionConfig,
  from: string,
  to: string,
): Promise<ClimateNormals | null> {
  const config = region.parks.find((p) => p.slug === CLIMATE_REFERENCE_PARK);
  if (!config) return null;
  const ref = referenceRange(from, to);
  const calendar = await parkCalendar(parkBase(config), ref.from, ref.to).catch(
    () => null,
  );
  if (!calendar) return null;
  const normals: ClimateNormals = {};
  for (const day of calendar.days as Array<{
    date: string;
    weather?: {
      basis?: string;
      tempMax?: number;
      tempMin?: number;
      precipitationMm?: number;
      snowMm?: number;
      windMax?: number;
      icon?: number;
    };
  }>) {
    const w = day.weather;
    if (w?.basis !== "climate_normal") continue;
    normals[day.date.slice(5, 10)] = {
      temperatureMax: w.tempMax ?? null,
      temperatureMin: w.tempMin ?? null,
      precipitationSum: w.precipitationMm ?? null,
      rainSum: null,
      snowfallSum: w.snowMm ?? null,
      windSpeedMax: w.windMax ?? null,
      weatherCode: w.icon ?? null,
    };
  }
  return Object.keys(normals).length > 0 ? normals : null;
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

/** The region with its ticket catalog: the seed, the shim having no table. */
function regionOr404(slug: string): RegionConfig {
  const region = findRegion(slug);
  if (!region) {
    throw new UpstreamError(
      404,
      JSON.stringify({ statusCode: 404, message: `No trip region "${slug}"` }),
    );
  }
  return withCatalog(region, seedCatalog(region));
}

async function readBody(req: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks);
}

/** The routes the shim answers itself; null for everything it passes on. */
async function route(req: IncomingMessage, url: URL): Promise<unknown | null> {
  const trip =
    /^\/v1\/trip-regions\/([^/]+)(?:\/(calendar|best-days|plan|when|school-breaks|tickets))?\/?$/.exec(
      url.pathname,
    );
  if (trip && req.method === "GET") {
    const region = regionOr404(trip[1]);
    const today = getCurrentDateInTimezone(region.timezone);
    switch (trip[2]) {
      case undefined:
        return describeRegion(region, await regionParks(region));
      case "tickets":
        return ticketCatalogResponse(region);
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
      case "when": {
        const q = await query(TripWhenQueryDto, url);
        const { from, to } = whenRange(q.from, q.to, today);
        const within = q.within
          ? resolveWithin(
              q.within,
              await schoolBreaksFor([countryOfCode(q.within)], from, to, today),
            )
          : undefined;
        const parks = await regionParks(region);
        const normalized = normalizeWhenQuery(
          region,
          parks.map((p) => p.config.slug),
          q,
          today,
          within,
        );
        const result = findTripDates({
          region,
          calendar: await regionCalendar(region, from, to),
          query: normalized,
          climate: await climateOf(region, from, to),
          climatePark: CLIMATE_REFERENCE_PARK,
          clock: () => performance.now(),
        });
        return {
          ...result,
          meta: { ...result.meta, computedAt: new Date().toISOString() },
        };
      }
      case "school-breaks": {
        const q = await query(TripSchoolBreaksQueryDto, url);
        const { from, to } = whenRange(q.from, q.to, today);
        return schoolBreaksFor(
          q.countries
            ? [...new Set(q.countries.toUpperCase().split(","))]
            : null,
          from,
          to,
          today,
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
        // The evenings' night shows, as `/plan/day` lists them.
        const shows: EveningShows = new Map();
        const configs = new Map(parks.map((p) => [p.config.slug, p.config]));
        const wanted = normalized.evenings
          ? eveningShowsWanted(
              region,
              calendar,
              eveningTriggers(
                region,
                calendar,
                optimized.plan.days,
                normalized,
              ),
            )
          : [];
        await Promise.all(
          wanted.map(async ({ park, date }) => {
            const config = configs.get(park);
            if (!config) return;
            const day = await upstreamJson<{ shows?: EveningShow[] }>(
              `${parkBase(config)}/plan/day?date=${date}`,
            ).catch(() => null);
            shows.set(showsKey(park, date), day?.shows ?? []);
          }),
        );
        return plan.planResponse(region, normalized, calendar, optimized, {
          computedAt: new Date().toISOString(),
          today,
          leadMae,
          shows,
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
            JSON.stringify({
              statusCode: 500,
              message: "Internal server error",
            }),
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
