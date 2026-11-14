import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { addIsoDays, isoDaysBetween } from "../../common/utils/date.util";
import { utcDay } from "../../common/utils/reader-hash.util";
import { TripRequestMetric } from "./trip-request-metric.entity";
import {
  answerFactsOf,
  errorOf,
  metricRowOf,
  regionOf,
  type TripAnswerFacts,
  type TripErrorCode,
  type TripMetricCaller,
  type TripMetricRoute,
  type TripRequestMetricRow,
} from "./trip-request-facts";
import { withTripTrace, type TripRequestTrace } from "./trip-request-trace";

/** One measured request: its route, region slug (as asked) and caller. */
export interface TripMeasure {
  route: TripMetricRoute;
  region: unknown;
  caller: TripMetricCaller;
}

/**
 * How long a row is kept: the site statistic's hourly rows live as long
 * (`HOURLY_RETENTION_DAYS`), and a window of the ops report reaches as far.
 */
export const TRIP_METRICS_RETENTION_DAYS = 90;

/** How often the buffer is written: a minute of answers per insert. */
export const TRIP_METRICS_FLUSH_MS = 60_000;

/** Rows a process holds unwritten at most; past it a row is dropped and counted. */
export const TRIP_METRICS_MAX_BUFFERED = 5000;

/** Rows per INSERT and per DELETE of the nightly prune. */
const INSERT_CHUNK = 500;
const PRUNE_BATCH = 10_000;

/** What the ops report reads: a window of UTC days, narrowed or not. */
export interface TripOpsQuery {
  from: string;
  to: string;
  region?: string;
  route?: string;
  /** `site` or `parky` (`TRIP_METRIC_CALLERS`). */
  caller?: string;
  /** Rows of `slowest`. */
  limit: number;
  series: boolean;
}

/** Latency in milliseconds, of the answers it says. */
export interface LatencyFigures {
  count: number;
  p50: number | null;
  p90: number | null;
  p99: number | null;
  max: number | null;
}

/** One region and route over the window. */
export interface TripOpsRouteRow {
  region: string;
  route: string;
  /** `site`, the route itself, or `parky`, his trip tools. */
  caller: string;
  requests: number;
  answered: number;
  errors: number;
  errorRate: number | null;
  /** Every 200, cache hits included. */
  latencyMs: LatencyFigures;
  /** The 200s whose answer was built: the optimizer's and the calendar's time. */
  builtLatencyMs: LatencyFigures;
  cache: { hit: number; joined: number; miss: number; hitRate: number | null };
  /** The calendar under the built answers. */
  calendarCache: { hit: number; joined: number; miss: number };
  /** Of the answers with a search (`/plan`, `/when`), the share proven. */
  exactShare: number | null;
  /** Of the 200s, the share that loosened a rule and that left a visit out. */
  relaxedShare: number | null;
  unplacedShare: number | null;
  /** Of the 200s, the share with a hopper and with owned tickets. */
  hopperShare: number | null;
  ownedTicketsShare: number | null;
  /** Of the 200s that say, the share that left a chosen park out (`parksLeftOut`). */
  leftOutShare: number | null;
  /** A region with bases: of its 200s, the share that suggested other stays. */
  staySuggestedShare: number | null;
  avgTripDays: number | null;
  avgParks: number | null;
}

export interface TripOpsShapeRow {
  route: string;
  tripDays: number | null;
  parks: number | null;
  bases: number | null;
  requests: number;
  p50: number | null;
  p90: number | null;
  max: number | null;
}

export interface TripOpsReport {
  from: string;
  to: string;
  days: number;
  region: string | null;
  route: string | null;
  caller: string | null;
  totals: { requests: number; answered: number; errors: number };
  routes: TripOpsRouteRow[];
  relaxed: Array<{
    route: string;
    code: string;
    requests: number;
    share: number | null;
  }>;
  unplaced: Array<{
    route: string;
    reason: string;
    visits: number;
    requests: number;
  }>;
  errors: Array<{
    route: string;
    status: number;
    code: string | null;
    count: number;
  }>;
  ticketKinds: Array<{
    route: string;
    kind: string;
    tickets: number;
    requests: number;
  }>;
  /** A region with bases: the 200s by the visitor's move buffer and by the ends they fixed. */
  stays: Array<{
    route: string;
    dim: "move_buffer" | "gateways";
    value: string;
    requests: number;
  }>;
  /** Built answers by their whole shape, slowest p90 first. */
  slowest: TripOpsShapeRow[];
  /** Built answers by one side of the shape each, in its order. */
  byLength: TripOpsShapeRow[];
  byParks: TripOpsShapeRow[];
  byBases: TripOpsShapeRow[];
  series?: Array<{
    day: string;
    route: string;
    requests: number;
    errors: number;
    hitRate: number | null;
    builtP90: number | null;
  }>;
  /** This process's writer: rows waiting for the next write, rows dropped since it started. */
  writer: { buffered: number; dropped: number };
}

type Num = number | string | null;
const num = (value: Num): number => (value == null ? 0 : Number(value));
const ms = (value: Num): number | null =>
  value == null ? null : Math.round(Number(value));
const share = (part: number, whole: number): number | null =>
  whole > 0 ? Math.round((part / whole) * 1000) / 1000 : null;
const avg = (value: Num): number | null =>
  value == null ? null : Math.round(Number(value) * 10) / 10;

/** The percentiles of `latency_ms` over the rows `filter` keeps, as columns named `prefix_*`. */
function latencyColumns(prefix: string, filter: string): string {
  const of = (p: number) =>
    `percentile_cont(${p}) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE ${filter})`;
  return [
    `count(*) FILTER (WHERE ${filter}) AS ${prefix}_count`,
    `${of(0.5)} AS ${prefix}_p50`,
    `${of(0.9)} AS ${prefix}_p90`,
    `${of(0.99)} AS ${prefix}_p99`,
    `max(latency_ms) FILTER (WHERE ${filter}) AS ${prefix}_max`,
  ].join(",\n");
}

const latencyOf = (row: Record<string, Num>, prefix: string) => ({
  count: num(row[`${prefix}_count`]),
  p50: ms(row[`${prefix}_p50`]),
  p90: ms(row[`${prefix}_p90`]),
  p99: ms(row[`${prefix}_p99`]),
  max: ms(row[`${prefix}_max`]),
});

const ANSWERED = "status = 200";
const BUILT = "status = 200 AND cache = 'miss'";

/**
 * The trip-region routes' operational metrics: every answer of `/plan`,
 * `/when` and `/best-days` becomes a row (`TripRequestMetricsInterceptor`),
 * held in this process and written once a minute, kept
 * {@link TRIP_METRICS_RETENTION_DAYS} days, and read by the MCP tool
 * `trip_planner_ops`. A metric never slows or fails an answer: a write that
 * fails keeps its rows for the next one, a full buffer drops and counts.
 * docs/frontend/trip-regions.md#what-the-server-records.
 */
@Injectable()
export class TripRequestMetricsService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(TripRequestMetricsService.name);
  private buffer: TripRequestMetricRow[] = [];
  private dropped = 0;
  private droppedLogged = 0;
  private timer?: NodeJS.Timeout;
  private writing: Promise<number> | null = null;

  constructor(
    @InjectRepository(TripRequestMetric)
    private readonly repository: Repository<TripRequestMetric>,
  ) {}

  onModuleInit(): void {
    this.timer = setInterval(() => void this.flush(), TRIP_METRICS_FLUSH_MS);
    this.timer.unref();
  }

  async onModuleDestroy(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.flush();
  }

  /** Takes one answer's row; it reaches Postgres with the next write. */
  record(row: TripRequestMetricRow): void {
    if (this.buffer.length >= TRIP_METRICS_MAX_BUFFERED) {
      this.dropped += 1;
      return;
    }
    this.buffer.push(row);
  }

  /**
   * Records one answer or failure of a measured request. Never throws: a row
   * that cannot be built is logged and left out, and the answer goes on.
   */
  recordAnswer(
    measure: TripMeasure,
    trace: TripRequestTrace,
    startedAt: number,
    outcome:
      | { status: 200; facts?: TripAnswerFacts }
      | { status: number; errorCode: TripErrorCode },
  ): void {
    try {
      this.record(
        metricRowOf({
          route: measure.route,
          caller: measure.caller,
          region: regionOf(measure.region),
          at: new Date(),
          latencyMs: performance.now() - startedAt,
          trace,
          status: outcome.status,
          ...("errorCode" in outcome
            ? { errorCode: outcome.errorCode }
            : { facts: outcome.facts }),
        }),
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Trip request metric for ${measure.route} not recorded: ${message}`,
      );
    }
  }

  /**
   * Runs a call to a trip route's service as a measured request, the way the
   * interceptor runs the route: a trace around it, one row for its answer or
   * its error. What Parky's trip tools call (`caller: parky`); the answer and
   * the error pass through unchanged.
   */
  async measure<T>(measure: TripMeasure, run: () => Promise<T>): Promise<T> {
    const trace: TripRequestTrace = {};
    const startedAt = performance.now();
    try {
      const body = await withTripTrace(trace, run);
      this.recordAnswer(measure, trace, startedAt, {
        status: 200,
        facts: answerFactsOf(measure.route, body),
      });
      return body;
    } catch (error) {
      const { status, code } = errorOf(error);
      this.recordAnswer(measure, trace, startedAt, { status, errorCode: code });
      throw error;
    }
  }

  /** Rows waiting, and rows dropped since the process started. */
  writerState(): { buffered: number; dropped: number } {
    return { buffered: this.buffer.length, dropped: this.dropped };
  }

  /**
   * Writes what the buffer holds, a chunk per INSERT; returns the rows
   * written. Rows a failed INSERT did not take go back to the front, as far
   * as the cap leaves room. One write at a time.
   */
  async flush(): Promise<number> {
    if (this.writing) return this.writing;
    this.writing = this.write().finally(() => {
      this.writing = null;
    });
    return this.writing;
  }

  private async write(): Promise<number> {
    const rows = this.buffer.splice(0);
    let written = 0;
    try {
      for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
        await this.repository.insert(rows.slice(i, i + INSERT_CHUNK));
        written = Math.min(rows.length, i + INSERT_CHUNK);
      }
    } catch (error) {
      const left = rows.slice(written);
      const room = Math.max(0, TRIP_METRICS_MAX_BUFFERED - this.buffer.length);
      this.buffer = [...left.slice(0, room), ...this.buffer];
      this.dropped += Math.max(0, left.length - room);
      const message = error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `Trip request metrics: ${left.length} row(s) not written, kept for the next write: ${message}`,
      );
    }
    if (this.dropped > this.droppedLogged) {
      this.logger.warn(
        `Trip request metrics: ${this.dropped - this.droppedLogged} row(s) dropped, the buffer was full`,
      );
      this.droppedLogged = this.dropped;
    }
    return written;
  }

  /** Deletes the rows past the retention, in batches; returns how many went. */
  async prune(now = new Date()): Promise<number> {
    const cutoff = `${addIsoDays(utcDay(now), -TRIP_METRICS_RETENTION_DAYS)}T00:00:00.000Z`;
    let removed = 0;
    for (;;) {
      const [{ n }]: Array<{ n: number }> = await this.repository.query(
        `WITH gone AS (
           DELETE FROM trip_request_metrics
            WHERE id IN (SELECT id FROM trip_request_metrics
                          WHERE bucket < $1::timestamptz
                          LIMIT ${PRUNE_BATCH})
           RETURNING 1)
         SELECT count(*)::int AS n FROM gone`,
        [cutoff],
      );
      removed += n;
      if (n < PRUNE_BATCH) return removed;
    }
  }

  /**
   * The ops report over a window of UTC days: per region, route and caller the
   * requests, errors, latency percentiles (all answers and the built ones),
   * the caches, the share proven, loosened and incomplete; the codes behind
   * them; the slowest shapes of a built answer; and a series on request.
   * Seven statements over the bucket index, an eighth for the series.
   */
  async ops(query: TripOpsQuery): Promise<TripOpsReport> {
    const params: unknown[] = [
      `${query.from}T00:00:00.000Z`,
      `${addIsoDays(query.to, 1)}T00:00:00.000Z`,
    ];
    const conditions = [
      "bucket >= $1::timestamptz",
      "bucket < $2::timestamptz",
    ];
    if (query.region) {
      params.push(query.region);
      conditions.push(`region = $${params.length}`);
    }
    if (query.route) {
      params.push(query.route);
      conditions.push(`route = $${params.length}`);
    }
    if (query.caller) {
      params.push(query.caller);
      conditions.push(`caller = $${params.length}`);
    }
    const where = conditions.join(" AND ");
    const read = <T>(sql: string) =>
      this.repository.query(sql, params) as Promise<T[]>;

    const [
      routeRows,
      relaxedRows,
      unplacedRows,
      errorRows,
      ticketRows,
      shapeRows,
      stayRows,
    ] = await Promise.all([
      read<
        Record<string, Num> & { region: string; route: string; caller: string }
      >(
        `SELECT region, route, caller,
                  count(*) AS requests,
                  count(*) FILTER (WHERE ${ANSWERED}) AS answered,
                  ${latencyColumns("all", ANSWERED)},
                  ${latencyColumns("built", BUILT)},
                  count(*) FILTER (WHERE ${ANSWERED} AND cache = 'hit') AS hit,
                  count(*) FILTER (WHERE ${ANSWERED} AND cache = 'joined') AS joined,
                  count(*) FILTER (WHERE ${ANSWERED} AND cache = 'miss') AS miss,
                  count(*) FILTER (WHERE ${BUILT} AND calendar_cache = 'hit') AS calendar_hit,
                  count(*) FILTER (WHERE ${BUILT} AND calendar_cache = 'joined') AS calendar_joined,
                  count(*) FILTER (WHERE ${BUILT} AND calendar_cache = 'miss') AS calendar_miss,
                  count(*) FILTER (WHERE ${ANSWERED} AND exact IS NOT NULL) AS searched,
                  count(*) FILTER (WHERE ${ANSWERED} AND exact) AS exact,
                  count(*) FILTER (WHERE ${ANSWERED} AND cardinality(relaxed) > 0) AS relaxed,
                  count(*) FILTER (WHERE ${ANSWERED} AND cardinality(unplaced_reasons) > 0) AS unplaced,
                  count(*) FILTER (WHERE ${ANSWERED} AND hopper) AS hopper,
                  count(*) FILTER (WHERE ${ANSWERED} AND cardinality(ticket_kinds) > 0) AS owned,
                  count(*) FILTER (WHERE ${ANSWERED} AND left_out IS NOT NULL) AS left_out_known,
                  count(*) FILTER (WHERE ${ANSWERED} AND left_out > 0) AS left_out,
                  count(*) FILTER (WHERE ${ANSWERED} AND stay_suggested IS NOT NULL) AS stay_known,
                  count(*) FILTER (WHERE ${ANSWERED} AND stay_suggested) AS stay_suggested,
                  avg(trip_days) FILTER (WHERE ${ANSWERED}) AS avg_trip_days,
                  avg(parks) FILTER (WHERE ${ANSWERED}) AS avg_parks
             FROM trip_request_metrics
            WHERE ${where}
            GROUP BY region, route, caller
            ORDER BY count(*) DESC, region, route, caller`,
      ),
      read<{ route: string; code: string; requests: Num }>(
        `SELECT route, code, count(*) AS requests
             FROM trip_request_metrics, unnest(relaxed) AS code
            WHERE ${where} AND ${ANSWERED}
            GROUP BY route, code
            ORDER BY count(*) DESC, route, code`,
      ),
      read<{ route: string; reason: string; visits: Num; requests: Num }>(
        `SELECT route, reason, count(*) AS visits, count(DISTINCT id) AS requests
             FROM trip_request_metrics, unnest(unplaced_reasons) AS reason
            WHERE ${where} AND ${ANSWERED}
            GROUP BY route, reason
            ORDER BY count(*) DESC, route, reason`,
      ),
      read<{
        route: string;
        status: Num;
        error_code: string | null;
        count: Num;
      }>(
        `SELECT route, status, error_code, count(*) AS count
             FROM trip_request_metrics
            WHERE ${where} AND status <> 200
            GROUP BY route, status, error_code
            ORDER BY count(*) DESC, route, status, error_code`,
      ),
      read<{ route: string; kind: string; tickets: Num; requests: Num }>(
        `SELECT route, kind, count(*) AS tickets, count(DISTINCT id) AS requests
             FROM trip_request_metrics, unnest(ticket_kinds) AS kind
            WHERE ${where} AND ${ANSWERED}
            GROUP BY route, kind
            ORDER BY count(*) DESC, route, kind`,
      ),
      read<{
        route: string;
        trip_days: Num;
        parks: Num;
        bases: Num;
        sides: Num;
        requests: Num;
        p50: Num;
        p90: Num;
        max: Num;
      }>(
        `SELECT route, trip_days, parks, bases,
                  GROUPING(trip_days, parks, bases) AS sides,
                  count(*) AS requests,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) AS p50,
                  percentile_cont(0.9) WITHIN GROUP (ORDER BY latency_ms) AS p90,
                  max(latency_ms) AS max
             FROM trip_request_metrics
            WHERE ${where} AND ${BUILT}
            GROUP BY GROUPING SETS (
              (route, trip_days, parks, bases),
              (route, trip_days), (route, parks), (route, bases))`,
      ),
      read<{ route: string; dim: string; value: string; requests: Num }>(
        `SELECT route, 'move_buffer' AS dim, move_buffer::text AS value, count(*) AS requests
             FROM trip_request_metrics
            WHERE ${where} AND ${ANSWERED} AND move_buffer IS NOT NULL
            GROUP BY route, move_buffer
           UNION ALL
           SELECT route, 'gateways', gateways, count(*)
             FROM trip_request_metrics
            WHERE ${where} AND ${ANSWERED} AND gateways IS NOT NULL
            GROUP BY route, gateways
            ORDER BY 1, 2, 4 DESC, 3`,
      ),
    ]);

    const routes: TripOpsRouteRow[] = routeRows.map((row) => {
      const requests = num(row.requests);
      const answered = num(row.answered);
      const errors = requests - answered;
      const hit = num(row.hit);
      const joined = num(row.joined);
      const miss = num(row.miss);
      return {
        region: row.region,
        route: row.route,
        caller: row.caller,
        requests,
        answered,
        errors,
        errorRate: share(errors, requests),
        latencyMs: latencyOf(row, "all"),
        builtLatencyMs: latencyOf(row, "built"),
        cache: { hit, joined, miss, hitRate: share(hit, hit + joined + miss) },
        calendarCache: {
          hit: num(row.calendar_hit),
          joined: num(row.calendar_joined),
          miss: num(row.calendar_miss),
        },
        exactShare: share(num(row.exact), num(row.searched)),
        relaxedShare: share(num(row.relaxed), answered),
        unplacedShare: share(num(row.unplaced), answered),
        hopperShare: share(num(row.hopper), answered),
        ownedTicketsShare: share(num(row.owned), answered),
        leftOutShare: share(num(row.left_out), num(row.left_out_known)),
        staySuggestedShare: share(num(row.stay_suggested), num(row.stay_known)),
        avgTripDays: avg(row.avg_trip_days),
        avgParks: avg(row.avg_parks),
      };
    });
    const answeredBy = new Map<string, number>();
    for (const row of routes) {
      answeredBy.set(
        row.route,
        (answeredBy.get(row.route) ?? 0) + row.answered,
      );
    }

    // GROUPING's bits are trip_days, parks, bases: 0 the whole shape, 3 the
    // length alone, 5 the parks alone, 6 the bases alone.
    const shapes = (sides: number) =>
      shapeRows
        .filter((row) => num(row.sides) === sides)
        .map((row) => ({
          route: row.route,
          tripDays: row.trip_days == null ? null : num(row.trip_days),
          parks: row.parks == null ? null : num(row.parks),
          bases: row.bases == null ? null : num(row.bases),
          requests: num(row.requests),
          p50: ms(row.p50),
          p90: ms(row.p90),
          max: ms(row.max),
        }));
    const inOrder =
      (key: "tripDays" | "parks" | "bases") =>
      (a: TripOpsShapeRow, b: TripOpsShapeRow) =>
        a.route.localeCompare(b.route) || (a[key] ?? -1) - (b[key] ?? -1);

    const totals = routes.reduce(
      (sum, row) => ({
        requests: sum.requests + row.requests,
        answered: sum.answered + row.answered,
        errors: sum.errors + row.errors,
      }),
      { requests: 0, answered: 0, errors: 0 },
    );

    return {
      from: query.from,
      to: query.to,
      days: isoDaysBetween(query.from, query.to) + 1,
      region: query.region ?? null,
      route: query.route ?? null,
      caller: query.caller ?? null,
      totals,
      routes,
      relaxed: relaxedRows.map((row) => ({
        route: row.route,
        code: row.code,
        requests: num(row.requests),
        share: share(num(row.requests), answeredBy.get(row.route) ?? 0),
      })),
      unplaced: unplacedRows.map((row) => ({
        route: row.route,
        reason: row.reason,
        visits: num(row.visits),
        requests: num(row.requests),
      })),
      errors: errorRows.map((row) => ({
        route: row.route,
        status: num(row.status),
        code: row.error_code,
        count: num(row.count),
      })),
      ticketKinds: ticketRows.map((row) => ({
        route: row.route,
        kind: row.kind,
        tickets: num(row.tickets),
        requests: num(row.requests),
      })),
      stays: stayRows.map((row) => ({
        route: row.route,
        dim: row.dim as "move_buffer" | "gateways",
        value: row.value,
        requests: num(row.requests),
      })),
      slowest: shapes(0)
        .sort(
          (a, b) =>
            (b.p90 ?? 0) - (a.p90 ?? 0) ||
            b.requests - a.requests ||
            a.route.localeCompare(b.route),
        )
        .slice(0, query.limit),
      byLength: shapes(3).sort(inOrder("tripDays")),
      byParks: shapes(5).sort(inOrder("parks")),
      byBases: shapes(6).sort(inOrder("bases")),
      ...(query.series && { series: await this.series(where, params) }),
      writer: this.writerState(),
    };
  }

  /** Per UTC day and route: requests, errors, the hit rate and the built p90. */
  private async series(where: string, params: unknown[]) {
    const rows: Array<{
      day: string;
      route: string;
      requests: Num;
      answered: Num;
      hit: Num;
      cached: Num;
      built_p90: Num;
    }> = await this.repository.query(
      `SELECT to_char(bucket AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS day, route,
              count(*) AS requests,
              count(*) FILTER (WHERE ${ANSWERED}) AS answered,
              count(*) FILTER (WHERE ${ANSWERED} AND cache = 'hit') AS hit,
              count(*) FILTER (WHERE ${ANSWERED} AND cache IS NOT NULL) AS cached,
              percentile_cont(0.9) WITHIN GROUP (ORDER BY latency_ms)
                FILTER (WHERE ${BUILT}) AS built_p90
         FROM trip_request_metrics
        WHERE ${where}
        GROUP BY 1, route
        ORDER BY 1, route`,
      params,
    );
    return rows.map((row) => ({
      day: row.day,
      route: row.route,
      requests: num(row.requests),
      errors: num(row.requests) - num(row.answered),
      hitRate: share(num(row.hit), num(row.cached)),
      builtP90: ms(row.built_p90),
    }));
  }
}
