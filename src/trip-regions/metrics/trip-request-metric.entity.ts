import { Column, Entity, Index, PrimaryGeneratedColumn } from "typeorm";

/**
 * One answer of `/plan`, `/when` or `/best-days`, as operational facts: what
 * route and region, how long, which caches, whether the search was proven,
 * what it loosened and left out, and the trip's shape in counts. No date, no
 * park or ticket name, no free text, no address, no visitor id: the hour is
 * the only time, and every string is a value of the API's own vocabulary.
 * Written in batches by `TripRequestMetricsService`, kept
 * `TRIP_METRICS_RETENTION_DAYS` and deleted nightly.
 * docs/frontend/trip-regions.md#what-the-server-records.
 *
 * The array columns have no default: the table was new when they were, and
 * every insert names them. Every column added since is nullable or has a
 * default (docs/rules/the-entities-are-the-schema.md).
 */
@Entity("trip_request_metrics")
@Index("idx_trip_request_metrics_bucket", ["bucket"])
export class TripRequestMetric {
  @PrimaryGeneratedColumn({ type: "bigint" })
  id: string;

  /** The UTC hour the answer left in. */
  @Column({ type: "timestamptz" })
  bucket: Date;

  /** The region's slug (`regions.ts`); `(unknown)` for a slug that names none. */
  @Column({ type: "varchar", length: 32 })
  region: string;

  /** `plan`, `when` or `best_days` (`TRIP_METRIC_ROUTES`). */
  @Column({ type: "varchar", length: 16 })
  route: string;

  /**
   * Who asked: `site`, the route itself, or `parky`, his trip tools calling
   * the route's service (`TRIP_METRIC_CALLERS`). Added to a table with rows,
   * so it has a default.
   */
  @Column({ type: "varchar", length: 8, default: "site" })
  caller: string;

  /** The HTTP status the answer left with. */
  @Column({ type: "smallint" })
  status: number;

  /** Why a request was refused or failed (`TRIP_ERROR_CODES`); null on a 200. */
  @Column({ name: "error_code", type: "varchar", length: 40, nullable: true })
  errorCode: string | null;

  /** Milliseconds from the route taking the request to its answer. */
  @Column({ name: "latency_ms", type: "integer" })
  latencyMs: number;

  /** The answer's own cache: `hit`, `joined`, `miss`; for `/best-days` the calendar's. */
  @Column({ type: "varchar", length: 8, nullable: true })
  cache: string | null;

  /** The region calendar a built answer read: `hit`, `joined`, `miss`. */
  @Column({
    name: "calendar_cache",
    type: "varchar",
    length: 8,
    nullable: true,
  })
  calendarCache: string | null;

  /** The search was proven (`meta.exact`, `meta.search.exact`); null without one. */
  @Column({ type: "boolean", nullable: true })
  exact: boolean | null;

  /** The rules the plan loosened (`meta.relaxed`), sorted. */
  @Column({ type: "text", array: true })
  relaxed: string[];

  /** One entry per visit the plan could not place: its `unplaced[].reason`. */
  @Column({ name: "unplaced_reasons", type: "text", array: true })
  unplacedReasons: string[];

  /** Trip length in days, arrival and departure included. */
  @Column({ name: "trip_days", type: "smallint", nullable: true })
  tripDays: number | null;

  /** Parks planned for: named, or the headliners. */
  @Column({ type: "smallint", nullable: true })
  parks: number | null;

  /** `parks=` named them; false: the headliners. */
  @Column({ name: "parks_named", type: "boolean", nullable: true })
  parksNamed: boolean | null;

  /** Park visits, second visits included. */
  @Column({ type: "smallint", nullable: true })
  visits: number | null;

  /** Pinned days (`lock=`). */
  @Column({ type: "smallint", nullable: true })
  locks: number | null;

  /** One entry per owned ticket: its product's kind (`TICKET_KINDS`). */
  @Column({ name: "ticket_kinds", type: "text", array: true })
  ticketKinds: string[];

  /** A hopper on plan first's resorts or on an owned ticket. */
  @Column({ type: "boolean", nullable: true })
  hopper: boolean | null;

  /** A region with bases: the stays of the plan. */
  @Column({ type: "smallint", nullable: true })
  bases: number | null;

  /** A region with bases: the moves between them. */
  @Column({ type: "smallint", nullable: true })
  moves: number | null;

  /** `/when` and `/best-days`: the days the search range or the window spans. */
  @Column({ name: "range_days", type: "smallint", nullable: true })
  rangeDays: number | null;

  /** `/when`: windows the search planned (`meta.search.planned`). */
  @Column({ name: "windows_planned", type: "smallint", nullable: true })
  windowsPlanned: number | null;

  /** `/when`: windows answered. */
  @Column({ type: "smallint", nullable: true })
  windows: number | null;

  /** `/when`: kept inside a school-holiday region's breaks. */
  @Column({ type: "boolean", nullable: true })
  within: boolean | null;

  /** The chosen parks the plan has no visit of (`parksLeftOut`); `/when`: its best window's. */
  @Column({ name: "left_out", type: "smallint", nullable: true })
  leftOut: number | null;

  /** A region with bases: the visitor's minutes for a hotel change (`moveBuffer`), null without. */
  @Column({ name: "move_buffer", type: "smallint", nullable: true })
  moveBuffer: number | null;

  /** A region with bases: which ends the visitor fixed, `none`, `start`, `end` or `both`. */
  @Column({ type: "varchar", length: 8, nullable: true })
  gateways: string | null;

  /** A region with bases: the answer suggested a change of stays (`meta.staySuggestion`). */
  @Column({ name: "stay_suggested", type: "boolean", nullable: true })
  staySuggested: boolean | null;
}
