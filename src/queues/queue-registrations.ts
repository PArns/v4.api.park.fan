import type { BullModuleOptions } from "@nestjs/bull";

/**
 * Bull's key prefix. Every queue keeps its jobs, `wait`, `delayed`, `active`,
 * `failed` and `repeat` under `<prefix>:<queue>:*` in the same Redis as the
 * caches, so a cache flush must never match that shape.
 */
export function bullPrefix(): string {
  return process.env.BULL_PREFIX || "parkfan";
}

/**
 * Every queue the app registers. Kept out of QueuesModule so a spec can read
 * the names without booting the module (see flush-cache-patterns.spec.ts).
 */
export const BULL_QUEUE_REGISTRATIONS: BullModuleOptions[] = [
  { name: "wait-times" },
  // The cache warmup after each wait-times sync, off the fetch's critical
  // path (PAR-822). See LiveCacheWarmupProcessor.
  { name: "live-cache-warmup" },
  { name: "park-metadata" },
  { name: "children-metadata" }, // Phase 6.2: Combined Attractions + Shows + Restaurants
  { name: "six-flags-heights" }, // Ride heights the wiki does not carry
  { name: "ride-stats" }, // Speed/height/length from Wikidata
  { name: "manual-metadata" }, // Curated data (name is historical — see CuratedDataProcessor)
  { name: "entity-mappings" }, // Phase 6.6.3: Multi-source mappings
  { name: "weather" },
  { name: "weather-warnings" },
  { name: "weather-historical" },
  { name: "holidays" },
  { name: "ml-training" },
  { name: "prediction-accuracy" },
  { name: "predictions" },
  { name: "park-enrichment" },
  { name: "analytics" },
  { name: "wartezeiten-schedule" },
  { name: "ml-monitoring" },
  { name: "stats" },
  {
    // P50 + P90 baseline calculation. calculate-attraction-baselines
    // does 113 PERCENTILE_CONT scans over 548-day queue_data — a single
    // run takes 3-6 min. Bull's default lockDuration of 30s would
    // wrongly mark the worker stalled mid-run, so bump it to 10 min
    // with proportional renew interval. Same queue handles park-level
    // and backfill jobs which are much shorter; the higher limit is
    // harmless for them.
    name: "p50-baseline",
    settings: {
      lockDuration: 600000, // 10 min
      lockRenewTime: 300000, // renew every 5 min
    },
  },
  {
    // Hourly history backfills can iterate 30+ days × 100+ parks — same
    // stall risk as p50-baseline. Yesterday-only cron is fast (<5s)
    // but the backfill path needs headroom.
    name: "attraction-hourly-history",
    settings: {
      lockDuration: 600000,
      lockRenewTime: 300000,
    },
  },
  {
    // Measured-operation verdicts: one aggregate over one day's chunks per
    // park for the nightly job, but the fill job walks months × 100+ parks —
    // same stall risk as the hourly-history backfill, same headroom.
    name: "park-day-operation",
    settings: {
      lockDuration: 600000, // 10 min
      lockRenewTime: 300000,
    },
  },
  {
    // The whole catalogue in two statements over a compressed hypertable,
    // then the profiles. Slower than the hourly-history rollup and for the
    // same reason — it reads history rather than a rollup — so it gets the
    // same headroom rather than being flagged stalled halfway through.
    name: "downtime",
    settings: {
      lockDuration: 900000, // 15 min
      lockRenewTime: 300000,
    },
  },
  { name: "geoip-update" }, // GeoLite2-City every 48h
  { name: "nf-training" }, // TFT train+forecast + TFT-vs-CatBoost scoreboard
  { name: "pcn-shadow" }, // PCN intraday shadow: train + forecast + score
  { name: "shape-shadow" }, // Shape day-curve shadow: build + forecast + score
  {
    // Rope-drop: one LATERAL query per park over hourly-history slots +
    // pure aggregation. Lighter than p50-baseline (no fresh PERCENTILE
    // scan) but still iterates all parks — give it the same headroom so a
    // batch run is never flagged stalled.
    name: "rope-drop",
    settings: {
      lockDuration: 600000, // 10 min
      lockRenewTime: 300000, // renew every 5 min
    },
  },
  {
    // Typical-waits: per-headliner aggregate reads across all parks — same
    // stall headroom as rope-drop.
    name: "typical-waits",
    settings: {
      lockDuration: 600000,
      lockRenewTime: 300000,
    },
  },
  // Push notifications: a five-minute tick over the trips somebody
  // subscribed to, plus followed shows (three `getShowtimeInstantsOnDate`
  // queries per distinct park among them — yesterday, today and tomorrow,
  // because a showtime is keyed on its operating day). No explicit lock
  // headroom — unlike rope-drop/typical-waits' long synchronous batch
  // runs, this job is all async I/O with plenty of await points for
  // Bull's lock renewal to keep up regardless of wall-clock duration;
  // revisit if that stops being true once this runs against real follow
  // counts.
  { name: "push-notifications" },
  // Stored plans: one daily sweep of the expired ones. Its own queue rather
  // than a second job on the push tick, because it is maintenance on a
  // table and has nothing to do with notifying anybody.
  { name: "trips" },
  // Showtime patterns: the nightly rebuild that lets a planned day show a
  // projected programme, since no feed publishes showtimes ahead of today.
  { name: "show-patterns" },
];
