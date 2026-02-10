import * as fs from "fs";
import * as path from "path";

const SLOW_REQUEST_LOG_PATH =
  process.env.SLOW_REQUEST_LOG_PATH || "logs/slow-requests.log";

export interface SlowRequestEntry {
  ts: string;
  method: string;
  url: string;
  /** Short label for grouping/alerting (e.g. attraction_detail, park_detail, search) */
  endpoint?: string;
  /** Query string only (e.g. "days=30") for debugging */
  query?: string;
  statusCode: number;
  responseTimeMs: number;
  ip?: string;
  /** Per-phase or per-operation timings (ms) to see where time was spent */
  breakdown?: Record<string, number>;
}

/**
 * Derive endpoint label from path for slow-request log (no query string).
 */
export function getEndpointLabel(pathname: string): string {
  const p = pathname.startsWith("/") ? pathname : `/${pathname}`;
  if (p.includes("/health")) return "health";
  if (p.includes("/search")) return "search";
  if (p.includes("/calendar")) return "calendar";
  // /v1/parks/:c/:country/:city/:parkSlug/attractions/:attractionSlug
  if (/\/v1\/parks\/[^/]+\/[^/]+\/[^/]+\/[^/]+\/attractions\/[^/]+/.test(p))
    return "attraction_detail";
  // /v1/parks/:c/:country/:city/:parkSlug (exactly 4 segments after /parks/)
  if (/\/v1\/parks\/[^/]+\/[^/]+\/[^/]+\/[^/]+$/.test(p)) return "park_detail";
  if (p.includes("/parks/")) return "parks";
  if (p.includes("/attractions/")) return "attractions";
  if (p.includes("/discovery")) return "discovery";
  if (p.includes("/admin") || p.includes("/ml")) return "admin_ml";
  const first = p.split("/").filter(Boolean)[1]; // v1, admin, ...
  return first || "other";
}

/**
 * Appends a slow-request entry to a dedicated log file (JSON Lines).
 * Fire-and-forget; does not block the request. Use this so slow requests
 * are not lost in the main log stream and can be tailed/alerted separately.
 */
export function recordSlowRequest(entry: SlowRequestEntry): void {
  const logPath = path.resolve(process.cwd(), SLOW_REQUEST_LOG_PATH);
  const dir = path.dirname(logPath);
  const line =
    JSON.stringify({
      ...entry,
      ts: entry.ts || new Date().toISOString(),
    }) + "\n";

  fs.mkdir(dir, { recursive: true }, (errDir) => {
    if (errDir) return;
    fs.appendFile(logPath, line, (err) => {
      if (err) {
        // Avoid circular logging; stderr is acceptable for rare write failures
        process.stderr.write(
          `[slow-request] Failed to write to ${logPath}: ${err.message}\n`,
        );
      }
    });
  });
}
