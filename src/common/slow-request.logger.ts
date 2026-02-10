import * as fs from "fs";
import * as path from "path";

const SLOW_REQUEST_LOG_PATH =
  process.env.SLOW_REQUEST_LOG_PATH || "logs/slow-requests.log";

export interface SlowRequestEntry {
  ts: string;
  method: string;
  url: string;
  statusCode: number;
  responseTimeMs: number;
  ip?: string;
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
