import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readdirSync,
  renameSync,
  statSync,
  unlinkSync,
} from "fs";
import { join } from "path";

const MAX_LOG_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB per daily file
const MAX_ROTATED_FILES = 3; // .log.1/.log.2/.log.3 within a single day
const RETENTION_DAYS = 7;

/** Returns today's date string in UTC: "2026-04-06" */
function todayUtc(): string {
  return new Date().toISOString().slice(0, 10);
}

/**
 * Deletes daily log files older than RETENTION_DAYS.
 * Matches files like: <filename>.2026-04-01.log and <filename>.2026-04-01.log.1
 */
function purgeOldLogs(logsDir: string, filename: string): void {
  try {
    const cutoff = Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000;
    const prefix = `${filename}.`;
    readdirSync(logsDir)
      .filter((f) => f.startsWith(prefix))
      .forEach((f) => {
        const full = join(logsDir, f);
        try {
          if (statSync(full).mtimeMs < cutoff) unlinkSync(full);
        } catch {
          // ignore individual file errors
        }
      });
  } catch {
    // non-fatal
  }
}

/**
 * Rotates today's log file if it exceeds MAX_LOG_SIZE_BYTES.
 * Keeps up to MAX_ROTATED_FILES copies within the same day: .log.1, .log.2, .log.3
 */
function rotateIfNeeded(filepath: string): void {
  try {
    if (!existsSync(filepath)) return;
    if (statSync(filepath).size < MAX_LOG_SIZE_BYTES) return;

    for (let i = MAX_ROTATED_FILES - 1; i >= 1; i--) {
      const src = `${filepath}.${i}`;
      const dst = `${filepath}.${i + 1}`;
      if (existsSync(src)) {
        if (i === MAX_ROTATED_FILES - 1 && existsSync(dst)) unlinkSync(dst);
        renameSync(src, dst);
      }
    }
    renameSync(filepath, `${filepath}.1`);
  } catch {
    // non-fatal
  }
}

/**
 * Simple file logger for critical errors that should not be missed in console logs.
 *
 * Writes to date-stamped daily files: <filename>.2026-04-06.log
 * - Files rotate at 10 MB within a day (up to 3 size-rotated copies)
 * - Files older than 7 days are automatically deleted
 *
 * Usage:
 * ```ts
 * logToFile('external-api-errors', { source: 'QueueTimesClient', error: 'fetch failed' });
 * ```
 *
 * Read today's log:
 * ```bash
 * tail -f /data/parkfan/logs/slow-queries.$(date +%Y-%m-%d).log
 * ```
 */
export function logToFile(filename: string, data: Record<string, any>): void {
  const logsDir = join(process.cwd(), "logs");

  if (!existsSync(logsDir)) {
    mkdirSync(logsDir, { recursive: true });
  }

  const filepath = join(logsDir, `${filename}.${todayUtc()}.log`);

  rotateIfNeeded(filepath);
  purgeOldLogs(logsDir, filename);

  const logLine = JSON.stringify({ timestamp: new Date().toISOString(), ...data }) + "\n";

  try {
    appendFileSync(filepath, logLine, "utf8");
  } catch (error) {
    console.error(`Failed to write to ${filepath}:`, error);
    console.error("Original log entry:", { ...data });
  }
}

/**
 * Log external API errors with enhanced details
 *
 * Note: Timeout errors (ETIMEDOUT, ECONNABORTED) are NOT logged to avoid log spam.
 * These are transient network errors that are common and expected.
 */
export function logExternalApiError(
  source: string,
  operation: string,
  error: any,
  context?: Record<string, any>,
): void {
  // Skip logging timeout errors (too noisy, transient network issues)
  const isTimeoutError =
    error?.code === "ETIMEDOUT" ||
    error?.code === "ECONNABORTED" ||
    error?.message?.toLowerCase().includes("timeout");

  // Also check AggregateError sub-errors for timeouts
  const hasOnlyTimeouts =
    error?.name === "AggregateError" &&
    Array.isArray(error.errors) &&
    error.errors.every(
      (e: any) =>
        e?.code === "ETIMEDOUT" ||
        e?.code === "ECONNABORTED" ||
        e?.message?.toLowerCase().includes("timeout"),
    );

  if (isTimeoutError || hasOnlyTimeouts) {
    return; // Don't log timeouts
  }

  const errorDetails: Record<string, any> = {
    source,
    operation,
    errorMessage: error?.message || String(error),
    errorName: error?.name,
    context,
  };

  // For AggregateError, capture all underlying errors
  if (error?.name === "AggregateError" && Array.isArray(error.errors)) {
    errorDetails.aggregateErrors = error.errors.map((err: any) => ({
      message: err?.message || String(err),
      name: err?.name,
      code: err?.code,
      syscall: err?.syscall,
      errno: err?.errno,
      address: err?.address,
      port: err?.port,
      stack: err?.stack,
    }));
  }

  // For TypeError: fetch failed, capture the cause
  if (error instanceof TypeError && error.message === "fetch failed") {
    const errorWithCause = error as any;
    if (errorWithCause.cause) {
      errorDetails.cause = errorWithCause.cause;
      errorDetails.causeString = JSON.stringify(
        errorWithCause.cause,
        Object.getOwnPropertyNames(errorWithCause.cause),
      );
    }
  }

  // For Axios errors, capture HTTP details
  if (error?.response) {
    errorDetails.httpStatus = error.response.status;
    errorDetails.httpStatusText = error.response.statusText;
  }

  // Capture error code if available (ECONNREFUSED, ETIMEDOUT, etc.)
  if (error?.code) {
    errorDetails.errorCode = error.code;
  }

  // Capture system call info (connect, getaddrinfo, etc.)
  if (error?.syscall) {
    errorDetails.syscall = error.syscall;
  }

  // Capture network error details
  if (error?.errno) {
    errorDetails.errno = error.errno;
  }
  if (error?.address) {
    errorDetails.address = error.address;
  }
  if (error?.port) {
    errorDetails.port = error.port;
  }

  // Capture full stack trace
  if (error?.stack) {
    errorDetails.stack = error.stack;

    // Also extract just the app-relevant stack (without node_modules)
    const appStackLines = error.stack.split("\n").filter((line: string) => {
      const hasAppPath = line.includes("/app/dist/src/");
      const hasNodeModules = line.includes("node_modules");
      return hasAppPath && !hasNodeModules;
    });

    if (appStackLines.length > 0) {
      errorDetails.appStack = appStackLines.join("\n");
    }
  }

  logToFile("external-api-errors", errorDetails);
}

/**
 * Log BullMQ job failures
 */
export function logJobFailure(
  jobName: string,
  queueName: string,
  error: any,
  jobData?: Record<string, any>,
): void {
  const errorDetails: Record<string, any> = {
    jobName,
    queueName,
    errorMessage: error?.message || String(error),
    errorName: error?.name,
    errorCode: error?.code,
    jobData,
  };

  // Capture full stack trace
  if (error?.stack) {
    errorDetails.stack = error.stack;

    // Extract app-relevant stack (without node_modules)
    const appStackLines = error.stack.split("\n").filter((line: string) => {
      const hasAppPath = line.includes("/app/dist/src/");
      const hasNodeModules = line.includes("node_modules");
      return hasAppPath && !hasNodeModules;
    });

    if (appStackLines.length > 0) {
      errorDetails.appStack = appStackLines.join("\n");
    }
  }

  logToFile("job-failures", errorDetails);
}

/**
 * Log ML service errors
 */
export function logMLServiceError(
  operation: string,
  error: any,
  context?: Record<string, any>,
): void {
  const errorDetails: Record<string, any> = {
    service: "ml-service",
    operation,
    errorMessage: error?.message || String(error),
    errorName: error?.name,
    errorCode: error?.code,
    context,
  };

  // Capture HTTP error details if available (axios errors)
  if (error?.response) {
    errorDetails.httpStatus = error.response.status;
    errorDetails.httpStatusText = error.response.statusText;
    errorDetails.responseData = error.response.data;
  }

  // Capture network error details
  if (error?.syscall) {
    errorDetails.syscall = error.syscall;
  }
  if (error?.address) {
    errorDetails.address = error.address;
  }

  // Capture full stack trace
  if (error?.stack) {
    errorDetails.stack = error.stack;

    // Extract app-relevant stack (without node_modules)
    const appStackLines = error.stack.split("\n").filter((line: string) => {
      const hasAppPath = line.includes("/app/dist/src/");
      const hasNodeModules = line.includes("node_modules");
      return hasAppPath && !hasNodeModules;
    });

    if (appStackLines.length > 0) {
      errorDetails.appStack = appStackLines.join("\n");
    }
  }

  logToFile("ml-service-errors", errorDetails);
}

/**
 * Log infrastructure errors (DB, Redis, etc.)
 */
export function logInfrastructureError(
  component: "database" | "redis" | "cache" | "queue",
  operation: string,
  error: any,
  context?: Record<string, any>,
): void {
  const errorDetails: Record<string, any> = {
    component,
    operation,
    errorMessage: error?.message || String(error),
    errorName: error?.name,
    context,
  };

  if (error?.stack) {
    errorDetails.stack = error.stack;
  }

  logToFile("infrastructure-errors", errorDetails);
}

/**
 * Log API rate limit blocks
 */
export function logRateLimitBlock(
  apiName: string,
  blockDurationMinutes: number,
  reason?: string,
  context?: Record<string, any>,
): void {
  const logDetails: Record<string, any> = {
    apiName,
    blockDurationMinutes,
    reason,
    context,
  };

  logToFile("rate-limit-blocks", logDetails);
}
