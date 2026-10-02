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

/**
 * The fields these loggers read off a thrown value. Anything can be thrown, so
 * every field is `unknown`; `errorFields()` gives a non-object an empty view,
 * which reads the same as the optional chaining (`error?.code`) it replaces.
 */
interface ErrorFields {
  message?: unknown;
  name?: unknown;
  code?: unknown;
  syscall?: unknown;
  errno?: unknown;
  address?: unknown;
  port?: unknown;
  stack?: unknown;
  errors?: unknown;
  cause?: unknown;
  response?: { status?: unknown; statusText?: unknown; data?: unknown };
}

function errorFields(error: unknown): ErrorFields {
  return (typeof error === "object" || typeof error === "function") &&
    error !== null
    ? (error as ErrorFields)
    : {};
}

function isTimeout(f: ErrorFields): boolean {
  return (
    f.code === "ETIMEDOUT" ||
    f.code === "ECONNABORTED" ||
    (typeof f.message === "string" &&
      f.message.toLowerCase().includes("timeout"))
  );
}

/** Stack lines from app code (`/app/dist/src/`), without node_modules. */
function appStackLines(stack: string): string[] {
  return stack.split("\n").filter((line) => {
    const hasAppPath = line.includes("/app/dist/src/");
    const hasNodeModules = line.includes("node_modules");
    return hasAppPath && !hasNodeModules;
  });
}

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
export function logToFile(
  filename: string,
  data: Record<string, unknown>,
): void {
  const logsDir = join(process.cwd(), "logs");

  if (!existsSync(logsDir)) {
    mkdirSync(logsDir, { recursive: true });
  }

  const filepath = join(logsDir, `${filename}.${todayUtc()}.log`);

  rotateIfNeeded(filepath);
  purgeOldLogs(logsDir, filename);

  const logLine =
    JSON.stringify({ timestamp: new Date().toISOString(), ...data }) + "\n";

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
  error: unknown,
  context?: Record<string, unknown>,
): void {
  const e = errorFields(error);
  const subErrors: unknown[] | undefined =
    e.name === "AggregateError" && Array.isArray(e.errors)
      ? (e.errors as unknown[])
      : undefined;

  // Skip logging timeout errors (too noisy, transient network issues)
  const isTimeoutError = isTimeout(e);

  // Also check AggregateError sub-errors for timeouts
  const hasOnlyTimeouts =
    subErrors !== undefined &&
    subErrors.every((sub) => isTimeout(errorFields(sub)));

  if (isTimeoutError || hasOnlyTimeouts) {
    return; // Don't log timeouts
  }

  const errorDetails: Record<string, unknown> = {
    source,
    operation,
    errorMessage: e.message || String(error),
    errorName: e.name,
    context,
  };

  // For AggregateError, capture all underlying errors
  if (subErrors !== undefined) {
    errorDetails.aggregateErrors = subErrors.map((err) => {
      const f = errorFields(err);
      return {
        message: f.message || String(err),
        name: f.name,
        code: f.code,
        syscall: f.syscall,
        errno: f.errno,
        address: f.address,
        port: f.port,
        stack: f.stack,
      };
    });
  }

  // For TypeError: fetch failed, capture the cause
  if (error instanceof TypeError && error.message === "fetch failed") {
    const cause = errorFields(error).cause;
    if (cause) {
      errorDetails.cause = cause;
      errorDetails.causeString = JSON.stringify(
        cause,
        Object.getOwnPropertyNames(cause),
      );
    }
  }

  // For Axios errors, capture HTTP details
  if (e.response) {
    errorDetails.httpStatus = e.response.status;
    errorDetails.httpStatusText = e.response.statusText;
  }

  // Capture error code if available (ECONNREFUSED, ETIMEDOUT, etc.)
  if (e.code) {
    errorDetails.errorCode = e.code;
  }

  // Capture system call info (connect, getaddrinfo, etc.)
  if (e.syscall) {
    errorDetails.syscall = e.syscall;
  }

  // Capture network error details
  if (e.errno) {
    errorDetails.errno = e.errno;
  }
  if (e.address) {
    errorDetails.address = e.address;
  }
  if (e.port) {
    errorDetails.port = e.port;
  }

  // Capture full stack trace
  if (e.stack) {
    errorDetails.stack = e.stack;

    // Also extract just the app-relevant stack (without node_modules)
    const appStack = typeof e.stack === "string" ? appStackLines(e.stack) : [];
    if (appStack.length > 0) {
      errorDetails.appStack = appStack.join("\n");
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
  error: unknown,
  jobData?: Record<string, unknown>,
): void {
  const e = errorFields(error);
  const errorDetails: Record<string, unknown> = {
    jobName,
    queueName,
    errorMessage: e.message || String(error),
    errorName: e.name,
    errorCode: e.code,
    jobData,
  };

  // Capture full stack trace
  if (e.stack) {
    errorDetails.stack = e.stack;

    // Extract app-relevant stack (without node_modules)
    const appStack = typeof e.stack === "string" ? appStackLines(e.stack) : [];
    if (appStack.length > 0) {
      errorDetails.appStack = appStack.join("\n");
    }
  }

  logToFile("job-failures", errorDetails);
}

/**
 * Log ML service errors
 */
export function logMLServiceError(
  operation: string,
  error: unknown,
  context?: Record<string, unknown>,
): void {
  const e = errorFields(error);
  const errorDetails: Record<string, unknown> = {
    service: "ml-service",
    operation,
    errorMessage: e.message || String(error),
    errorName: e.name,
    errorCode: e.code,
    context,
  };

  // Capture HTTP error details if available (axios errors)
  if (e.response) {
    errorDetails.httpStatus = e.response.status;
    errorDetails.httpStatusText = e.response.statusText;
    errorDetails.responseData = e.response.data;
  }

  // Capture network error details
  if (e.syscall) {
    errorDetails.syscall = e.syscall;
  }
  if (e.address) {
    errorDetails.address = e.address;
  }

  // Capture full stack trace
  if (e.stack) {
    errorDetails.stack = e.stack;

    // Extract app-relevant stack (without node_modules)
    const appStack = typeof e.stack === "string" ? appStackLines(e.stack) : [];
    if (appStack.length > 0) {
      errorDetails.appStack = appStack.join("\n");
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
  error: unknown,
  context?: Record<string, unknown>,
): void {
  const e = errorFields(error);
  const errorDetails: Record<string, unknown> = {
    component,
    operation,
    errorMessage: e.message || String(error),
    errorName: e.name,
    context,
  };

  if (e.stack) {
    errorDetails.stack = e.stack;
  }

  logToFile("infrastructure-errors", errorDetails);
}

/**
 * Log API rate limit blocks
 */
export function logRateLimitBlock(
  source: string,
  blockDurationMinutes: number,
  reason?: string,
  context?: Record<string, unknown>,
): void {
  // `source` (not `apiName`) to match the field name used by logExternalApiError
  // and the other diagnostic logs, so all log streams key the origin the same way.
  const logDetails: Record<string, unknown> = {
    source,
    blockDurationMinutes,
    reason,
    context,
  };

  logToFile("rate-limit-blocks", logDetails);
}
