import { appendFileSync, existsSync, mkdirSync, renameSync, statSync, unlinkSync } from "fs";
import { join } from "path";

const MAX_LOG_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB
const MAX_ROTATED_FILES = 3;

/**
 * Rotates a log file if it exceeds MAX_LOG_SIZE_BYTES.
 * Keeps up to MAX_ROTATED_FILES rotated copies: .log.1, .log.2, .log.3
 */
function rotateIfNeeded(filepath: string): void {
  try {
    if (!existsSync(filepath)) return;
    const { size } = statSync(filepath);
    if (size < MAX_LOG_SIZE_BYTES) return;

    // Shift existing rotated files: .log.2 → .log.3, .log.1 → .log.2
    for (let i = MAX_ROTATED_FILES - 1; i >= 1; i--) {
      const src = `${filepath}.${i}`;
      const dst = `${filepath}.${i + 1}`;
      if (existsSync(src)) {
        if (i === MAX_ROTATED_FILES - 1 && existsSync(dst)) {
          unlinkSync(dst);
        }
        renameSync(src, dst);
      }
    }

    // Rotate current log: .log → .log.1
    renameSync(filepath, `${filepath}.1`);
  } catch {
    // Rotation failure is non-fatal — keep writing to current file
  }
}

/**
 * Simple file logger for critical errors that should not be missed in console logs.
 * Automatically rotates files at 10 MB, keeping 3 rotated copies.
 *
 * Usage:
 * ```ts
 * logToFile('external-api-errors', {
 *   source: 'QueueTimesClient',
 *   error: 'fetch failed',
 *   details: { parkId: 275, url: '...' }
 * });
 * ```
 */
export function logToFile(filename: string, data: Record<string, any>): void {
  const logsDir = join(process.cwd(), "logs");

  if (!existsSync(logsDir)) {
    mkdirSync(logsDir, { recursive: true });
  }

  const filepath = join(logsDir, `${filename}.log`);

  rotateIfNeeded(filepath);

  const logEntry = {
    timestamp: new Date().toISOString(),
    ...data,
  };

  const logLine = JSON.stringify(logEntry) + "\n";

  try {
    appendFileSync(filepath, logLine, "utf8");
  } catch (error) {
    // Fallback to console if file write fails
    console.error(`Failed to write to ${filepath}:`, error);
    console.error("Original log entry:", logEntry);
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
