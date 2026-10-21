import * as fs from "fs";
import * as path from "path";
import { isShuttingDown } from "./graceful-shutdown";

/** The slice of `process` these handlers use, so the spec can hand in a stand-in. */
export interface CrashProcess {
  on(event: string, listener: (...args: unknown[]) => void): unknown;
  exit(code?: number): never;
}

interface CrashHandlerOptions {
  /** The process to listen on. Injected by the spec. */
  proc?: CrashProcess;
  /** Where the crash line goes. Injected by the spec. */
  write?: (type: string, message: string) => void;
  /** Whether a shutdown is running. Injected by the spec. */
  shuttingDown?: () => boolean;
}

/** Appends one JSON line to `logs/crash.log`, best effort. */
export function writeCrashLog(type: string, message: string): void {
  const logsDir = path.join(process.cwd(), "logs");
  if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
  }
  const entry =
    JSON.stringify({ timestamp: new Date().toISOString(), type, message }) +
    "\n";
  try {
    fs.appendFileSync(path.join(logsDir, "crash.log"), entry, "utf8");
  } catch {
    // ignore — we're already crashing
  }
}

/** What a thrown value or a rejection reason reads as in the log. */
function describe(reason: unknown): string {
  if (reason instanceof Error) return reason.stack ?? reason.message;
  return String(reason);
}

/**
 * Installs the last-resort handlers for an uncaught exception and an unhandled
 * rejection. Both record the crash and then end the process — except while a
 * shutdown is running.
 *
 * That exception is the point of this file. Closing 31 Bull queues, ioredis and
 * the TypeORM pool rejects with "Connection is closed" as a matter of course,
 * and a rejection that escapes Nest's own `close()` chain arrives here. Exiting
 * on one of those would stop the hooks mid-run and report exit code 1, so the
 * `exit` line in `logs/shutdown.<date>.log` would be missing and PAR-948 would
 * read this handler's own doing as a failed shutdown. The 20 s deadline in
 * `enableGracefulShutdown()` remains the thing that ends a hanging shutdown.
 */
export function installCrashHandlers(options: CrashHandlerOptions = {}): void {
  const { proc = process, write = writeCrashLog } = options;
  const shuttingDown = options.shuttingDown ?? isShuttingDown;

  proc.on("uncaughtException", (...args: unknown[]) => {
    const [error] = args;
    // EPIPE on stdout/stderr is benign — the log pipe closed, the API is fine.
    if ((error as NodeJS.ErrnoException | undefined)?.code === "EPIPE") return;
    write("uncaughtException", describe(error));
    if (shuttingDown()) return;
    proc.exit(1);
  });

  proc.on("unhandledRejection", (...args: unknown[]) => {
    const message = describe(args[0]);
    console.error("[CRASH] unhandledRejection", message);
    write("unhandledRejection", message);
    if (shuttingDown()) return;
    proc.exit(1);
  });
}
