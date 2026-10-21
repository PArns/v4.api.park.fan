import { Logger, ShutdownSignal } from "@nestjs/common";
import type { INestApplication } from "@nestjs/common";
import { logToFile } from "./file-logger.util";

/**
 * The signals that shut the app down. Deliberately NOT Nest's default set:
 * that one covers every `ShutdownSignal`, SIGUSR2 included, and SIGUSR2 is the
 * signal a diagnostic report is requested with when a process has to be asked
 * what holds its event loop (G-22). Capturing it would turn a debugging request
 * into a shutdown. SIGILL/SIGSEGV/SIGBUS/SIGFPE are left alone for the same
 * reason in reverse: a crashing process should dump core, not run hooks.
 */
export const SHUTDOWN_SIGNALS: ShutdownSignal[] = [
  ShutdownSignal.SIGTERM,
  ShutdownSignal.SIGINT,
];

/**
 * How long the hooks get before the process exits anyway.
 *
 * Coolify stops the container with `docker stop --timeout=30`, measured in the
 * deploy log, so everything still running at 30 s is SIGKILLed — which is the
 * ungraceful stop this whole path exists to avoid. The hooks can take that
 * long: Bull's `queue.close()` waits for the job in flight across 31 queues,
 * and jobs here run for minutes (the daily `downtime` run takes ~200 s). 20 s
 * leaves 10 s of head room for the exit itself.
 */
export const SHUTDOWN_DEADLINE_MS = 20_000;

/** 128 + SIGTERM(15), so a forced exit is distinguishable from a clean one. */
export const FORCED_SHUTDOWN_EXIT_CODE = 143;

/**
 * Written to `/data/parkfan/logs/shutdown.<date>.log`, which is a bind mount on
 * the host and survives the deploy that caused the shutdown — unlike stdout,
 * which goes away with the container (G-167).
 */
export const SHUTDOWN_LOG_FILE = "shutdown";

/** The slice of `process` this uses, so the spec can hand in a stand-in. */
export interface ShutdownProcess {
  on(event: string, listener: (...args: never[]) => void): unknown;
  exit(code?: number): never;
}

interface GracefulShutdownOptions {
  /** Overrides {@link SHUTDOWN_DEADLINE_MS}. */
  deadlineMs?: number;
  /** The process to listen on. Injected by the spec. */
  proc?: ShutdownProcess;
  /** Where the trace goes. Injected by the spec. */
  trace?: (data: Record<string, unknown>) => void;
}

/**
 * When the shutdown began, or `undefined` while the process runs normally.
 *
 * Module state rather than a closure because the crash handlers in
 * `installCrashHandlers()` have to read it: they are installed before the Nest
 * application exists, and during a shutdown they must not exit the process.
 */
let shutdownStartedAt: number | undefined;

/**
 * Whether a signal has started the shutdown.
 *
 * Closing 31 Bull queues, ioredis and the TypeORM pool rejects with
 * "Connection is closed" as a matter of course. Exiting on one of those would
 * abort the hooks mid-run, with code 1 instead of 0 or 143 and without the
 * `exit` line in `logs/shutdown.<date>.log` that PAR-948 measures. The deadline
 * stays the backstop for a hook that hangs.
 */
export function isShuttingDown(): boolean {
  return shutdownStartedAt !== undefined;
}

/**
 * Makes a signal run the lifecycle hooks, and bounds how long they may take.
 *
 * Without this, nothing in this process reacts to SIGTERM: Nest calls
 * `onModuleDestroy` / `onApplicationShutdown` only from `app.close()`, so a
 * Coolify redeploy killed the process mid-flight — 31 Bull queues with their
 * locks still held, ~19 TypeORM pool connections, and the abortable boot chains
 * from PAR-79 never reached (PAR-903). Measured: the old container returned
 * from `docker stop` after 133 ms.
 *
 * `useProcessExit` is required rather than optional here. Nest's default ending
 * is `process.kill(process.pid, signal)`, which relies on the default signal
 * action — and that action is suppressed while any listener is registered, the
 * deadline listener below included. The app would then wait out the deadline on
 * every clean shutdown instead of exiting.
 *
 * The deadline is global instead of per hook on purpose. A per-chain timeout
 * would let `app.close()` continue while the query it gave up on still runs,
 * which is the closed-pool-under-a-running-query case `AbortableRuns` was
 * written to avoid. Exiting the process takes the statement with it, and every
 * statement on that path is idempotent, so the next boot resumes it.
 */
export function enableGracefulShutdown(
  app: Pick<INestApplication, "enableShutdownHooks">,
  options: GracefulShutdownOptions = {},
): void {
  const {
    deadlineMs = SHUTDOWN_DEADLINE_MS,
    proc = process,
    trace = (data: Record<string, unknown>) =>
      logToFile(SHUTDOWN_LOG_FILE, data),
  } = options;
  const logger = new Logger("Shutdown");

  // Registering is the start of a lifecycle, so nothing is shutting down yet.
  // Only the spec ever reaches this twice; `bootstrap()` calls it once.
  shutdownStartedAt = undefined;

  app.enableShutdownHooks(SHUTDOWN_SIGNALS, { useProcessExit: true });

  /**
   * Writing the trace must never be the reason a shutdown stops. `logToFile`
   * creates the log directory outside its own try/catch, so a read-only mount
   * or a full disk throws out of it (EACCES, ENOSPC) — and an exception out of
   * a signal listener lands in the `uncaughtException` handler, which is the
   * one path that can still kill this process while the hooks run.
   */
  const traceSafely = (data: Record<string, unknown>): void => {
    try {
      trace(data);
    } catch (error) {
      // Best effort: if the file trace failed because the mount is read-only
      // or full, stdout may be gone as well.
      try {
        logger.error(`Failed to write the shutdown trace: ${String(error)}`);
      } catch {
        // nothing left to report with
      }
    }
  };

  const onSignal = (signal: ShutdownSignal): void => {
    // A second signal while the first shutdown runs is ignored, the same way
    // Nest's own handler ignores it.
    if (shutdownStartedAt !== undefined) return;
    const begunAt = Date.now();
    shutdownStartedAt = begunAt;

    // The deadline is armed before anything that logs or writes. It is the only
    // thing left that ends this process if a hook hangs, so nothing that can
    // throw may run ahead of it.
    const deadline = setTimeout(() => {
      const waitedMs = Date.now() - begunAt;
      try {
        logger.error(
          `Shutdown hooks still running after ${waitedMs} ms — forcing exit`,
        );
        traceSafely({ event: "deadline-exceeded", signal, waitedMs });
      } finally {
        proc.exit(FORCED_SHUTDOWN_EXIT_CODE);
      }
    }, deadlineMs);
    // The deadline must not be the reason the process stays alive: if the hooks
    // finish and nothing else holds the loop, the process may just end.
    deadline.unref();

    logger.log(
      `${signal} received — running shutdown hooks (${deadlineMs} ms)`,
    );
    traceSafely({ event: "signal", signal, deadlineMs });
  };

  for (const signal of SHUTDOWN_SIGNALS) {
    proc.on(signal, () => onSignal(signal));
  }

  // The duration is the number that says whether the hooks ran at all, and it
  // is only knowable here, after Nest's `process.exit(0)`.
  proc.on("exit", (code: number) => {
    if (shutdownStartedAt === undefined) return;
    traceSafely({
      event: "exit",
      exitCode: code,
      durationMs: Date.now() - shutdownStartedAt,
    });
  });
}
