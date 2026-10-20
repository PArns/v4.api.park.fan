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

  app.enableShutdownHooks(SHUTDOWN_SIGNALS, { useProcessExit: true });

  let startedAt: number | undefined;

  const onSignal = (signal: ShutdownSignal): void => {
    // A second signal while the first shutdown runs is ignored, the same way
    // Nest's own handler ignores it.
    if (startedAt !== undefined) return;
    const begunAt = Date.now();
    startedAt = begunAt;

    logger.log(
      `${signal} received — running shutdown hooks (${deadlineMs} ms)`,
    );
    trace({ event: "signal", signal, deadlineMs });

    const deadline = setTimeout(() => {
      const waitedMs = Date.now() - begunAt;
      logger.error(
        `Shutdown hooks still running after ${waitedMs} ms — forcing exit`,
      );
      trace({ event: "deadline-exceeded", signal, waitedMs });
      proc.exit(FORCED_SHUTDOWN_EXIT_CODE);
    }, deadlineMs);
    // The deadline must not be the reason the process stays alive: if the hooks
    // finish and nothing else holds the loop, the process may just end.
    deadline.unref();
  };

  for (const signal of SHUTDOWN_SIGNALS) {
    proc.on(signal, () => onSignal(signal));
  }

  // The duration is the number that says whether the hooks ran at all, and it
  // is only knowable here, after Nest's `process.exit(0)`.
  proc.on("exit", (code: number) => {
    if (startedAt === undefined) return;
    trace({
      event: "exit",
      exitCode: code,
      durationMs: Date.now() - startedAt,
    });
  });
}
