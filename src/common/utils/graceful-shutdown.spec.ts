import { ShutdownSignal } from "@nestjs/common";
import {
  FORCED_SHUTDOWN_EXIT_CODE,
  SHUTDOWN_DEADLINE_MS,
  SHUTDOWN_SIGNALS,
  enableGracefulShutdown,
  isShuttingDown,
} from "./graceful-shutdown";

type Handler = (...args: unknown[]) => void;

/** A stand-in for `process` that lets the spec raise a signal by hand. */
class FakeProcess {
  readonly handlers = new Map<string, Handler[]>();
  readonly exits: number[] = [];

  on = (event: string, handler: Handler): unknown => {
    const existing = this.handlers.get(event) ?? [];
    this.handlers.set(event, [...existing, handler]);
    return this;
  };

  exit = (code?: number): never => {
    this.exits.push(code ?? 0);
    return undefined as never;
  };

  raise(event: string, ...args: unknown[]): void {
    for (const handler of this.handlers.get(event) ?? []) handler(...args);
  }
}

describe("enableGracefulShutdown", () => {
  const deadlineMs = 20_000;
  let proc: FakeProcess;
  let trace: jest.Mock;
  let app: { enableShutdownHooks: jest.Mock };

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date("2026-10-20T19:44:58.000Z"));
    proc = new FakeProcess();
    trace = jest.fn();
    app = { enableShutdownHooks: jest.fn() };
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  function enable(options: { deadlineMs?: number } = {}): void {
    enableGracefulShutdown(app as never, {
      proc: proc as never,
      trace,
      deadlineMs: options.deadlineMs ?? deadlineMs,
    });
  }

  it("asks Nest for SIGTERM and SIGINT only, and for an explicit exit", () => {
    enable();

    expect(app.enableShutdownHooks).toHaveBeenCalledWith(
      [ShutdownSignal.SIGTERM, ShutdownSignal.SIGINT],
      { useProcessExit: true },
    );
    // SIGUSR2 is how a diagnostic report is requested (G-22), and the crash
    // signals should dump core instead of running hooks.
    expect(SHUTDOWN_SIGNALS).not.toContain(ShutdownSignal.SIGUSR2);
    expect(SHUTDOWN_SIGNALS).not.toContain(ShutdownSignal.SIGSEGV);
  });

  it("forces the exit once the deadline passes", () => {
    enable();
    proc.raise(ShutdownSignal.SIGTERM);

    jest.advanceTimersByTime(deadlineMs - 1);
    expect(proc.exits).toEqual([]);

    jest.advanceTimersByTime(1);
    expect(proc.exits).toEqual([FORCED_SHUTDOWN_EXIT_CODE]);
    expect(trace).toHaveBeenCalledWith({
      event: "deadline-exceeded",
      signal: ShutdownSignal.SIGTERM,
      waitedMs: deadlineMs,
    });
  });

  it("traces how long the hooks took when they finish inside the deadline", () => {
    enable();
    proc.raise(ShutdownSignal.SIGTERM);

    jest.advanceTimersByTime(1_500);
    // What Nest does after the hooks: process.exit(0), which fires 'exit'.
    proc.raise("exit", 0);

    expect(proc.exits).toEqual([]);
    expect(trace).toHaveBeenCalledWith({
      event: "exit",
      exitCode: 0,
      durationMs: 1_500,
    });
  });

  it("keeps the deadline of the first signal when a second one arrives", () => {
    enable();
    proc.raise(ShutdownSignal.SIGTERM);
    jest.advanceTimersByTime(deadlineMs - 1_000);
    proc.raise(ShutdownSignal.SIGINT);

    jest.advanceTimersByTime(1_000);

    expect(proc.exits).toEqual([FORCED_SHUTDOWN_EXIT_CODE]);
    expect(
      trace.mock.calls.filter((call) => call[0].event === "signal"),
    ).toHaveLength(1);
  });

  it("writes nothing when the process exits without a signal", () => {
    enable();

    proc.raise("exit", 1);

    expect(trace).not.toHaveBeenCalled();
  });

  it("pauses the Bull workers before Nest's own listener runs the hooks", () => {
    const order: string[] = [];
    // What Nest does in enableShutdownHooks: one listener per signal that
    // runs the hooks.
    app.enableShutdownHooks.mockImplementation((signals: string[]) => {
      for (const signal of signals) proc.on(signal, () => order.push("hooks"));
    });
    enableGracefulShutdown(app as never, {
      proc: proc as never,
      trace,
      deadlineMs,
      work: {
        stopTakingJobs: () => order.push("pause"),
        describeOpen: () => ({ openHooks: [], activeJobs: {} }),
      },
    });

    proc.raise(ShutdownSignal.SIGTERM);

    expect(order).toEqual(["pause", "hooks"]);
  });

  it("names the open hooks and the active jobs in the deadline line", () => {
    enableGracefulShutdown(app as never, {
      proc: proc as never,
      trace,
      deadlineMs,
      work: {
        stopTakingJobs: () => undefined,
        describeOpen: () => ({
          openHooks: ["Queue(live-cache-warmup).onApplicationShutdown"],
          activeJobs: {
            "live-cache-warmup": [{ id: "3698", name: "warmup-after-sync" }],
          },
        }),
      },
    });
    proc.raise(ShutdownSignal.SIGTERM);

    jest.advanceTimersByTime(deadlineMs);

    expect(trace).toHaveBeenCalledWith({
      event: "deadline-exceeded",
      signal: ShutdownSignal.SIGTERM,
      waitedMs: deadlineMs,
      openHooks: ["Queue(live-cache-warmup).onApplicationShutdown"],
      activeJobs: {
        "live-cache-warmup": [{ id: "3698", name: "warmup-after-sync" }],
      },
    });
  });

  it("still arms the deadline and traces the signal when pausing throws", () => {
    enableGracefulShutdown(app as never, {
      proc: proc as never,
      trace,
      deadlineMs,
      work: {
        stopTakingJobs: () => {
          throw new Error("Connection is closed.");
        },
        describeOpen: () => {
          throw new Error("Connection is closed.");
        },
      },
    });

    expect(() => proc.raise(ShutdownSignal.SIGTERM)).not.toThrow();
    expect(trace).toHaveBeenCalledWith(
      expect.objectContaining({ event: "signal" }),
    );

    jest.advanceTimersByTime(deadlineMs);
    expect(proc.exits).toEqual([FORCED_SHUTDOWN_EXIT_CODE]);
    expect(trace).toHaveBeenCalledWith(
      expect.objectContaining({ event: "deadline-exceeded" }),
    );
  });

  it("leaves Coolify's 30 s stop timeout head room", () => {
    expect(SHUTDOWN_DEADLINE_MS).toBeLessThan(30_000);
  });

  // The three cases below cover the one way this file can make a shutdown
  // worse than no shutdown at all: `logToFile` creates the log directory
  // outside its own try/catch, so the trace throws on a read-only or full
  // mount — and an exception out of a signal listener reaches the
  // `uncaughtException` handler.
  it("arms the deadline even when the trace throws on the signal", () => {
    trace.mockImplementation(() => {
      throw new Error("EROFS: read-only file system, mkdir '/app/logs'");
    });
    enable();

    expect(() => proc.raise(ShutdownSignal.SIGTERM)).not.toThrow();

    jest.advanceTimersByTime(deadlineMs);
    expect(proc.exits).toEqual([FORCED_SHUTDOWN_EXIT_CODE]);
  });

  it("forces the exit even when the trace throws on the deadline", () => {
    enable();
    proc.raise(ShutdownSignal.SIGTERM);
    trace.mockImplementation(() => {
      throw new Error("ENOSPC: no space left on device");
    });

    expect(() => jest.advanceTimersByTime(deadlineMs)).not.toThrow();
    expect(proc.exits).toEqual([FORCED_SHUTDOWN_EXIT_CODE]);
  });

  it("reports a running shutdown, which is what keeps the crash handlers out of it", () => {
    enable();
    expect(isShuttingDown()).toBe(false);

    proc.raise(ShutdownSignal.SIGTERM);

    expect(isShuttingDown()).toBe(true);
  });
});
