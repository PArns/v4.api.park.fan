import { ShutdownSignal } from "@nestjs/common";
import {
  FORCED_SHUTDOWN_EXIT_CODE,
  SHUTDOWN_DEADLINE_MS,
  SHUTDOWN_SIGNALS,
  enableGracefulShutdown,
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

  it("leaves Coolify's 30 s stop timeout head room", () => {
    expect(SHUTDOWN_DEADLINE_MS).toBeLessThan(30_000);
  });
});
