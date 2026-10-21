import { installCrashHandlers } from "./crash-handlers";

type Handler = (...args: unknown[]) => void;

/** A stand-in for `process` that lets the spec raise a crash by hand. */
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

describe("installCrashHandlers", () => {
  let proc: FakeProcess;
  let write: jest.Mock;
  let shuttingDown: jest.Mock;

  beforeEach(() => {
    proc = new FakeProcess();
    write = jest.fn();
    shuttingDown = jest.fn().mockReturnValue(false);
    jest.spyOn(console, "error").mockImplementation(() => undefined);
    installCrashHandlers({
      proc: proc as never,
      write,
      shuttingDown: shuttingDown as () => boolean,
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it("records an uncaught exception and exits", () => {
    proc.raise("uncaughtException", new Error("boom"));

    expect(write).toHaveBeenCalledWith(
      "uncaughtException",
      expect.stringContaining("boom"),
    );
    expect(proc.exits).toEqual([1]);
  });

  it("records an unhandled rejection and exits", () => {
    proc.raise("unhandledRejection", new Error("nope"));

    expect(write).toHaveBeenCalledWith(
      "unhandledRejection",
      expect.stringContaining("nope"),
    );
    expect(proc.exits).toEqual([1]);
  });

  it("ignores EPIPE, which is the log pipe closing and not a crash", () => {
    const epipe: NodeJS.ErrnoException = new Error("write EPIPE");
    epipe.code = "EPIPE";

    proc.raise("uncaughtException", epipe);

    expect(write).not.toHaveBeenCalled();
    expect(proc.exits).toEqual([]);
  });

  // The reason this file exists: closing 31 Bull queues, ioredis and the
  // TypeORM pool rejects with "Connection is closed" as a matter of course.
  // Exiting on that would abort the hooks mid-run, report 1 instead of 0 or
  // 143, and leave no `exit` line for PAR-948 to measure.
  it("does not exit on a rejection while the shutdown runs", () => {
    shuttingDown.mockReturnValue(true);

    proc.raise("unhandledRejection", new Error("Connection is closed"));

    expect(write).toHaveBeenCalledWith(
      "unhandledRejection",
      expect.stringContaining("Connection is closed"),
    );
    expect(proc.exits).toEqual([]);
  });

  it("does not exit on an uncaught exception while the shutdown runs", () => {
    shuttingDown.mockReturnValue(true);

    proc.raise("uncaughtException", new Error("Connection is closed"));

    expect(write).toHaveBeenCalled();
    expect(proc.exits).toEqual([]);
  });

  it("stringifies a rejection that is not an Error", () => {
    proc.raise("unhandledRejection", "just a string");

    expect(write).toHaveBeenCalledWith("unhandledRejection", "just a string");
    expect(proc.exits).toEqual([1]);
  });
});
