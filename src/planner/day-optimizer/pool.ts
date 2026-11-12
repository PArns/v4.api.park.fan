import { existsSync } from "fs";
import { join } from "path";
import { Worker } from "worker_threads";
import {
  runDayOptimizer,
  type DayOptimizerRequest,
  type DayOptimizerResult,
} from "./engine";
import type { DayClock, OptimizerDay } from "./types";

/**
 * Runs {@link runDayOptimizer} off the request thread.
 *
 * One search on a full day takes about a tenth of a second and a fit question with its levers
 * several of them, all synchronous: on the event loop that is a pause for every other request the
 * process serves. So the searches run in a few worker threads, queued, with a ceiling on the queue
 * and a time limit per job; both refuse with an error the controller turns into a 503.
 *
 * Inline where there is no compiled worker to start (ts-jest, ts-node): the answer is the same.
 */

/** One job: the day, the request and the clock it is planned against. */
export interface DayOptimizerJob {
  day: OptimizerDay | null;
  request: DayOptimizerRequest;
  clock: DayClock;
}

/** The queue is full. */
export class DayOptimizerBusyError extends Error {
  constructor() {
    super("day optimizer queue is full");
  }
}

/** A job ran past its time limit and its worker was stopped. */
export class DayOptimizerTimeoutError extends Error {
  constructor() {
    super("day optimizer job timed out");
  }
}

export interface DayOptimizerPoolOptions {
  /** Worker threads at most. */
  size: number;
  /** Jobs that may wait for a worker before a request is refused. */
  maxQueue: number;
  /** How long one job may run before its worker is stopped. */
  timeoutMs: number;
  /** The compiled worker; `null` runs every job inline. */
  workerFile: string | null;
}

interface Task {
  job: DayOptimizerJob;
  resolve: (result: DayOptimizerResult) => void;
  reject: (error: Error) => void;
}

interface Running extends Task {
  id: number;
  timer: NodeJS.Timeout;
}

/** The compiled worker beside this file, or `null` where this file is TypeScript. */
export function compiledWorkerFile(): string | null {
  const file = join(__dirname, "day-optimizer.worker.js");
  return __filename.endsWith(".js") && existsSync(file) ? file : null;
}

export class DayOptimizerPool {
  private readonly workers = new Set<Worker>();
  private readonly idle: Worker[] = [];
  private readonly running = new Map<Worker, Running>();
  private readonly queue: Task[] = [];
  private nextId = 1;
  private closed = false;

  constructor(private readonly options: DayOptimizerPoolOptions) {}

  /** Whether jobs run in worker threads. */
  get threaded(): boolean {
    return this.options.workerFile !== null;
  }

  run(job: DayOptimizerJob): Promise<DayOptimizerResult> {
    if (!this.options.workerFile) {
      return Promise.resolve().then(() =>
        runDayOptimizer(job.day, job.request, job.clock),
      );
    }
    if (this.closed) return Promise.reject(new DayOptimizerBusyError());
    if (this.queue.length >= this.options.maxQueue) {
      return Promise.reject(new DayOptimizerBusyError());
    }
    return new Promise((resolve, reject) => {
      this.queue.push({ job, resolve, reject });
      this.drain();
    });
  }

  /** Stops every worker and refuses what is still queued. */
  async close(): Promise<void> {
    this.closed = true;
    for (const task of this.queue.splice(0)) {
      task.reject(new DayOptimizerBusyError());
    }
    for (const [, task] of this.running) {
      clearTimeout(task.timer);
      task.reject(new DayOptimizerBusyError());
    }
    this.running.clear();
    const workers = [...this.workers];
    this.workers.clear();
    this.idle.length = 0;
    await Promise.all(workers.map((worker) => worker.terminate()));
  }

  private drain(): void {
    while (this.queue.length > 0 && !this.closed) {
      const worker =
        this.idle.pop() ??
        (this.workers.size < this.options.size ? this.spawn() : null);
      if (!worker) return;
      const task = this.queue.shift() as Task;
      const id = this.nextId++;
      const timer = setTimeout(
        () => this.stop(worker, new DayOptimizerTimeoutError()),
        this.options.timeoutMs,
      );
      this.running.set(worker, { ...task, id, timer });
      worker.postMessage({ id, ...task.job });
    }
  }

  private spawn(): Worker {
    const worker = new Worker(this.options.workerFile as string);
    // A pool of idle threads must not keep the process alive on shutdown.
    worker.unref();
    worker.on(
      "message",
      (message: {
        id: number;
        result?: DayOptimizerResult;
        error?: string;
      }) => {
        const task = this.running.get(worker);
        if (!task || task.id !== message.id) return;
        clearTimeout(task.timer);
        this.running.delete(worker);
        this.idle.push(worker);
        if (message.error !== undefined) task.reject(new Error(message.error));
        else task.resolve(message.result as DayOptimizerResult);
        this.drain();
      },
    );
    worker.on("error", (error: unknown) =>
      this.stop(
        worker,
        error instanceof Error ? error : new Error(String(error)),
      ),
    );
    worker.on("exit", () => this.stop(worker, new Error("worker exited")));
    this.workers.add(worker);
    return worker;
  }

  /** Takes a worker out of the pool, failing the job it held, and lets the queue move on. */
  private stop(worker: Worker, error: Error): void {
    if (!this.workers.delete(worker)) return;
    const index = this.idle.indexOf(worker);
    if (index >= 0) this.idle.splice(index, 1);
    const task = this.running.get(worker);
    this.running.delete(worker);
    if (task) {
      clearTimeout(task.timer);
      task.reject(error);
    }
    void worker.terminate();
    this.drain();
  }
}
