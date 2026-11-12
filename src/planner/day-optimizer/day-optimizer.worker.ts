import { parentPort } from "worker_threads";
import { runDayOptimizer } from "./engine";
import type { DayOptimizerJob } from "./pool";

/** The thread `DayOptimizerPool` starts: one job in, one answer out, nothing kept between them. */
parentPort?.on("message", (job: DayOptimizerJob & { id: number }) => {
  try {
    parentPort?.postMessage({
      id: job.id,
      result: runDayOptimizer(job.day, job.request, job.clock),
    });
  } catch (error) {
    parentPort?.postMessage({
      id: job.id,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});
