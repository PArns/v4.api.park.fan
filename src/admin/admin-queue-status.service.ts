import { Injectable } from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";

export interface QueueCounts {
  name: string;
  active: number;
  pending: number;
  failed: number;
  delayed: number;
  completed: number;
}

export interface QueueFailure {
  id: string | number;
  name: string;
  data: unknown;
  attemptsMade: number;
  failedReason: string | undefined;
  stack: string[];
}

/**
 * Bull queue counts and the reasons recent jobs failed, for the admin's queue
 * screen and the MCP server's `queue_status` tool.
 */
@Injectable()
export class AdminQueueStatusService {
  /**
   * The queues the status screen counts, and whose failures it can read: one
   * list, because the screen offers "why" on every queue that shows a failure.
   */
  private readonly queues: Record<string, Queue>;

  constructor(
    @InjectQueue("holidays") holidays: Queue,
    @InjectQueue("park-metadata") parkMetadata: Queue,
    @InjectQueue("park-enrichment") parkEnrichment: Queue,
    @InjectQueue("ml-training") mlTraining: Queue,
    @InjectQueue("wait-times") waitTimes: Queue,
    @InjectQueue("children-metadata") children: Queue,
    @InjectQueue("six-flags-heights") sixFlagsHeights: Queue,
    @InjectQueue("ride-stats") rideStats: Queue,
    @InjectQueue("manual-metadata") manualMetadata: Queue,
    @InjectQueue("prediction-accuracy") accuracy: Queue,
    @InjectQueue("analytics") analytics: Queue,
    @InjectQueue("search-console") searchConsole: Queue,
    @InjectQueue("downtime") downtime: Queue,
    @InjectQueue("pcn-shadow") pcnShadow: Queue,
    @InjectQueue("park-day-operation") parkDayOperation: Queue,
  ) {
    this.queues = {
      "wait-times": waitTimes,
      "park-metadata": parkMetadata,
      "ml-training": mlTraining,
      "prediction-accuracy": accuracy,
      analytics,
      holidays,
      "park-enrichment": parkEnrichment,
      "children-metadata": children,
      // The queues you can trigger by hand, because right after firing one
      // is when you want to see whether the job waits, failed or is done.
      "manual-metadata": manualMetadata,
      "six-flags-heights": sixFlagsHeights,
      "ride-stats": rideStats,
      "search-console": searchConsole,
      downtime,
      "pcn-shadow": pcnShadow,
      "park-day-operation": parkDayOperation,
    };
  }

  async counts(): Promise<QueueCounts[]> {
    return Promise.all(
      Object.entries(this.queues).map(async ([name, queue]) => ({
        name,
        active: await queue.getActiveCount(),
        pending: await queue.getWaitingCount(),
        failed: await queue.getFailedCount(),
        delayed: await queue.getDelayedCount(),
        completed: await queue.getCompletedCount(),
      })),
    );
  }

  /** Names `failures` accepts. */
  inspectableQueues(): string[] {
    return Object.keys(this.queues);
  }

  /** The most recent failed jobs of one queue, or null for an unknown queue. */
  async failures(name: string, limit: number): Promise<QueueFailure[] | null> {
    const queue = Object.prototype.hasOwnProperty.call(this.queues, name)
      ? this.queues[name]
      : undefined;
    if (!queue) return null;
    const jobs = await queue.getFailed(0, Math.max(limit, 1) - 1);
    return jobs.map((job) => ({
      id: job.id,
      name: job.name,
      data: job.data as unknown,
      attemptsMade: job.attemptsMade,
      failedReason: job.failedReason,
      // The first frames only: the interesting line is near the top, and a
      // full stack turns the answer into a wall.
      stack: (job.stacktrace ?? []).join("\n").split("\n").slice(0, 12),
    }));
  }
}
