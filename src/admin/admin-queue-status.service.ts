import { Injectable } from "@nestjs/common";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import {
  newestSuccessByName,
  recentFailures,
  settledAt,
  type QueueKey,
} from "../queues/queue-job-history";

/**
 * The newest failures of a queue that `failedOpen` looks at. Bull keeps up to
 * 500; a queue with more than 100 recent failures is red either way.
 */
const OPEN_FAILURE_SCAN = 100;

export interface QueueCounts {
  name: string;
  active: number;
  pending: number;
  /** Every failure Bull still keeps, however old. */
  failed: number;
  /**
   * The failures among the newest 100 that no later run of the same job has
   * succeeded since: the ones that still say something is wrong.
   */
  failedOpen: number;
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
  /** When the job failed, ISO; null for a job Bull holds no finish time for. */
  failedAt: string | null;
  /** When a later run of the same job succeeded, ISO; null while none has. */
  succeededSince: string | null;
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
      Object.entries(this.queues).map(async ([name, queue]) => {
        const failed = await queue.getFailedCount();
        return {
          name,
          active: await queue.getActiveCount(),
          pending: await queue.getWaitingCount(),
          failed,
          failedOpen: failed > 0 ? await this.openFailures(queue) : 0,
          delayed: await queue.getDelayedCount(),
          completed: await queue.getCompletedCount(),
        };
      }),
    );
  }

  /**
   * The failures no later run of the same job has succeeded since, by the
   * data-quality report's rule: a weekly sync that failed stays open until
   * its next run succeeds, a click that failed before a fix closes with the
   * next good run of that job.
   */
  private async openFailures(queue: Queue): Promise<number> {
    const key = keyOf(queue);
    const failures = await recentFailures(queue.client, key, OPEN_FAILURE_SCAN);
    if (failures.length === 0) return 0;
    const successes = await newestSuccessByName(
      queue.client,
      key,
      Math.min(...failures.map((f) => f.failedAt)),
    );
    return failures.filter(
      (f) => settledAt(successes, f.name, f.failedAt) === null,
    ).length;
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
    const times = jobs.map((job) => job.finishedOn ?? null);
    const known = times.filter((t): t is number => t !== null);
    const successes =
      known.length > 0
        ? await newestSuccessByName(
            queue.client,
            keyOf(queue),
            Math.min(...known),
          )
        : new Map<string, number>();
    return jobs.map((job, i) => {
      const failedAt = times[i];
      const settled =
        failedAt === null ? null : settledAt(successes, job.name, failedAt);
      return {
        id: job.id,
        name: job.name,
        data: job.data as unknown,
        attemptsMade: job.attemptsMade,
        failedReason: job.failedReason,
        // The first frames only: the interesting line is near the top, and a
        // full stack turns the answer into a wall.
        stack: (job.stacktrace ?? []).join("\n").split("\n").slice(0, 12),
        failedAt: failedAt === null ? null : new Date(failedAt).toISOString(),
        succeededSince:
          settled === null ? null : new Date(settled).toISOString(),
      };
    });
  }
}

/** A queue's Redis keys, with Bull's own prefix. */
function keyOf(queue: Queue): QueueKey {
  return (typeOrId) => queue.toKey(typeOrId);
}
