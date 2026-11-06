import { Injectable, OnModuleInit } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { getQueueToken } from "@nestjs/bull";
import type { Job, Queue } from "bull";
import { BULL_QUEUE_REGISTRATIONS } from "../queue-registrations";
import { newestSuccessByName, recordSuccess } from "../queue-job-history";

/**
 * Notes, per queue and job name, when a run last completed, so a failure that
 * a later run already fixed stops counting as open even after Bull trimmed
 * that run from its `completed` set (pcn-shadow: a daily training next to a
 * 15-minute forecast).
 */
@Injectable()
export class QueueSuccessRecorderService implements OnModuleInit {
  constructor(private readonly moduleRef: ModuleRef) {}

  onModuleInit(): void {
    for (const { name } of BULL_QUEUE_REGISTRATIONS) {
      if (!name) continue;
      const queue = this.moduleRef.get<Queue>(getQueueToken(name), {
        strict: false,
      });
      const key = (typeOrId: string) => queue.toKey(typeOrId);
      queue.on("completed", (job: Job) => {
        void recordSuccess(
          queue.client,
          key,
          job.name,
          job.finishedOn ?? Date.now(),
        );
      });
      // What the completed set still holds, so a deploy does not wait for the next run.
      void newestSuccessByName(queue.client, key, 0)
        .then((newest) =>
          Promise.all(
            [...newest].map(([job, at]) =>
              recordSuccess(queue.client, key, job, at),
            ),
          ),
        )
        .catch(() => undefined);
    }
  }
}
