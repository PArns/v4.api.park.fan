import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { AdminQueueStatusService } from "../../admin/admin-queue-status.service";

export const queueStatusTool = defineMcpTool({
  name: "queue_status",
  title: "Job queues",
  description:
    "The Bull job queues (wait-times, park-metadata, ml-training, analytics, holidays …): active, " +
    "waiting, failed, delayed and completed counts for each. With `failuresOf`, also the most " +
    "recent failed jobs of that queue with their reason and first stack lines.",
  input: {
    failuresOf: z
      .string()
      .max(40)
      .optional()
      .describe('A queue name, e.g. "wait-times", to list its recent failures'),
    failureLimit: z
      .number()
      .int()
      .min(1)
      .max(10)
      .default(3)
      .describe("Failures to list"),
  },
  async handler({ failuresOf, failureLimit }, ctx) {
    const queues = ctx.get(AdminQueueStatusService);
    const counts = await queues.counts();
    if (!failuresOf) return { queues: counts };

    const failures = await queues.failures(failuresOf, failureLimit);
    return {
      queues: counts,
      failures: failures
        ? { queue: failuresOf, jobs: failures }
        : {
            error: `No failure log for "${failuresOf}"`,
            known: queues.inspectableQueues(),
          },
    };
  },
});
