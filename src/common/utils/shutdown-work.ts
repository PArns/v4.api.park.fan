import { Logger } from "@nestjs/common";
import type { ModulesContainer } from "@nestjs/core";

/** What `enableGracefulShutdown` asks of the running app (PAR-948). */
export interface ShutdownWork {
  /**
   * Stops every Bull worker in this process from taking another job. Called
   * first on the signal, before Nest runs a single hook. Never throws.
   */
  stopTakingJobs(): void;
  /** The hooks still running and the Bull jobs still active, per queue. */
  describeOpen(): {
    openHooks: string[];
    activeJobs: Record<string, { id: string; name: string }[]>;
  };
}

/** The slice of a Bull queue this uses, so the spec can hand in a stand-in. */
interface BullQueueLike {
  name: string;
  pause(isLocal?: boolean, doNotWaitActive?: boolean): Promise<void>;
  whenCurrentJobsFinished(): Promise<void>;
  on(
    event: string,
    listener: (job: { id: unknown; name: string }) => void,
  ): unknown;
}

const LIFECYCLE_HOOKS = [
  "onModuleDestroy",
  "beforeApplicationShutdown",
  "onApplicationShutdown",
] as const;

function isBullQueue(instance: unknown): instance is BullQueueLike {
  const queue = instance as Partial<BullQueueLike> | null;
  return (
    typeof queue === "object" &&
    queue !== null &&
    typeof queue.name === "string" &&
    typeof queue.pause === "function" &&
    typeof queue.whenCurrentJobsFinished === "function"
  );
}

/**
 * Collects the Bull queues and the instances with shutdown hooks from the Nest
 * module graph, so a shutdown can stop the workers first and say what is still
 * open when the deadline hits.
 *
 * Measured on 2026-10-22: three of six deploy shutdowns ran into the 20 s
 * deadline, and one of them took a `live-cache-warmup` job 2 s AFTER SIGTERM —
 * the job the deadline then killed and the next container marked stalled. The
 * deadline line could not say which hook held the shutdown (PAR-948).
 */
export function watchShutdownWork(modules: ModulesContainer): ShutdownWork {
  const logger = new Logger("Shutdown");
  const queues = new Set<BullQueueLike>();
  const hookOwners = new Set<object>();

  for (const module of modules.values()) {
    for (const wrappers of [
      module.providers,
      module.controllers,
      module.injectables,
    ]) {
      for (const { instance } of wrappers.values()) {
        if (isBullQueue(instance)) queues.add(instance);
        else if (
          typeof instance === "object" &&
          instance !== null &&
          LIFECYCLE_HOOKS.some(
            (hook) =>
              typeof (instance as Record<string, unknown>)[hook] === "function",
          )
        ) {
          hookOwners.add(instance);
        }
      }
    }
  }

  // Bull keeps no local list of the jobs it runs, so this process keeps one.
  // The events are local: only the worker that runs a job emits them.
  const active = new Map<string, Map<string, string>>();
  for (const queue of queues) {
    const running = active.get(queue.name) ?? new Map<string, string>();
    active.set(queue.name, running);
    queue.on("active", (job) => running.set(String(job.id), job.name));
    queue.on("completed", (job) => running.delete(String(job.id)));
    queue.on("failed", (job) => running.delete(String(job.id)));
  }

  const openHooks = new Map<object, string>();

  /**
   * Wraps the hooks only once the shutdown begins, so a running app (and the
   * E2E suites' `app.close()`) never goes through the wrapper. Nest looks the
   * hook up on the instance when it calls it, which is after this runs.
   */
  const trackHooks = (): void => {
    for (const owner of hookOwners) {
      const target = owner as Record<string, unknown>;
      for (const hook of LIFECYCLE_HOOKS) {
        const original = target[hook];
        if (typeof original !== "function") continue;
        const label = `${owner.constructor.name}.${hook}`;
        target[hook] = function (this: unknown, ...args: unknown[]): unknown {
          const key = {};
          openHooks.set(key, label);
          try {
            const result: unknown = original.apply(this, args);
            if (result instanceof Promise) {
              return result.finally(() => openHooks.delete(key));
            }
            openHooks.delete(key);
            return result;
          } catch (error) {
            openHooks.delete(key);
            throw error;
          }
        };
      }
    }
  };

  return {
    stopTakingJobs(): void {
      for (const queue of queues) {
        try {
          // Local pause; `doNotWaitActive` drops the blocking BRPOPLPUSH so a
          // job pushed after the signal is not taken, and leaves the running
          // one to `queue.close()` in Bull's own hook.
          queue.pause(true, true).catch((error: unknown) => {
            logger.warn(`Pausing ${queue.name} failed: ${String(error)}`);
          });
        } catch (error) {
          logger.warn(`Pausing ${queue.name} failed: ${String(error)}`);
        }
      }
      try {
        trackHooks();
      } catch (error) {
        logger.warn(`Tracking the shutdown hooks failed: ${String(error)}`);
      }
    },
    describeOpen() {
      const activeJobs: Record<string, { id: string; name: string }[]> = {};
      for (const [queue, running] of active) {
        if (running.size === 0) continue;
        activeJobs[queue] = [...running].map(([id, name]) => ({ id, name }));
      }
      return { openHooks: [...openHooks.values()], activeJobs };
    },
  };
}
