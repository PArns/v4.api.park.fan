import { E2eContainers, E2E_CONTAINERS } from "./global-setup";

/**
 * Stops the pair `global-setup.ts` started. Runs once for the whole run, after
 * the last test file — the containers have to outlive every suite, so nothing
 * may stop them earlier.
 */
export default async function globalTeardown(): Promise<void> {
  const containers = (globalThis as Record<symbol, unknown>)[E2E_CONTAINERS] as
    E2eContainers | undefined;

  if (!containers) {
    return;
  }

  await containers.postgres.stop();
  console.log("🧹 Test container stopped");

  await containers.redis.stop();
  console.log("🧹 Redis container stopped");
}
