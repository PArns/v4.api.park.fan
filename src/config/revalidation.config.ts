/**
 * On-demand revalidation webhook config (frontend cache busting).
 *
 * After a background batch recomputes derived, cached-on-the-frontend data
 * (e.g. the precomputed best-days snapshot), the backend pings the frontend's
 * `/api/revalidate` endpoint with the affected cache tags so Next.js drops the
 * stale entry immediately instead of waiting for its time-based TTL.
 *
 * Functions (not constants) so tests can override the env vars after import,
 * matching the ml-services.config.ts pattern.
 *
 * The webhook is a no-op unless a secret is configured — this keeps dev, test
 * and CI instances from firing at the production frontend.
 */

/** Frontend revalidation endpoint. Defaults to production. */
export function getRevalidateUrl(): string {
  return process.env.REVALIDATE_URL || "https://park.fan/api/revalidate";
}

/**
 * Shared secret sent with every revalidation request (header
 * `x-revalidate-secret`). Empty ⇒ the webhook is disabled.
 */
export function getRevalidateSecret(): string {
  return process.env.REVALIDATE_SECRET || "";
}

/** Whether the revalidation webhook is enabled (secret configured). */
export function isRevalidationEnabled(): boolean {
  return getRevalidateSecret().length > 0;
}
