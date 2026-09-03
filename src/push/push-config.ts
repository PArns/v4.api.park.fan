/**
 * The VAPID keypair, and what happens when it is not there.
 *
 * Read lazily, never at import time. `@nestjs/config` only assigns a `.env`
 * file into `process.env` when `ConfigModule.forRoot()` runs, which happens
 * AFTER this module's imports are evaluated — the same trap `throttler.config`
 * documents, and the same fix.
 *
 * **An unconfigured deploy disables push; it does not break.** That is the
 * opposite of the choice `lib/security/turnstile.ts` makes on the frontend, and
 * deliberately so: a missing Turnstile secret must fail closed because the thing
 * it guards is a login, and letting strangers past a challenge that silently
 * stopped running is a security hole. A missing VAPID key guards nothing. The
 * failure mode is "no notifications", which is exactly what a deploy that has
 * not been given keys should do — and taking the API down over it would make
 * every other endpoint depend on a feature nobody asked for.
 *
 * So the endpoints answer 503 with a reason and the job does not run. What must
 * never happen is a subscription being accepted and stored against a server that
 * can never send to it: the browser then shows the visitor a switch that is on
 * and does nothing.
 */

export interface VapidConfig {
  publicKey: string;
  privateKey: string;
  /** `mailto:` or an https URL. Push services reject a JWT without one. */
  subject: string;
}

/**
 * The configured keypair, or `null`.
 *
 * All three parts or nothing: a public key with no private key cannot sign, and
 * a keypair with no subject is rejected by the push services rather than by us,
 * which turns a configuration mistake into an intermittent delivery failure.
 */
export function getVapidConfig(): VapidConfig | null {
  const publicKey = (process.env.VAPID_PUBLIC_KEY ?? "").trim();
  const privateKey = (process.env.VAPID_PRIVATE_KEY ?? "").trim();
  const subject = (process.env.VAPID_SUBJECT ?? "").trim();

  if (!publicKey || !privateKey || !subject) return null;
  // A subject that is neither of the two forms is not a subtle problem: every
  // push service rejects the JWT, so every send fails and the failure counter
  // eats the whole subscriber base.
  if (!subject.startsWith("mailto:") && !subject.startsWith("https://")) {
    return null;
  }
  return { publicKey, privateKey, subject };
}

/** Whether this deploy can send at all. */
export function isPushConfigured(): boolean {
  return getVapidConfig() !== null;
}

/**
 * The topics a browser may ask for.
 *
 * A closed set, checked on the way in, because the value is stored and then read
 * months later by a job with no request behind it — an unrecognised topic would
 * sit in the table forever matching nothing, and the visitor would see a switch
 * that is on and does nothing.
 *
 * - `next-up` — the next block in the plan is about to start.
 *
 * That is the whole list, and it is short on purpose. "A ride in your plan
 * stopped running" and "rain is moving in" are both obviously wanted and both
 * need a producer that does not exist yet — the notification job walks the plan
 * and the clock, and nothing else. Listing them here before something sends
 * them would put a switch in the browser that turns on and does nothing, which
 * is the one failure this whole module is arranged against. They go in the same
 * commit as their producer or not at all.
 */
export const PUSH_TOPICS = ["next-up"] as const;
export type PushTopic = (typeof PUSH_TOPICS)[number];

export function isPushTopic(value: unknown): value is PushTopic {
  return (
    typeof value === "string" &&
    (PUSH_TOPICS as readonly string[]).includes(value)
  );
}
