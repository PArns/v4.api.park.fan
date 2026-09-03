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

/**
 * The hosts a subscription endpoint may point at.
 *
 * `web-push` makes an HTTP request to whatever string is stored here, from
 * inside this network, on a schedule, with no human watching. An `https`-only
 * check bounds that but does not close it: any URL the caller likes still gets
 * a POST from our origin, which is the shape of an SSRF and also a way to make
 * this API a traffic amplifier pointed at somebody else.
 *
 * The real endpoint always belongs to a browser's push service, and there are
 * four of them. Matching on the host closes the hole while accepting every
 * subscription a real browser can produce.
 *
 * `PUSH_ENDPOINT_HOSTS` exists because that list is not ours: a new browser or
 * a renamed service would otherwise be a silent refusal for those users until a
 * deploy. Comma-separated suffixes, added to the defaults rather than replacing
 * them, so a deploy cannot lose the known ones by setting it.
 */
const DEFAULT_PUSH_ENDPOINT_HOSTS = [
  // Chrome, Edge, Opera, and every Chromium derivative.
  "fcm.googleapis.com",
  "android.googleapis.com",
  // Firefox.
  "push.services.mozilla.com",
  // Windows / Edge legacy WNS.
  "notify.windows.com",
  // Safari, macOS and iOS.
  "web.push.apple.com",
] as const;

function allowedPushHosts(): string[] {
  const extra = (process.env.PUSH_ENDPOINT_HOSTS ?? "")
    .split(",")
    .map((h) => h.trim().toLowerCase())
    .filter(Boolean);
  return [...DEFAULT_PUSH_ENDPOINT_HOSTS, ...extra];
}

/**
 * Whether this is an endpoint a push service issued.
 *
 * Suffix match on a dot boundary, never `includes`: `endsWith("apple.com")`
 * alone would accept `web.push.apple.com.evil.test`, and a bare `includes`
 * would accept anything with the string in its path. The exact host counts as a
 * match so a service that stops using a subdomain keeps working.
 */
export function isAllowedPushEndpoint(url: URL): boolean {
  const host = url.hostname.toLowerCase();
  return allowedPushHosts().some(
    (allowed) => host === allowed || host.endsWith(`.${allowed}`),
  );
}
