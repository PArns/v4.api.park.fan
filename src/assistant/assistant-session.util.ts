import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";

/**
 * The answer half of Parky's challenge-response. A browser solves Turnstile once, the API checks
 * that with Cloudflare and hands back this token; every chat, rating and feedback must carry it.
 * It is signed with a key derived from `ASSISTANT_SECRET_KEY` and expires. It is not bound to an
 * address, which changes on a phone between Wi-Fi and mobile data; the limits count per session
 * instead, and only issuing sessions is counted per address.
 */

const VERSION = "s1";
/** Two hours: a visit, not a day. A browser that outlives it solves the challenge again. */
export const SESSION_TTL_SECONDS = 2 * 60 * 60;

/** A separate key for signing, so the sealing key is never used for two jobs. */
export function sessionKey(masterKey: Buffer): Buffer {
  return createHmac("sha256", masterKey).update("assistant-session").digest();
}

function signature(key: Buffer, expires: number, nonce: string): string {
  return createHmac("sha256", key)
    .update(`${VERSION}.${expires}.${nonce}`)
    .digest("base64url");
}

/** Sits between an app session's random part and its data key's id; in neither, nor in a token's dots. */
const KEY_MARK = "~";

/**
 * `s1.<expires>.<nonce>.<signature>`. A session opened with a data key (park.fan's app) carries
 * the key's id in its nonce, under the signature: where a chat came from is what the server
 * checked when it issued the session, never what a client says later.
 */
export function issueSessionToken(
  key: Buffer,
  nowMs = Date.now(),
  apiKeyId?: string,
): string {
  const expires = Math.floor(nowMs / 1000) + SESSION_TTL_SECONDS;
  const random = randomBytes(12).toString("base64url");
  const nonce = apiKeyId ? `${random}${KEY_MARK}${apiKeyId}` : random;
  return `${VERSION}.${expires}.${nonce}.${signature(key, expires, nonce)}`;
}

/**
 * The session's id (its nonce) for a token this server signed that has not expired, else null.
 * The id is what the limits and the chat binding count by.
 */
export function verifySessionToken(
  key: Buffer,
  token: string,
  nowMs = Date.now(),
): string | null {
  const [version, rawExpires, nonce, given] = token.split(".");
  const expires = Number(rawExpires);
  if (version !== VERSION || !nonce || !given || !Number.isInteger(expires))
    return null;
  if (expires * 1000 <= nowMs) return null;
  const expected = Buffer.from(signature(key, expires, nonce));
  const actual = Buffer.from(given);
  return actual.length === expected.length && timingSafeEqual(actual, expected)
    ? nonce
    : null;
}

/** The data key a session was opened with (park.fan's app), or null for a browser's session. */
export function sessionApiKeyId(session: string): string | null {
  return session.split(KEY_MARK)[1] || null;
}

/** Where a chat comes from: the website's widget, or park.fan's app through its data key. */
export type AssistantSource = "web" | "app";

export function sessionSource(session: string): AssistantSource {
  return sessionApiKeyId(session) ? "app" : "web";
}
