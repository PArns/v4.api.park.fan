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

/** `s1.<expires>.<nonce>.<signature>`. */
export function issueSessionToken(key: Buffer, nowMs = Date.now()): string {
  const expires = Math.floor(nowMs / 1000) + SESSION_TTL_SECONDS;
  const nonce = randomBytes(12).toString("base64url");
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
