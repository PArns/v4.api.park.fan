import { createHash } from "crypto";
import { constantTimeEquals } from "../auth/token-hash.util";

/**
 * PKCE with S256 only (RFC 7636, required by OAuth 2.1). `plain` is refused:
 * it proves nothing to a server whose authorization request travels through
 * the same browser that would leak the code.
 */

/** An S256 challenge is the base64url of a SHA-256: always 43 characters. */
const S256_CHALLENGE = /^[A-Za-z0-9_-]{43}$/;

/** RFC 7636 §4.1: 43 to 128 unreserved characters. */
const CODE_VERIFIER = /^[A-Za-z0-9._~-]{43,128}$/;

export function isValidCodeChallenge(value: unknown): value is string {
  return typeof value === "string" && S256_CHALLENGE.test(value);
}

export function isValidCodeVerifier(value: unknown): value is string {
  return typeof value === "string" && CODE_VERIFIER.test(value);
}

export function s256Challenge(verifier: string): string {
  return createHash("sha256").update(verifier, "ascii").digest("base64url");
}

/** Whether `verifier` is the one the stored `challenge` was made from. */
export function verifyPkceS256(verifier: unknown, challenge: string): boolean {
  if (!isValidCodeVerifier(verifier)) return false;
  return constantTimeEquals(s256Challenge(verifier), challenge);
}
