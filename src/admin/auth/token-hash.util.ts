import { createHash, randomBytes, timingSafeEqual } from "crypto";

/**
 * Bearer secrets are stored and looked up by their SHA-256, never in clear:
 * whoever reads a Redis dump or a database backup holds hashes they cannot
 * present. Sessions, API tokens, OAuth codes and refresh tokens all go
 * through here.
 */
export function hashToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

/**
 * 32 bytes of CSPRNG, base64url: 256 bits, and nothing that needs escaping in
 * a header, a cookie or a query string.
 */
export function generateToken(prefix = ""): string {
  return `${prefix}${randomBytes(32).toString("base64url")}`;
}

/** Constant-time equality of two strings of any length. */
export function constantTimeEquals(a: string, b: string): boolean {
  if (!a || !b) return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  // timingSafeEqual throws on a length mismatch, which would itself leak the
  // length; pad both so the comparison always runs.
  const length = Math.max(bufA.length, bufB.length);
  const paddedA = Buffer.alloc(length);
  const paddedB = Buffer.alloc(length);
  bufA.copy(paddedA);
  bufB.copy(paddedB);
  return timingSafeEqual(paddedA, paddedB) && bufA.length === bufB.length;
}
