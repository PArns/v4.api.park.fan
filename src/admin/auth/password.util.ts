import { randomBytes, scrypt as scryptCallback, timingSafeEqual } from "crypto";
import { promisify } from "util";

const scrypt = promisify(scryptCallback) as (
  password: string | Buffer,
  salt: string | Buffer,
  keylen: number,
  options: { N: number; r: number; p: number; maxmem: number },
) => Promise<Buffer>;

/**
 * Password hashing for admin accounts, on Node's own scrypt.
 *
 * scrypt rather than bcrypt or argon2 because both of those are native addons
 * that have to compile per platform, and this API already ships in a slim
 * Docker image with no build toolchain — an admin login is not worth a
 * node-gyp dependency. scrypt is memory-hard, is in the standard library, and
 * at these parameters costs ~100 ms per verification, which is the point: it
 * prices an offline guessing attack out of reach while a human logging in once
 * a day never notices.
 *
 * The parameters travel INSIDE the stored string rather than living in a
 * constant, so raising the cost later does not invalidate every existing hash:
 * old hashes keep verifying with the parameters they were written at, and
 * `needsRehash` reports the ones worth upgrading on their owner's next
 * successful login (which is the only moment the plaintext exists).
 */

/** CPU/memory cost. 2^15 × 8 × 128 B ≈ 32 MB per hash. */
const N = 32768;
const R = 8;
const P = 1;
const KEY_LENGTH = 64;
const SALT_LENGTH = 16;

/** scrypt refuses to allocate past `maxmem`; the default 32 MB is exactly on
 *  the line for N=32768 and throws. Give it room rather than lowering N. */
const MAX_MEM = 128 * 1024 * 1024;

const PREFIX = "scrypt";

/** Hash a plaintext password into a self-describing `scrypt$N$r$p$salt$key`. */
export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(SALT_LENGTH);
  const key = await scrypt(password.normalize("NFKC"), salt, KEY_LENGTH, {
    N,
    r: R,
    p: P,
    maxmem: MAX_MEM,
  });
  return [
    PREFIX,
    N,
    R,
    P,
    salt.toString("base64"),
    key.toString("base64"),
  ].join("$");
}

/**
 * Verify a password against a stored hash.
 *
 * Never throws on a malformed stored value: a row whose hash was corrupted
 * should fail the login, not 500 the endpoint and tell the caller that this
 * particular account exists.
 */
export async function verifyPassword(
  password: string,
  stored: string,
): Promise<boolean> {
  const parts = stored?.split("$") ?? [];
  if (parts.length !== 6 || parts[0] !== PREFIX) return false;

  const n = Number.parseInt(parts[1], 10);
  const r = Number.parseInt(parts[2], 10);
  const p = Number.parseInt(parts[3], 10);
  if (!Number.isFinite(n) || !Number.isFinite(r) || !Number.isFinite(p)) {
    return false;
  }

  let salt: Buffer;
  let expected: Buffer;
  try {
    salt = Buffer.from(parts[4], "base64");
    expected = Buffer.from(parts[5], "base64");
  } catch {
    return false;
  }
  if (salt.length === 0 || expected.length === 0) return false;

  try {
    const actual = await scrypt(
      password.normalize("NFKC"),
      salt,
      expected.length,
      { N: n, r, p, maxmem: MAX_MEM },
    );
    return timingSafeEqual(actual, expected);
  } catch {
    return false;
  }
}

/** True when a stored hash was written at weaker parameters than we use now. */
export function needsRehash(stored: string): boolean {
  const parts = stored?.split("$") ?? [];
  if (parts.length !== 6 || parts[0] !== PREFIX) return true;
  return (
    Number.parseInt(parts[1], 10) < N ||
    Number.parseInt(parts[2], 10) < R ||
    Number.parseInt(parts[3], 10) < P
  );
}

/**
 * Minimum password policy for admin accounts.
 *
 * Length only, deliberately. Composition rules ("one uppercase, one digit")
 * measurably push people towards `Password1!` and are not what makes a
 * passphrase hard to guess; 12 characters is. Returns the complaint or null.
 */
export function validatePasswordStrength(password: string): string | null {
  if (typeof password !== "string" || password.length < 12) {
    return "Password must be at least 12 characters long";
  }
  if (password.length > 256) {
    return "Password must be at most 256 characters long";
  }
  if (password.trim().length === 0) {
    return "Password must not be only whitespace";
  }
  return null;
}
