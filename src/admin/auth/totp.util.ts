import { createHmac, randomBytes, timingSafeEqual } from "crypto";

/**
 * RFC 6238 TOTP, implemented here rather than pulled in.
 *
 * It is thirty lines of HMAC and a base32 codec, and the alternative is a
 * dependency in the trust path of the login — which is the one place in this
 * codebase where a supply-chain surprise would be worst. Interoperable with
 * Google Authenticator, 1Password, Aegis and anything else that speaks
 * otpauth://.
 */

const ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
const DIGITS = 6;
const STEP_SECONDS = 30;

/**
 * How far either side of now a code is accepted.
 *
 * One step (±30 s) rather than the more common two: the extra window exists to
 * forgive clock drift, and every phone in use has NTP. Widening it to ±90 s
 * triples the number of codes valid at any moment for no practical gain.
 */
const WINDOW_STEPS = 1;

export function generateTotpSecret(byteLength = 20): string {
  return base32Encode(randomBytes(byteLength));
}

export function base32Encode(buffer: Buffer): string {
  let bits = 0;
  let value = 0;
  let output = "";
  for (const byte of buffer) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      output += ALPHABET[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) output += ALPHABET[(value << (5 - bits)) & 31];
  return output;
}

export function base32Decode(input: string): Buffer {
  const clean = input.replace(/=+$/, "").replace(/\s+/g, "").toUpperCase();
  let bits = 0;
  let value = 0;
  const bytes: number[] = [];
  for (const char of clean) {
    const index = ALPHABET.indexOf(char);
    if (index === -1) throw new Error("Invalid base32 character");
    value = (value << 5) | index;
    bits += 5;
    if (bits >= 8) {
      bytes.push((value >>> (bits - 8)) & 0xff);
      bits -= 8;
    }
  }
  return Buffer.from(bytes);
}

/** The counter value for a moment in time. Exported because the replay guard
 *  stores it: a consumed step must not be accepted a second time. */
export function totpStep(atMs: number = Date.now()): number {
  return Math.floor(atMs / 1000 / STEP_SECONDS);
}

export function totpCodeForStep(secret: string, step: number): string {
  const key = base32Decode(secret);
  const counter = Buffer.alloc(8);
  counter.writeBigUInt64BE(BigInt(step));

  const digest = createHmac("sha1", key).update(counter).digest();
  const offset = digest[digest.length - 1] & 0x0f;
  const binary =
    ((digest[offset] & 0x7f) << 24) |
    ((digest[offset + 1] & 0xff) << 16) |
    ((digest[offset + 2] & 0xff) << 8) |
    (digest[offset + 3] & 0xff);

  return (binary % 10 ** DIGITS).toString().padStart(DIGITS, "0");
}

export interface TotpVerification {
  valid: boolean;
  /** The step the code belongs to — store it to make the code single-use. */
  step: number | null;
}

/**
 * Check a code against the accepted window.
 *
 * `lastUsedStep` is the replay guard: a code is refused if its step is not
 * strictly newer than the last one this account consumed. Without it a code
 * stays usable for its whole window, so anything that observes it once — a
 * shoulder, a proxy log, a screenshot — can spend it.
 */
export function verifyTotp(
  secret: string,
  code: string,
  lastUsedStep: number | null = null,
  atMs: number = Date.now(),
): TotpVerification {
  const normalized = (code ?? "").replace(/\s+/g, "");
  if (!/^\d{6}$/.test(normalized)) return { valid: false, step: null };

  const current = totpStep(atMs);
  for (let offset = -WINDOW_STEPS; offset <= WINDOW_STEPS; offset++) {
    const step = current + offset;
    if (lastUsedStep !== null && step <= lastUsedStep) continue;

    let expected: string;
    try {
      expected = totpCodeForStep(secret, step);
    } catch {
      return { valid: false, step: null };
    }
    const a = Buffer.from(expected);
    const b = Buffer.from(normalized);
    if (a.length === b.length && timingSafeEqual(a, b)) {
      return { valid: true, step };
    }
  }
  return { valid: false, step: null };
}

/** The otpauth:// URI an authenticator app scans. */
export function totpUri(
  secret: string,
  accountEmail: string,
  issuer = "park.fan",
): string {
  const label = encodeURIComponent(`${issuer}:${accountEmail}`);
  const params = new URLSearchParams({
    secret,
    issuer,
    algorithm: "SHA1",
    digits: String(DIGITS),
    period: String(STEP_SECONDS),
  });
  return `otpauth://totp/${label}?${params.toString()}`;
}
