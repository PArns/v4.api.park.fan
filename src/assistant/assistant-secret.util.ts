import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

/**
 * The Claude key entered in the admin, sealed with AES-256-GCM.
 *
 * Unlike an admin token, which is only ever compared and so is kept as a
 * hash, this key has to be read back to call Anthropic. The master key lives
 * in the environment (`ASSISTANT_SECRET_KEY`), the ciphertext in Postgres, so
 * a database dump alone does not hand out the key.
 */

const ALGORITHM = "aes-256-gcm";
const IV_BYTES = 12;
const VERSION = "v1";

/** The 32-byte master key, from base64 or hex; null when unset or malformed. */
export function readMasterKey(raw: string | undefined): Buffer | null {
  const value = raw?.trim();
  if (!value) return null;
  const key = /^[0-9a-f]{64}$/i.test(value)
    ? Buffer.from(value, "hex")
    : Buffer.from(value, "base64");
  return key.length === 32 ? key : null;
}

/** `v1.<iv>.<tag>.<ciphertext>`, each part base64url. */
export function sealSecret(plain: string, masterKey: Buffer): string {
  const iv = randomBytes(IV_BYTES);
  const cipher = createCipheriv(ALGORITHM, masterKey, iv);
  const data = Buffer.concat([cipher.update(plain, "utf8"), cipher.final()]);
  return [VERSION, iv, cipher.getAuthTag(), data]
    .map((part) =>
      typeof part === "string" ? part : part.toString("base64url"),
    )
    .join(".");
}

/** The plain secret, or null when the text was sealed with another key or changed. */
export function openSecret(sealed: string, masterKey: Buffer): string | null {
  const [version, iv, tag, data] = sealed.split(".");
  if (version !== VERSION || !iv || !tag || !data) return null;
  try {
    const decipher = createDecipheriv(
      ALGORITHM,
      masterKey,
      Buffer.from(iv, "base64url"),
    );
    decipher.setAuthTag(Buffer.from(tag, "base64url"));
    return Buffer.concat([
      decipher.update(Buffer.from(data, "base64url")),
      decipher.final(),
    ]).toString("utf8");
  } catch {
    return null;
  }
}

/** What the admin shows of a stored key: its kind and last four characters. */
export function maskApiKey(key: string): string {
  return `${key.slice(0, 7)}…${key.slice(-4)}`;
}
