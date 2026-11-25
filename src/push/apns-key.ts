import { createPrivateKey, createPublicKey, verify } from "crypto";
import {
  apnsFingerprint,
  apnsJwt,
  type ApnsAnswer,
  type ApnsEnvironment,
} from "./apns";

/**
 * The APNs key file as the admin uploads it: reading the key id off its name,
 * refusing everything that is not the key, and Apple's answers in the words
 * the admin shows. Nothing here logs or returns the file's content.
 */

/** A `.p8` is about 260 characters; anything far larger is another file. */
export const APNS_KEY_MAX_CHARS = 4096;

const TEN = /^[A-Z0-9]{10}$/;

/** Apple's key ids and team ids: ten capitals or digits. */
export function isAppleId(value: string): boolean {
  return TEN.test(value);
}

/** The key id of `AuthKey_<KEYID>.p8`, also from a browser's `AuthKey_… (1).p8`. */
export function keyIdFromFilename(filename: string | undefined): string | null {
  return /AuthKey_([A-Z0-9]{10})(?![A-Z0-9])/.exec(filename ?? "")?.[1] ?? null;
}

export type ApnsKeyVerdict =
  | { ok: true; pem: string; fingerprint: string }
  | { ok: false; message: string };

const NOT_THE_KEY =
  "Das ist nicht der APNs-Schlüssel. Gebraucht wird die Datei AuthKey_<Key-ID>.p8 aus dem Apple-Developer-Konto.";

/**
 * The file's PEM block when it is a PKCS#8 EC P-256 private key that signs a
 * provider token, else why not. The message never quotes the content.
 */
export function validateApnsKey(
  content: string,
  filename?: string,
): ApnsKeyVerdict {
  if (content.length > APNS_KEY_MAX_CHARS) {
    return {
      ok: false,
      message: "Die Datei ist zu groß für einen APNs-Schlüssel (.p8).",
    };
  }
  if (
    content.includes("-----BEGIN CERTIFICATE") ||
    /\.(p12|pfx|cer|crt|der)$/i.test(filename ?? "")
  ) {
    return {
      ok: false,
      message:
        "Das ist ein Zertifikat, gebraucht wird der APNs-Schlüssel (.p8).",
    };
  }
  const pem =
    /-----BEGIN PRIVATE KEY-----[A-Za-z0-9+/=\s]+-----END PRIVATE KEY-----/.exec(
      content,
    )?.[0];
  if (!pem) return { ok: false, message: NOT_THE_KEY };
  try {
    const key = createPrivateKey(pem);
    if (
      key.asymmetricKeyType !== "ec" ||
      key.asymmetricKeyDetails?.namedCurve !== "prime256v1"
    ) {
      return { ok: false, message: NOT_THE_KEY };
    }
    const fingerprint = apnsFingerprint(key);
    const [header, claims, signature] = apnsJwt(
      { keyId: "", teamId: "", key },
      0,
    ).split(".");
    const signed = verify(
      "sha256",
      Buffer.from(`${header}.${claims}`),
      { key: createPublicKey(key), dsaEncoding: "ieee-p1363" },
      Buffer.from(signature, "base64url"),
    );
    return signed
      ? { ok: true, pem: `${pem}\n`, fingerprint }
      : { ok: false, message: NOT_THE_KEY };
  } catch {
    return { ok: false, message: NOT_THE_KEY };
  }
}

export type ApnsAnswerState = "ok" | "warning" | "error";

export interface ApnsExplanation {
  state: ApnsAnswerState;
  text: string;
}

/**
 * Apple's answer in plain German. `check` reads the answers to the probe with
 * a token no device has, where `BadDeviceToken` is the good one; `send` reads
 * the answers to a real device.
 */
export function explainApnsAnswer(
  answer: ApnsAnswer,
  context: {
    mode: "check" | "send";
    environment: ApnsEnvironment;
    bundleId: string;
  },
): ApnsExplanation {
  const { status, reason } = answer;
  const error = (text: string): ApnsExplanation => ({ state: "error", text });
  const where = context.environment === "production" ? "Produktion" : "Sandbox";

  if (status === 200) {
    return { state: "ok", text: "Apple hat die Mitteilung angenommen." };
  }
  if (status === 0) {
    return reason === "NotConfigured"
      ? error("Es ist kein APNs-Schlüssel hinterlegt.")
      : error("Apple ist nicht erreichbar. Später erneut prüfen.");
  }
  switch (reason) {
    case "BadDeviceToken":
      return context.mode === "check"
        ? { state: "ok", text: "Schlüssel wird von Apple akzeptiert." }
        : error(
            "Apple kennt das Gerät in keiner der beiden Umgebungen. Es wurde entfernt und meldet sich beim nächsten Start der App neu an.",
          );
    case "InvalidProviderToken":
      return error(
        "Key ID passt nicht zum Schlüssel oder Team, oder der Schlüssel wurde bei Apple widerrufen.",
      );
    case "ExpiredProviderToken":
      return error(
        "Apple hält die Anmeldung für abgelaufen. Die Uhr des Servers geht vermutlich falsch.",
      );
    case "BadEnvironmentKeyIdInToken":
      return {
        state: context.environment === "production" ? "error" : "warning",
        text:
          context.environment === "production"
            ? "Der Schlüssel ist bei Apple nicht für die Produktion freigegeben. Apps aus TestFlight und dem App Store bekommen damit keine Mitteilungen."
            : "Der Schlüssel ist bei Apple nur für die Produktion freigegeben. Apps aus TestFlight und dem App Store bekommen Mitteilungen, aus Xcode installierte nicht.",
      };
    case "TopicDisallowed":
      return error(
        `Push ist für die App-ID ${context.bundleId} nicht aktiviert, oder der Schlüssel gilt nicht für sie.`,
      );
    case "BadTopic":
    case "MissingTopic":
      return error(`Apple kennt die Bundle-ID ${context.bundleId} nicht.`);
    case "DeviceTokenNotForTopic":
      return error(
        `Das Gerät gehört nicht zur App-ID ${context.bundleId}. Es wurde entfernt.`,
      );
    case "Unregistered":
    case "ExpiredToken":
      return error(
        "Die App ist auf dem Gerät nicht mehr für Mitteilungen angemeldet. Es wurde entfernt.",
      );
    case "UnrelatedKeyIdInToken":
    case "Forbidden":
    case "MissingProviderToken":
      return error(`Apple lehnt die Anmeldung ab (${reason}).`);
    case "TooManyProviderTokenUpdates":
    case "TooManyRequests":
      return {
        state: "warning",
        text: "Apple bremst gerade. In ein paar Minuten erneut versuchen.",
      };
  }
  return status >= 500
    ? {
        state: "warning",
        text: `Apple (${where}) hat gerade eine Störung (${status}). Später erneut prüfen.`,
      }
    : error(`Apple antwortet mit ${status} ${reason}`.trim() + ".");
}
