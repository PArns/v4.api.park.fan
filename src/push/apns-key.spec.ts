import { generateKeyPairSync } from "crypto";
import {
  APNS_KEY_MAX_CHARS,
  explainApnsAnswer,
  keyIdFromFilename,
  validateApnsKey,
} from "./apns-key";

const ec = generateKeyPairSync("ec", { namedCurve: "prime256v1" });
const PEM = ec.privateKey.export({ type: "pkcs8", format: "pem" }).toString();

describe("the APNs key file", () => {
  it.each([
    ["AuthKey_ABC123DEFG.p8", "ABC123DEFG"],
    ["AuthKey_ABC123DEFG (1).p8", "ABC123DEFG"],
    ["/Users/x/Downloads/AuthKey_9Z8Y7X6W5V.p8", "9Z8Y7X6W5V"],
    ["AuthKey_abc123defg.p8", null],
    ["AuthKey_ABC123.p8", null],
    ["AuthKey_ABC123DEFGH.p8", null],
    ["key.p8", null],
    [undefined, null],
  ])("reads the key id of %s", (filename, keyId) => {
    expect(keyIdFromFilename(filename)).toBe(keyId);
  });

  it("accepts a PKCS#8 P-256 key, also with Windows line ends and text around it", () => {
    const plain = validateApnsKey(PEM, "AuthKey_ABC123DEFG.p8");
    expect(plain).toMatchObject({ ok: true });
    const wrapped = validateApnsKey(`\n${PEM.replace(/\n/g, "\r\n")}\n\n`);
    expect(wrapped).toMatchObject({
      ok: true,
      fingerprint: plain.ok && plain.fingerprint,
    });
    expect(plain.ok && plain.fingerprint).toMatch(/^[0-9a-f]{8}$/);
  });

  const rsa = generateKeyPairSync("rsa", { modulusLength: 2048 });
  const p384 = generateKeyPairSync("ec", { namedCurve: "secp384r1" });
  it.each([
    ["an RSA key", rsa.privateKey.export({ type: "pkcs8", format: "pem" })],
    ["a P-384 key", p384.privateKey.export({ type: "pkcs8", format: "pem" })],
    ["a public key", ec.publicKey.export({ type: "spki", format: "pem" })],
    ["a SEC1 key", ec.privateKey.export({ type: "sec1", format: "pem" })],
    ["garbage", "hello"],
    [
      "a broken body",
      "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----",
    ],
    ["nothing", ""],
  ])("refuses %s without repeating it", (_name, content) => {
    const verdict = validateApnsKey(content.toString());
    expect(verdict.ok).toBe(false);
    expect(!verdict.ok && verdict.message).toContain("AuthKey_<Key-ID>.p8");
  });

  it.each([
    ["-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----", "a.pem"],
    ["\u0000\u0001binary", "Certificates.p12"],
    ["\u0000\u0001binary", "aps.cer"],
  ])("says a certificate is not the key", (content, filename) => {
    expect(validateApnsKey(content, filename)).toEqual({
      ok: false,
      message:
        "Das ist ein Zertifikat, gebraucht wird der APNs-Schlüssel (.p8).",
    });
  });

  it("refuses an oversized file before reading it", () => {
    const verdict = validateApnsKey(PEM + " ".repeat(APNS_KEY_MAX_CHARS));
    expect(verdict).toMatchObject({ ok: false });
    expect(!verdict.ok && verdict.message).toContain("zu groß");
  });
});

describe("Apple's answers in German", () => {
  const explain = (
    status: number,
    reason: string,
    mode: "check" | "send" = "check",
    environment: "production" | "sandbox" = "production",
  ) =>
    explainApnsAnswer(
      { status, reason },
      { mode, environment, bundleId: "fan.park.app" },
    );

  it("reads the probe's BadDeviceToken as an accepted key", () => {
    expect(explain(400, "BadDeviceToken")).toEqual({
      state: "ok",
      text: "Schlüssel wird von Apple akzeptiert.",
    });
    expect(explain(400, "BadDeviceToken", "send").state).toBe("error");
  });

  it.each([
    [403, "InvalidProviderToken", "Key ID passt nicht zum Schlüssel oder Team"],
    [400, "TopicDisallowed", "Push ist für die App-ID fan.park.app nicht"],
    [400, "BadTopic", "Bundle-ID fan.park.app"],
    [403, "ExpiredProviderToken", "Uhr des Servers"],
    [403, "Forbidden", "(Forbidden)"],
    [410, "Unregistered", "nicht mehr für Mitteilungen angemeldet"],
    [400, "DeviceTokenNotForTopic", "gehört nicht zur App-ID"],
    [0, "Unreachable", "nicht erreichbar"],
    [0, "NotConfigured", "kein APNs-Schlüssel"],
    [418, "Teapot", "418 Teapot"],
  ])("%i %s is an error that says why", (status, reason, part) => {
    const answer = explain(status, reason, "send");
    expect(answer.state).toBe("error");
    expect(answer.text).toContain(part);
  });

  it("tells a key restricted to one environment apart by where it fails", () => {
    expect(explain(403, "BadEnvironmentKeyIdInToken").state).toBe("error");
    const sandbox = explain(
      403,
      "BadEnvironmentKeyIdInToken",
      "check",
      "sandbox",
    );
    expect(sandbox.state).toBe("warning");
    expect(sandbox.text).toContain("nur für die Produktion");
  });

  it("calls Apple's own trouble a warning, and a sent one sent", () => {
    expect(explain(503, "ServiceUnavailable").state).toBe("warning");
    expect(explain(429, "TooManyRequests").state).toBe("warning");
    expect(explain(200, "", "send")).toEqual({
      state: "ok",
      text: "Apple hat die Mitteilung angenommen.",
    });
  });
});
