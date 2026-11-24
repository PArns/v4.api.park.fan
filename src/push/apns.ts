import { Injectable, Logger, OnModuleDestroy } from "@nestjs/common";
import { createHash, createPrivateKey, sign, type KeyObject } from "crypto";
import { connect, type ClientHttp2Session } from "http2";
import type { PushMessage } from "./push.service";

/**
 * Apple Push Notification service, for the iOS app.
 *
 * Token-based auth (a `.p8` key) over HTTP/2, written on `node:http2` and
 * `node:crypto` rather than a package: the whole protocol is one ES256 JWT and
 * one POST per notification, and the maintained libraries (`@parse/node-apn`,
 * `apns2`) bring a JWT library and their own connection handling for those
 * ~100 lines.
 *
 * Same failure rule as VAPID (`push-config.ts`): without the env vars this is
 * a no-op — device registration answers 503 and nothing is sent — never a
 * crash.
 */
export interface ApnsConfig {
  keyId: string;
  teamId: string;
  key: KeyObject;
  /** The app's bundle id, sent as `apns-topic`. */
  bundleId: string;
  /** `api.push.apple.com`, or the sandbox for development builds. */
  host: string;
}

let parsed: { raw: string; key: KeyObject | null } | null = null;

/** The configured key, or `null` when any part is missing or the key does not parse. */
export function getApnsConfig(): ApnsConfig | null {
  const keyId = (process.env.APNS_KEY_ID ?? "").trim();
  const teamId = (process.env.APNS_TEAM_ID ?? "").trim();
  const bundleId = (process.env.APNS_BUNDLE_ID ?? "").trim();
  // A PEM pasted into one env line carries `\n` literally.
  const raw = (process.env.APNS_KEY ?? "").trim().replace(/\\n/g, "\n");
  if (!keyId || !teamId || !bundleId || !raw) return null;

  if (parsed?.raw !== raw) {
    let key: KeyObject | null = null;
    try {
      key = createPrivateKey(raw);
    } catch {
      // A broken key is the same as none: no 503 loop of failed sends.
    }
    parsed = { raw, key };
  }
  if (!parsed.key) return null;

  const production = (process.env.APNS_PRODUCTION ?? "true").trim() !== "false";
  return {
    keyId,
    teamId,
    key: parsed.key,
    bundleId,
    host: production
      ? "https://api.push.apple.com"
      : "https://api.sandbox.push.apple.com",
  };
}

export function isApnsConfigured(): boolean {
  return getApnsConfig() !== null;
}

/** The provider token: an ES256 JWT, `kid` the key id, `iss` the team. */
export function apnsJwt(config: ApnsConfig, nowSeconds: number): string {
  const part = (value: object) =>
    Buffer.from(JSON.stringify(value)).toString("base64url");
  const input = `${part({ alg: "ES256", kid: config.keyId })}.${part({ iss: config.teamId, iat: nowSeconds })}`;
  const signature = sign("sha256", Buffer.from(input), {
    key: config.key,
    dsaEncoding: "ieee-p1363",
  }).toString("base64url");
  return `${input}.${signature}`;
}

export interface ApnsRequest {
  headers: Record<string, string>;
  body: string;
}

/**
 * One notification as APNs wants it. `url` and `kind` sit beside `aps`: the
 * app routes a tap by `url` (the same site-relative path the web opens).
 */
export function buildApnsRequest(
  message: PushMessage,
  kind: string,
  nowMs: number,
): ApnsRequest {
  // Apple caps the collapse id at 64 bytes; a sha256 hex is exactly that.
  const collapseId =
    Buffer.byteLength(message.tag) <= 64
      ? message.tag
      : createHash("sha256").update(message.tag).digest("hex");
  return {
    headers: {
      "apns-push-type": "alert",
      "apns-priority": "10",
      // The web's TTL: a notification about the next minutes is worthless later.
      "apns-expiration": String(Math.floor(nowMs / 1000) + 900),
      "apns-collapse-id": collapseId,
    },
    body: JSON.stringify({
      aps: {
        alert: { title: message.title, body: message.body },
        sound: "default",
        "thread-id": kind,
      },
      url: message.url,
      kind,
    }),
  };
}

export type ApnsOutcome = "sent" | "dead" | "failed";

/**
 * What an APNs answer means for the device row: `dead` deletes it (410, or a
 * token Apple says is not one for this app), anything else counts as a failure.
 */
export function classifyApnsResponse(
  status: number,
  body: string,
): ApnsOutcome {
  if (status === 200) return "sent";
  if (status === 410) return "dead";
  let reason = "";
  try {
    reason = (JSON.parse(body) as { reason?: string }).reason ?? "";
  } catch {
    // An empty or non-JSON body is just a failure.
  }
  return status === 400 &&
    (reason === "BadDeviceToken" || reason === "DeviceTokenNotForTopic")
    ? "dead"
    : "failed";
}

/** The connection and the cached provider token. */
@Injectable()
export class ApnsClient implements OnModuleDestroy {
  private readonly logger = new Logger(ApnsClient.name);
  private session: ClientHttp2Session | null = null;
  private sessionHost = "";
  private token: { value: string; at: number; keyId: string } | null = null;

  /** Apple wants a token refreshed at most every 20 and at least every 60 minutes. */
  private static readonly TOKEN_TTL_MS = 50 * 60_000;
  private static readonly TIMEOUT_MS = 10_000;

  async send(deviceToken: string, request: ApnsRequest): Promise<ApnsOutcome> {
    const config = getApnsConfig();
    if (!config) return "failed";
    try {
      const { status, body } = await this.post(config, deviceToken, request);
      // A rejected provider token is ours to fix, not the device's.
      if (status === 403) this.token = null;
      const outcome = classifyApnsResponse(status, body);
      if (outcome === "failed") {
        this.logger.warn(`APNs answered ${status}: ${body.slice(0, 200)}`);
      }
      return outcome;
    } catch (error) {
      this.logger.warn(`APNs request failed: ${(error as Error)?.message}`);
      this.session?.destroy();
      this.session = null;
      return "failed";
    }
  }

  onModuleDestroy(): void {
    this.session?.close();
  }

  private jwt(config: ApnsConfig): string {
    const now = Date.now();
    if (
      !this.token ||
      this.token.keyId !== config.keyId ||
      now - this.token.at > ApnsClient.TOKEN_TTL_MS
    ) {
      this.token = {
        value: apnsJwt(config, Math.floor(now / 1000)),
        at: now,
        keyId: config.keyId,
      };
    }
    return this.token.value;
  }

  private connection(host: string): ClientHttp2Session {
    if (this.session && !this.session.destroyed && this.sessionHost === host) {
      return this.session;
    }
    this.session?.close();
    const session = connect(host);
    session.on("error", () => session.destroy());
    session.on("goaway", () => session.close());
    session.on("close", () => {
      if (this.session === session) this.session = null;
    });
    // Never the reason the process stays up.
    session.unref();
    this.session = session;
    this.sessionHost = host;
    return session;
  }

  private post(
    config: ApnsConfig,
    deviceToken: string,
    request: ApnsRequest,
  ): Promise<{ status: number; body: string }> {
    return new Promise((resolve, reject) => {
      const stream = this.connection(config.host).request({
        ":method": "POST",
        ":path": `/3/device/${deviceToken}`,
        authorization: `bearer ${this.jwt(config)}`,
        "apns-topic": config.bundleId,
        "content-type": "application/json",
        ...request.headers,
      });
      let status = 0;
      let body = "";
      stream.setEncoding("utf8");
      stream.setTimeout(ApnsClient.TIMEOUT_MS, () =>
        stream.close(undefined, () => reject(new Error("APNs timeout"))),
      );
      stream.on("response", (headers) => (status = Number(headers[":status"])));
      stream.on("data", (chunk: string) => (body += chunk));
      stream.on("end", () => resolve({ status, body }));
      stream.on("error", reject);
      stream.end(request.body);
    });
  }
}
