import { Injectable, Logger, OnModuleDestroy } from "@nestjs/common";
import {
  createHash,
  createPrivateKey,
  createPublicKey,
  randomBytes,
  sign,
  type KeyObject,
} from "crypto";
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
 * Same failure rule as VAPID (`push-config.ts`): without a key this is a
 * no-op — device registration answers 503 and nothing is sent — never a
 * crash. The key is the one uploaded in the admin (`ApnsAdminService`), else
 * the `APNS_*` variables.
 */
export type ApnsEnvironment = "production" | "sandbox";

export const APNS_ENVIRONMENTS = ["production", "sandbox"] as const;

export const APNS_HOSTS: Record<ApnsEnvironment, string> = {
  production: "https://api.push.apple.com",
  sandbox: "https://api.sandbox.push.apple.com",
};

export const APNS_DEFAULT_TEAM_ID = "KC5QAXDX94";
export const APNS_DEFAULT_BUNDLE_ID = "fan.park.app";

export interface ApnsConfig {
  keyId: string;
  teamId: string;
  key: KeyObject;
  /** The app's bundle id, sent as `apns-topic`. */
  bundleId: string;
  /** Where the key comes from: uploaded in the admin, or the `APNS_*` variables. */
  source: "database" | "environment";
  /** The first 8 hex characters of the SHA-256 over the public key. */
  fingerprint: string;
  /**
   * The environment tried first for a device whose own is not known yet:
   * production, or the sandbox with `APNS_PRODUCTION=false`.
   */
  first: ApnsEnvironment;
}

/** The key's parts as text, as the admin stores them or the environment holds them. */
export interface ApnsCredentials {
  keyId: string;
  teamId: string;
  bundleId: string;
  pem: string;
}

/** What tells one key from another without showing any of it. */
export function apnsFingerprint(key: KeyObject): string {
  return createHash("sha256")
    .update(createPublicKey(key).export({ type: "spki", format: "der" }))
    .digest("hex")
    .slice(0, 8);
}

type ParsedKey = Pick<ApnsConfig, "key" | "fingerprint">;

function parseKey(pem: string): ParsedKey | null {
  try {
    const key = createPrivateKey(pem);
    return { key, fingerprint: apnsFingerprint(key) };
  } catch {
    // A broken key is the same as none: no 503 loop of failed sends.
    return null;
  }
}

let stored: (ApnsCredentials & ParsedKey) | null = null;
let parsed: { raw: string; key: ParsedKey | null } | null = null;

/**
 * The key uploaded in the admin, which wins over the environment; `null`
 * falls back to it. `ApnsAdminService` sets it at boot and after every upload
 * or removal, and the next send picks it up. False when the key does not parse.
 */
export function setStoredApnsCredentials(
  credentials: ApnsCredentials | null,
): boolean {
  const key = credentials && parseKey(credentials.pem);
  stored = credentials && key ? { ...credentials, ...key } : null;
  return stored !== null;
}

/** The configured key, or `null` when any part is missing or the key does not parse. */
export function getApnsConfig(): ApnsConfig | null {
  const first: ApnsEnvironment =
    (process.env.APNS_PRODUCTION ?? "true").trim() !== "false"
      ? "production"
      : "sandbox";
  if (stored) {
    const { keyId, teamId, bundleId, key, fingerprint } = stored;
    return {
      keyId,
      teamId,
      bundleId,
      key,
      fingerprint,
      source: "database",
      first,
    };
  }

  const keyId = (process.env.APNS_KEY_ID ?? "").trim();
  const teamId = (process.env.APNS_TEAM_ID ?? "").trim();
  const bundleId = (process.env.APNS_BUNDLE_ID ?? "").trim();
  // A PEM pasted into one env line carries `\n` literally.
  const raw = (process.env.APNS_KEY ?? "").trim().replace(/\\n/g, "\n");
  if (!keyId || !teamId || !bundleId || !raw) return null;

  if (parsed?.raw !== raw) parsed = { raw, key: parseKey(raw) };
  if (!parsed.key) return null;
  return {
    keyId,
    teamId,
    bundleId,
    ...parsed.key,
    source: "environment",
    first,
  };
}

export function isApnsConfigured(): boolean {
  return getApnsConfig() !== null;
}

/** The provider token: an ES256 JWT, `kid` the key id, `iss` the team. */
export function apnsJwt(
  config: Pick<ApnsConfig, "keyId" | "teamId" | "key">,
  nowSeconds: number,
): string {
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

/** The `reason` of an APNs error body, or "" for a success or a body that is not one. */
export function apnsReason(body: string): string {
  try {
    return (JSON.parse(body) as { reason?: string }).reason ?? "";
  } catch {
    return "";
  }
}

/**
 * What an APNs answer means for the device row: `dead` deletes it (410, or a
 * token Apple says is not one for this app); anything else is a failure that
 * leaves the row alone.
 */
export function classifyApnsResponse(
  status: number,
  body: string,
): ApnsOutcome {
  return outcomeOf(status, apnsReason(body));
}

function outcomeOf(status: number, reason: string): ApnsOutcome {
  if (status === 200) return "sent";
  if (status === 410) return "dead";
  return status === 400 &&
    (reason === "BadDeviceToken" || reason === "DeviceTokenNotForTopic")
    ? "dead"
    : "failed";
}

/** One answer from Apple; status 0 when there was none (network, timeout, no key). */
export interface ApnsAnswer {
  status: number;
  reason: string;
}

export interface ApnsSendResult extends ApnsAnswer {
  outcome: ApnsOutcome;
  /** The environment whose answer this is; on `sent`, the one the token belongs to. */
  environment: ApnsEnvironment;
}

/** One environment's connection and its circuit breaker. */
interface Lane {
  session: ClientHttp2Session | null;
  unanswered: number;
  openUntilMs: number;
}

/**
 * A request that proves the key without reaching anyone: a background push
 * that would show nothing, to a token no device has.
 */
const CHECK_REQUEST: ApnsRequest = {
  headers: {
    "apns-push-type": "background",
    "apns-priority": "5",
    "apns-expiration": "0",
  },
  body: JSON.stringify({ aps: { "content-available": 1 } }),
};

/** The connections, one per environment, and the cached provider token. */
@Injectable()
export class ApnsClient implements OnModuleDestroy {
  private readonly logger = new Logger(ApnsClient.name);
  private readonly lanes: Record<ApnsEnvironment, Lane> = {
    production: { session: null, unanswered: 0, openUntilMs: 0 },
    sandbox: { session: null, unanswered: 0, openUntilMs: 0 },
  };
  private token: { value: string; at: number } | null = null;
  /** Which key the token and the connections belong to. */
  private identity = "";
  /** Replaced in specs by a fake HTTP/2 client. */
  connectTo: (host: string) => ClientHttp2Session = connect;
  /** Told every final non-200 answer of a send, for the admin's error counts. */
  onError: (environment: ApnsEnvironment, answer: ApnsAnswer) => void =
    () => {};

  /**
   * Apple wants a token refreshed at most every 20 and at least every 60
   * minutes, and reports a newer one on the same connection as an error.
   */
  private static readonly TOKEN_TTL_MS = 50 * 60_000;
  private static readonly TOKEN_MIN_AGE_MS = 20 * 60_000;
  private static readonly TIMEOUT_MS = 10_000;
  /**
   * Circuit breaker, per environment: after this many requests in a row that
   * got no answer (network error, timeout), sends fail at once for
   * `BREAKER_OPEN_MS`. Each would otherwise cost up to `TIMEOUT_MS` inside the
   * five-minute sweep that also serves the web's alerts. An answer of any
   * status, a wrong-environment `BadDeviceToken` included, is not a failure.
   */
  static readonly BREAKER_FAILURES = 5;
  static readonly BREAKER_OPEN_MS = 5 * 60_000;

  /**
   * Send to a device, in the environment its token belongs to.
   *
   * A token only works in the environment that issued it (sandbox for a build
   * from Xcode, production for TestFlight and the App Store) and the other
   * answers `BadDeviceToken`. So that answer is asked again in the other
   * environment, starting with `known` or else `ApnsConfig.first`, and the
   * second answer decides: the caller stores `environment` of a `sent`. A key
   * Apple restricts to one environment (`BadEnvironmentKeyIdInToken` from the
   * other) leaves the first answer standing.
   */
  async send(
    deviceToken: string,
    request: ApnsRequest,
    known: ApnsEnvironment | null = null,
  ): Promise<ApnsSendResult> {
    const config = getApnsConfig();
    if (!config) {
      return {
        outcome: "failed",
        status: 0,
        reason: "NotConfigured",
        environment: known ?? "production",
      };
    }
    let environment = known ?? config.first;
    let answer = await this.attempt(config, environment, deviceToken, request);
    if (answer.status === 400 && answer.reason === "BadDeviceToken") {
      const other = environment === "production" ? "sandbox" : "production";
      const retry = await this.attempt(config, other, deviceToken, request);
      if (retry.reason !== "BadEnvironmentKeyIdInToken") {
        answer = retry;
        environment = other;
      }
    }
    const outcome = outcomeOf(answer.status, answer.reason);
    if (outcome !== "sent") {
      this.onError(environment, answer);
      if (outcome === "failed" && answer.status !== 0) {
        this.logger.warn(
          `APNs (${environment}) answered ${answer.status} ${answer.reason}`,
        );
      }
    }
    return { outcome, environment, ...answer };
  }

  /**
   * Ask Apple whether it accepts the key, without a device: `BadDeviceToken`
   * means the token, team and topic passed. On a connection of its own, since
   * Apple closes one that collects such errors and sends must not pay for it.
   */
  async check(environment: ApnsEnvironment): Promise<ApnsAnswer> {
    const config = getApnsConfig();
    if (!config) return { status: 0, reason: "NotConfigured" };
    this.adopt(config);
    const session = this.connectTo(APNS_HOSTS[environment]);
    session.on("error", () => session.destroy());
    try {
      return await this.post(
        session,
        config,
        randomBytes(32).toString("hex"),
        CHECK_REQUEST,
      );
    } catch {
      return { status: 0, reason: "Unreachable" };
    } finally {
      session.close();
    }
  }

  onModuleDestroy(): void {
    for (const lane of Object.values(this.lanes)) lane.session?.close();
  }

  private async attempt(
    config: ApnsConfig,
    environment: ApnsEnvironment,
    deviceToken: string,
    request: ApnsRequest,
  ): Promise<ApnsAnswer> {
    this.adopt(config);
    const lane = this.lanes[environment];
    if (Date.now() < lane.openUntilMs) {
      return { status: 0, reason: "Unreachable" };
    }
    try {
      const answer = await this.post(
        this.connection(environment),
        config,
        deviceToken,
        request,
      );
      lane.unanswered = 0;
      // A stale provider token is ours to fix, not the device's; a younger
      // one stays, because Apple refuses a new one within 20 minutes.
      if (
        answer.reason === "ExpiredProviderToken" &&
        this.token &&
        Date.now() - this.token.at >= ApnsClient.TOKEN_MIN_AGE_MS
      ) {
        this.token = null;
      }
      return answer;
    } catch (error) {
      this.logger.warn(
        `APNs (${environment}) request failed: ${(error as Error)?.message}`,
      );
      lane.session?.destroy();
      lane.session = null;
      if (++lane.unanswered >= ApnsClient.BREAKER_FAILURES) {
        lane.unanswered = 0;
        lane.openUntilMs = Date.now() + ApnsClient.BREAKER_OPEN_MS;
        this.logger.warn(
          `APNs (${environment}) unreachable ${ApnsClient.BREAKER_FAILURES} times in a row; skipping sends for ${ApnsClient.BREAKER_OPEN_MS / 60_000} min`,
        );
      }
      return { status: 0, reason: "Unreachable" };
    }
  }

  /**
   * A changed key starts over: Apple ties a connection to the key of its
   * first push, so the old connections and the old token go.
   */
  private adopt(config: ApnsConfig): void {
    const identity = `${config.keyId}.${config.teamId}.${config.fingerprint}`;
    if (identity === this.identity) return;
    this.identity = identity;
    this.token = null;
    for (const lane of Object.values(this.lanes)) {
      lane.session?.close();
      lane.session = null;
      lane.unanswered = 0;
      lane.openUntilMs = 0;
    }
  }

  private jwt(config: ApnsConfig): string {
    const now = Date.now();
    if (!this.token || now - this.token.at > ApnsClient.TOKEN_TTL_MS) {
      this.token = { value: apnsJwt(config, Math.floor(now / 1000)), at: now };
    }
    return this.token.value;
  }

  private connection(environment: ApnsEnvironment): ClientHttp2Session {
    const lane = this.lanes[environment];
    if (lane.session && !lane.session.destroyed) return lane.session;
    const session = this.connectTo(APNS_HOSTS[environment]);
    session.on("error", () => session.destroy());
    session.on("goaway", () => session.close());
    session.on("close", () => {
      if (lane.session === session) lane.session = null;
    });
    // Never the reason the process stays up.
    session.unref();
    lane.session = session;
    return session;
  }

  private post(
    session: ClientHttp2Session,
    config: ApnsConfig,
    deviceToken: string,
    request: ApnsRequest,
  ): Promise<ApnsAnswer> {
    return new Promise((resolve, reject) => {
      const stream = session.request({
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
      stream.on("end", () => resolve({ status, reason: apnsReason(body) }));
      stream.on("error", reject);
      stream.end(request.body);
    });
  }
}
