import {
  HttpException,
  HttpStatus,
  Inject,
  Injectable,
  Logger,
  OnModuleInit,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Redis } from "ioredis";
import { IsNull, Not, Repository } from "typeorm";
import {
  openSecret,
  readMasterKey,
  sealSecret,
} from "../assistant/assistant-secret.util";
import { checkRateLimit } from "../common/redis/rate-limit.util";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import {
  APNS_DEFAULT_BUNDLE_ID,
  APNS_DEFAULT_TEAM_ID,
  APNS_ENVIRONMENTS,
  ApnsClient,
  getApnsConfig,
  setStoredApnsCredentials,
  type ApnsEnvironment,
} from "./apns";
import {
  explainApnsAnswer,
  isAppleId,
  keyIdFromFilename,
  validateApnsKey,
  type ApnsExplanation,
} from "./apns-key";
import {
  ApnsSettings,
  type ApnsCheckResult,
} from "./entities/apns-settings.entity";
import { PushSubscription } from "./entities/push-subscription.entity";
import { PushService } from "./push.service";

const SETTINGS_ID = 1;
const ERRORS_PREFIX = "push:apns:errors:";
const ERROR_HOURS = 24;
const DEVICE_LIST_MAX = 50;

/** What the admin sends with the `.p8` file; the ids fall back to the file name and the app's own. */
export interface ApnsKeyUpload {
  content: string;
  filename?: string;
  keyId?: string;
  teamId?: string;
  bundleId?: string;
}

/** The APNs setup as the admin sees it: never the key, only what tells it apart. */
export interface ApnsStatusView {
  configured: boolean;
  source: "database" | "environment" | null;
  keyId: string | null;
  teamId: string | null;
  bundleId: string | null;
  fingerprint: string | null;
  /** Set for an uploaded key only. */
  uploadedBy: string | null;
  uploadedAt: Date | null;
  /** False when `ASSISTANT_SECRET_KEY` is missing: no key can be stored or read. */
  secretConfigured: boolean;
  /** True when a key is stored that this server's secret cannot open. */
  storedKeyUnreadable: boolean;
  defaults: { teamId: string; bundleId: string };
  checks: Record<ApnsEnvironment, ApnsCheckResult | null>;
  devices: {
    total: number;
    byEnvironment: Record<ApnsEnvironment | "unknown", number>;
    byKind: Record<string, number>;
  };
  /** Sends Apple refused in the last 24 hours, most frequent first. */
  errors: { environment: string; reason: string; count: number }[];
}

/** One registered app install in the admin's list; of the token only its end. */
export interface ApnsDeviceView {
  id: string;
  tokenTail: string;
  kinds: string[];
  locale: string;
  timezone: string | null;
  environment: ApnsEnvironment | null;
  registeredAt: Date;
  lastNotifiedAt: Date | null;
  lastError: string | null;
}

function refuse(message: string, status = HttpStatus.BAD_REQUEST): never {
  throw new HttpException(message, status);
}

/**
 * The APNs key in the admin: upload, removal, the check against Apple and
 * what the status page shows.
 *
 * The key's PEM is sealed exactly as Parky's Claude key is
 * (`assistant-secret.util.ts`, master key `ASSISTANT_SECRET_KEY`) and handed
 * to the transport through `setStoredApnsCredentials`, at boot and after every
 * change, so no restart is needed.
 *
 * shortcut: the handover is in this process only, which is the one API
 * container; a second instance needs a reload on a timer.
 */
@Injectable()
export class ApnsAdminService implements OnModuleInit {
  private readonly logger = new Logger(ApnsAdminService.name);

  constructor(
    @InjectRepository(ApnsSettings)
    private readonly settings: Repository<ApnsSettings>,
    @InjectRepository(PushSubscription)
    private readonly subscriptions: Repository<PushSubscription>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly apns: ApnsClient,
    private readonly push: PushService,
  ) {
    this.apns.onError = (environment, answer) =>
      void this.countError(environment, answer.reason || `${answer.status}`);
  }

  /** A database that is down at boot leaves the environment's key in charge. */
  async onModuleInit(): Promise<void> {
    try {
      await this.load();
    } catch (error) {
      this.logger.error(
        `Stored APNs key not loaded: ${(error as Error).message}`,
      );
    }
  }

  private masterKey(): Buffer | null {
    return readMasterKey(process.env.ASSISTANT_SECRET_KEY);
  }

  private async row(): Promise<ApnsSettings> {
    return (
      (await this.settings.findOneBy({ id: SETTINGS_ID })) ??
      this.settings.create({ id: SETTINGS_ID })
    );
  }

  /** Hand the stored key to the transport; true when it is in use. */
  private async load(row?: ApnsSettings): Promise<boolean> {
    const { keySealed, keyId, teamId, bundleId } = row ?? (await this.row());
    const masterKey = this.masterKey();
    const pem =
      keySealed && masterKey ? openSecret(keySealed, masterKey) : null;
    if (keySealed && !pem) {
      this.logger.error("Stored APNs key cannot be opened");
    }
    return setStoredApnsCredentials(
      pem && keyId && teamId && bundleId
        ? { pem, keyId, teamId, bundleId }
        : null,
    );
  }

  /**
   * Store the uploaded key and start using it. Refused without storing
   * anything when the file is not an APNs key, the key id cannot be told, or
   * the server has no secret to seal it with.
   */
  async upload(
    input: ApnsKeyUpload,
    uploadedBy: string,
  ): Promise<ApnsStatusView> {
    const masterKey = this.masterKey();
    if (!masterKey) {
      refuse(
        "ASSISTANT_SECRET_KEY ist auf der API nicht gesetzt. Ohne dieses Server-Geheimnis kann kein Schlüssel gespeichert werden.",
        HttpStatus.CONFLICT,
      );
    }
    const keyId = input.keyId || keyIdFromFilename(input.filename);
    if (!keyId) {
      refuse(
        "Die Key ID lässt sich nicht aus dem Dateinamen lesen. Bitte die Key ID angeben: zehn Zeichen, Großbuchstaben und Ziffern, wie im Namen AuthKey_<Key-ID>.p8.",
      );
    }
    if (!isAppleId(keyId)) {
      refuse("Die Key ID besteht aus zehn Großbuchstaben und Ziffern.");
    }
    const teamId = input.teamId || APNS_DEFAULT_TEAM_ID;
    if (!isAppleId(teamId)) {
      refuse("Die Team ID besteht aus zehn Großbuchstaben und Ziffern.");
    }
    const bundleId = input.bundleId || APNS_DEFAULT_BUNDLE_ID;
    if (!/^[A-Za-z0-9.-]{1,155}$/.test(bundleId)) {
      refuse("Die Bundle-ID sieht nicht wie eine aus, etwa fan.park.app.");
    }
    const verdict = validateApnsKey(input.content, input.filename);
    if (!verdict.ok) refuse(verdict.message);

    const row = await this.row();
    row.keySealed = sealSecret(verdict.pem, masterKey);
    row.keyId = keyId;
    row.teamId = teamId;
    row.bundleId = bundleId;
    row.fingerprint = verdict.fingerprint;
    row.uploadedBy = uploadedBy;
    row.uploadedAt = new Date();
    row.checks = null;
    await this.load(await this.settings.save(row));
    await this.check();
    return this.status();
  }

  /** Delete the stored key; the environment's key takes over, if there is one. */
  async remove(): Promise<ApnsStatusView> {
    const row = await this.row();
    row.keySealed = null;
    row.keyId = null;
    row.teamId = null;
    row.bundleId = null;
    row.fingerprint = null;
    row.uploadedBy = null;
    row.uploadedAt = null;
    row.checks = null;
    await this.settings.save(row);
    setStoredApnsCredentials(null);
    return this.status();
  }

  /** Ask Apple in both environments whether it accepts the key in use, and keep the answers. */
  async check(): Promise<Record<ApnsEnvironment, ApnsCheckResult | null>> {
    const config = getApnsConfig();
    if (!config) return { production: null, sandbox: null };
    const at = new Date().toISOString();
    const [production, sandbox] = await Promise.all(
      APNS_ENVIRONMENTS.map(async (environment) => {
        const answer = await this.apns.check(environment);
        return {
          at,
          ...answer,
          ...explainApnsAnswer(answer, {
            mode: "check",
            environment,
            bundleId: config.bundleId,
          }),
          fingerprint: config.fingerprint,
        };
      }),
    );
    const checks = { production, sandbox };
    const row = await this.row();
    row.checks = checks;
    await this.settings.save(row);
    return checks;
  }

  async status(): Promise<ApnsStatusView> {
    const row = await this.row();
    const config = getApnsConfig();
    const fromDatabase = config?.source === "database";
    // A check belongs to the key it was made with.
    const checkOf = (environment: ApnsEnvironment) => {
      const check = row.checks?.[environment];
      return check && check.fingerprint === config?.fingerprint ? check : null;
    };
    const devices = await this.subscriptions.find({
      where: { apnsToken: Not(IsNull()) },
      select: { id: true, topics: true, apnsEnvironment: true },
    });
    const byEnvironment = { production: 0, sandbox: 0, unknown: 0 };
    const byKind: Record<string, number> = {};
    for (const device of devices) {
      byEnvironment[device.apnsEnvironment ?? "unknown"]++;
      for (const kind of device.topics ?? []) {
        byKind[kind] = (byKind[kind] ?? 0) + 1;
      }
    }
    return {
      configured: config !== null,
      source: config?.source ?? null,
      keyId: config?.keyId ?? null,
      teamId: config?.teamId ?? null,
      bundleId: config?.bundleId ?? null,
      fingerprint: config?.fingerprint ?? null,
      uploadedBy: fromDatabase ? row.uploadedBy : null,
      uploadedAt: fromDatabase ? row.uploadedAt : null,
      secretConfigured: this.masterKey() !== null,
      storedKeyUnreadable: Boolean(row.keySealed) && !fromDatabase,
      defaults: {
        teamId: APNS_DEFAULT_TEAM_ID,
        bundleId: APNS_DEFAULT_BUNDLE_ID,
      },
      checks: {
        production: checkOf("production"),
        sandbox: checkOf("sandbox"),
      },
      devices: { total: devices.length, byEnvironment, byKind },
      errors: await this.errors(),
    };
  }

  /** The app installs registered last, newest first. */
  async devices(): Promise<ApnsDeviceView[]> {
    const rows = await this.subscriptions.find({
      where: { apnsToken: Not(IsNull()) },
      order: { createdAt: "DESC" },
      take: DEVICE_LIST_MAX,
    });
    return rows.map((row) => ({
      id: row.id,
      tokenTail: (row.apnsToken ?? "").slice(-6),
      kinds: row.topics ?? [],
      locale: row.locale,
      timezone: row.timezone ?? null,
      environment: row.apnsEnvironment ?? null,
      registeredAt: row.createdAt,
      lastNotifiedAt: row.lastNotifiedAt ?? null,
      lastError: row.apnsLastError ?? null,
    }));
  }

  /** Send a test notification to one device and say what Apple answered. */
  async test(
    deviceId: string,
  ): Promise<
    ApnsExplanation & { status: number; reason: string; environment: string }
  > {
    const config = getApnsConfig();
    if (!config) refuse("Es ist kein APNs-Schlüssel hinterlegt.", 409);
    const result = await this.push.sendTest(deviceId);
    if (!result) refuse("Dieses Gerät ist nicht mehr angemeldet.", 404);
    return {
      status: result.status,
      reason: result.reason,
      environment: result.environment,
      ...explainApnsAnswer(result, {
        mode: "send",
        environment: result.environment,
        bundleId: config.bundleId,
      }),
    };
  }

  /** The checks and test sends one account may trigger: each one is a request to Apple. */
  async limit(accountKey: string): Promise<void> {
    const verdict = await checkRateLimit({
      redis: this.redis,
      logger: this.logger,
      label: "APNs admin limiter",
      key: `push:apns:admin:${accountKey}`,
      max: 20,
      windowSeconds: 10 * 60,
    });
    if (!verdict.allowed) {
      refuse(
        `Zu viele Anfragen an Apple. In ${Math.ceil(verdict.retryAfterSeconds / 60)} Minuten erneut versuchen.`,
        HttpStatus.TOO_MANY_REQUESTS,
      );
    }
  }

  /**
   * One hash per UTC hour, `<environment>:<reason>` → count, kept a day. In
   * Redis like the other counters here: a lost one costs a number on a status
   * page.
   */
  private async countError(
    environment: ApnsEnvironment,
    reason: string,
  ): Promise<void> {
    try {
      const key = ERRORS_PREFIX + hourKey(Date.now());
      await this.redis.hincrby(key, `${environment}:${reason}`, 1);
      await this.redis.expire(key, (ERROR_HOURS + 1) * 3600, "NX");
    } catch {
      // The count is a diagnostic; a send never waits on it.
    }
  }

  private async errors(): Promise<ApnsStatusView["errors"]> {
    const counts = new Map<string, number>();
    try {
      const now = Date.now();
      for (let hour = 0; hour < ERROR_HOURS; hour++) {
        const hash = await this.redis.hgetall(
          ERRORS_PREFIX + hourKey(now - hour * 3_600_000),
        );
        for (const [field, count] of Object.entries(hash ?? {})) {
          counts.set(field, (counts.get(field) ?? 0) + Number(count));
        }
      }
    } catch {
      return [];
    }
    return [...counts]
      .map(([field, count]) => {
        const [environment, reason] = field.split(":");
        return { environment, reason, count };
      })
      .sort((a, b) => b.count - a.count);
  }
}

function hourKey(ms: number): string {
  return new Date(ms).toISOString().slice(0, 13);
}
