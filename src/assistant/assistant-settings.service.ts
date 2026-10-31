import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Redis } from "ioredis";
import { Repository } from "typeorm";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { AssistantSettings } from "./entities/assistant-settings.entity";
import {
  maskApiKey,
  openSecret,
  readMasterKey,
  sealSecret,
} from "./assistant-secret.util";

const SETTINGS_ID = 1;
const TOKENS_PREFIX = "assistant:tokens:";
const VISITOR_PREFIX = "assistant:visitor-tokens:";
/** What one session may spend a day: a few dozen chats, a sliver of the budget. */
export const VISITOR_DAILY_TOKENS = 150_000;
const TWO_DAYS_SECONDS = 2 * 24 * 60 * 60;

/** What the admin may change; every field optional. `apiKey: null` removes the key. */
export interface AssistantSettingsUpdate {
  enabled?: boolean;
  rolloutPercent?: number;
  dailyTokenBudget?: number;
  apiKey?: string | null;
}

/** The settings as the admin sees them: the key only masked. */
export interface AssistantSettingsView {
  enabled: boolean;
  rolloutPercent: number;
  dailyTokenBudget: number;
  apiKeyMasked: string | null;
  /** False when `ASSISTANT_SECRET_KEY` is missing: no key can be stored or read. */
  secretConfigured: boolean;
  tokensToday: number;
  updatedBy: string | null;
  updatedAt: Date | null;
}

/** What the widget asks before it shows itself. */
export interface AssistantPublicConfig {
  enabled: boolean;
  rolloutPercent: number;
}

/**
 * Parky's settings row, the stored Claude key and the daily token budget.
 *
 * The budget counts in Redis per UTC day: an instant, not a park's day, so no
 * park timezone applies. A lost counter (eviction, restart) means a day may
 * spend more than its budget, which is bounded by the rate limits per visitor.
 */
@Injectable()
export class AssistantSettingsService {
  private readonly logger = new Logger(AssistantSettingsService.name);

  constructor(
    @InjectRepository(AssistantSettings)
    private readonly repository: Repository<AssistantSettings>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  private masterKey(): Buffer | null {
    return readMasterKey(process.env.ASSISTANT_SECRET_KEY);
  }

  private async row(): Promise<AssistantSettings> {
    const found = await this.repository.findOneBy({ id: SETTINGS_ID });
    return found ?? this.repository.create({ id: SETTINGS_ID });
  }

  async view(): Promise<AssistantSettingsView> {
    const row = await this.row();
    return {
      enabled: row.enabled ?? false,
      rolloutPercent: row.rolloutPercent ?? 0,
      dailyTokenBudget: row.dailyTokenBudget ?? 0,
      apiKeyMasked: row.apiKeyMasked ?? null,
      secretConfigured: this.masterKey() !== null,
      tokensToday: await this.tokensToday(),
      updatedBy: row.updatedBy ?? null,
      updatedAt: row.updatedAt ?? null,
    };
  }

  async update(
    change: AssistantSettingsUpdate,
    updatedBy: string,
  ): Promise<AssistantSettingsView> {
    const row = await this.row();
    if (change.enabled !== undefined) row.enabled = change.enabled;
    if (change.rolloutPercent !== undefined)
      row.rolloutPercent = change.rolloutPercent;
    if (change.dailyTokenBudget !== undefined)
      row.dailyTokenBudget = change.dailyTokenBudget;
    if (change.apiKey === null) {
      row.apiKeySealed = null;
      row.apiKeyMasked = null;
    } else if (change.apiKey !== undefined) {
      const masterKey = this.masterKey();
      if (!masterKey) throw new Error("ASSISTANT_SECRET_KEY is not set");
      row.apiKeySealed = sealSecret(change.apiKey, masterKey);
      row.apiKeyMasked = maskApiKey(change.apiKey);
    }
    row.updatedBy = updatedBy;
    await this.repository.save(row);
    return this.view();
  }

  /** The stored Claude key, or null when none is stored or it cannot be opened. */
  async apiKey(): Promise<string | null> {
    const row = await this.row();
    const masterKey = this.masterKey();
    if (!row.apiKeySealed || !masterKey) return null;
    const key = openSecret(row.apiKeySealed, masterKey);
    if (!key) this.logger.error("Stored Claude key cannot be opened");
    return key;
  }

  /** Off unless switched on, a key is readable and today's budget is not spent. */
  async publicConfig(): Promise<AssistantPublicConfig> {
    const row = await this.row();
    const usable =
      (row.enabled ?? false) &&
      (await this.apiKey()) !== null &&
      !(await this.budgetSpent(row));
    return {
      enabled: usable,
      rolloutPercent: usable ? (row.rolloutPercent ?? 0) : 0,
    };
  }

  async budgetSpent(row?: AssistantSettings): Promise<boolean> {
    const budget = (row ?? (await this.row())).dailyTokenBudget ?? 0;
    return budget > 0 && (await this.tokensToday()) >= budget;
  }

  async addTokens(
    tokens: number,
    session: string | null = null,
  ): Promise<void> {
    if (tokens <= 0) return;
    try {
      const keys = [TOKENS_PREFIX + utcDay()];
      const visitor = visitorBucket(session);
      if (visitor) keys.push(visitor);
      for (const key of keys) {
        await this.redis.incrby(key, tokens);
        await this.redis.expire(key, TWO_DAYS_SECONDS, "NX");
      }
    } catch (error) {
      this.logger.warn(
        `Token counter unavailable: ${(error as Error).message}`,
      );
    }
  }

  /** True when this session has spent its share of today. */
  async visitorOverBudget(session: string | null): Promise<boolean> {
    const key = visitorBucket(session);
    if (!key) return false;
    try {
      return Number((await this.redis.get(key)) ?? 0) >= VISITOR_DAILY_TOKENS;
    } catch {
      return false;
    }
  }

  async tokensToday(): Promise<number> {
    try {
      return Number((await this.redis.get(TOKENS_PREFIX + utcDay())) ?? 0);
    } catch {
      return 0;
    }
  }
}

function visitorBucket(session: string | null): string | null {
  return session ? `${VISITOR_PREFIX}${utcDay()}:${session}` : null;
}

function utcDay(): string {
  return new Date().toISOString().slice(0, 10);
}
