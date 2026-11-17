import { Inject, Injectable, Logger } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import Anthropic from "@anthropic-ai/sdk";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { cachedJson } from "../common/cache/redis-json-cache";
import { parseDateRange } from "../common/utils/date-parsing.util";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import type { CrowdLevel } from "../common/types/crowd-level.type";
import type { Park } from "../parks/entities/park.entity";
import { CalendarService } from "../parks/services/calendar.service";
import { AssistantSettingsService } from "./assistant-settings.service";
import { ASSISTANT_MODEL } from "./assistant-chat.service";
import type { AssistantLocale } from "./assistant-locales";
import {
  PARKY_TIP_MAX_CHARS,
  PARKY_TIP_PROMPT_VERSION,
  parkyTipInput,
  parkyTipSystemPrompt,
} from "./parky-tip-prompt";

/** How far ahead a tip may be asked: the calendar's longest range from today. */
export const PARKY_TIP_HORIZON_DAYS = 90;
/** Neighbouring days on each side the prompt sees. */
const NEIGHBOUR_DAYS = 3;
const CACHE_TTL_SECONDS = 12 * 60 * 60;

export type ParkyVerdict = "good" | "meh" | "bad";

export interface ParkyTip {
  date: string;
  verdict: ParkyVerdict;
  text: string;
  promptVersion: number;
}

const VERDICTS: Partial<Record<CrowdLevel | "closed", ParkyVerdict>> = {
  very_low: "good",
  low: "good",
  moderate: "meh",
  high: "bad",
  very_high: "bad",
  extreme: "bad",
};

/** The stamp beside the tip, from the day's crowd level; null for `closed` and `unknown`. */
export function parkyVerdict(
  level: CrowdLevel | "closed",
): ParkyVerdict | null {
  return VERDICTS[level] ?? null;
}

/** The model's sentence as served, or null when it is not one plain line within the cap. */
export function validParkyTip(raw: string): string | null {
  const text = raw.trim().replace(/^["„“«]+|["“”»]+$/g, "");
  if (!text || /[\r\n]/.test(text)) return null;
  if ([...text].length > PARKY_TIP_MAX_CHARS) return null;
  if (/https?:|www\.|[*_`#[\]<>|]/i.test(text)) return null;
  return text;
}

/**
 * Parky's one-sentence tip for a day of the crowd calendar. One model call per park, park-local
 * today, date, locale and prompt version every 12 hours; a sentence that fails the checks is kept
 * as `false` for as long, so a bad answer is not asked again on every request.
 */
@Injectable()
export class ParkyTipService {
  private readonly logger = new Logger(ParkyTipService.name);

  constructor(
    private readonly moduleRef: ModuleRef,
    private readonly settings: AssistantSettingsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /** Null when there is nothing to say: see the route's 204. */
  async tip(
    park: Park,
    date: string,
    locale: AssistantLocale,
  ): Promise<ParkyTip | null> {
    const today = getCurrentDateInTimezone(park.timezone);
    if (date < today || date > addIsoDays(today, PARKY_TIP_HORIZON_DAYS)) {
      return null;
    }
    // Off, keyless or past the day's budget: no tip, cached ones included.
    if (!(await this.settings.publicConfig()).enabled) return null;
    try {
      const tip = await cachedJson<ParkyTip | false | null>(
        this.redis,
        // Today in the key: the sentence says "tomorrow" or "on Saturday" counted from it.
        `parky-tip:v${PARKY_TIP_PROMPT_VERSION}:${park.id}:${today}:${date}:${locale}`,
        CACHE_TTL_SECONDS,
        () => this.write(park, today, date, locale),
      );
      return tip || null;
    } catch (error) {
      this.logger.warn(
        `Parky tip failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      return null;
    }
  }

  /** `false` is kept in the cache, null is not. */
  private async write(
    park: Park,
    today: string,
    date: string,
    locale: AssistantLocale,
  ): Promise<ParkyTip | false | null> {
    const from = addIsoDays(date, -NEIGHBOUR_DAYS);
    const { fromDate, toDate } = parseDateRange(
      from < today ? today : from,
      addIsoDays(date, NEIGHBOUR_DAYS),
      { timezone: park.timezone },
    );
    const calendar = await this.moduleRef
      .get(CalendarService, { strict: false })
      .buildCalendarResponse(park, fromDate, toDate, "none");
    const day = calendar.days.find((d) => d.date === date);
    const verdict =
      day && day.status !== "CLOSED" ? parkyVerdict(day.crowdLevel) : null;
    if (!verdict) return false;

    const apiKey = await this.settings.apiKey();
    if (!apiKey || (await this.settings.budgetSpent())) return null;
    const client = new Anthropic({ apiKey, maxRetries: 1, timeout: 20_000 });
    const response = await client.messages.create({
      model: ASSISTANT_MODEL,
      max_tokens: 1024,
      output_config: { effort: "low" },
      system: parkyTipSystemPrompt(locale),
      messages: [
        {
          role: "user",
          content: parkyTipInput(
            park.name,
            park.timezone,
            today,
            date,
            calendar.days,
          ),
        },
      ],
    });
    const usage = response.usage;
    await this.settings.addTokens(
      usage.input_tokens +
        usage.output_tokens +
        (usage.cache_creation_input_tokens ?? 0),
    );
    const text =
      response.stop_reason === "end_turn"
        ? validParkyTip(
            response.content
              .map((block) => (block.type === "text" ? block.text : ""))
              .join(""),
          )
        : null;
    if (!text) {
      this.logger.warn(
        `Parky tip refused for ${park.id} ${date} ${locale} (${response.stop_reason})`,
      );
      return false;
    }
    return { date, verdict, text, promptVersion: PARKY_TIP_PROMPT_VERSION };
  }
}
