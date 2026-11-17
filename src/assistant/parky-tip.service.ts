import { createHash } from "node:crypto";
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
import type { CalendarDay } from "../parks/dto/integrated-calendar.dto";
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
  /** The day's crowd level the sentence was written for; a level that moves gets a new one. */
  crowdLevel: CrowdLevel;
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

/** What Parky writes about, and the stamp he gives it. */
interface DayRating {
  crowdLevel: CrowdLevel;
  verdict: ParkyVerdict;
}

/** A day's crowd level and Parky's verdict on it; null for a closed or unrated day. */
function rateDay(
  day: Pick<CalendarDay, "status" | "crowdLevel"> | undefined,
): DayRating | null {
  if (!day || day.status === "CLOSED" || day.crowdLevel === "closed") {
    return null;
  }
  const verdict = parkyVerdict(day.crowdLevel);
  return verdict ? { crowdLevel: day.crowdLevel, verdict } : null;
}

/** Whether a tip is written for `date`: from the park's today to the horizon. */
function inHorizon(today: string, date: string): boolean {
  return date >= today && date <= addIsoDays(today, PARKY_TIP_HORIZON_DAYS);
}

/**
 * What the sentence may say about each day it reads: the day's status and crowd level, and on
 * today the level so far. The sentence names a quieter neighbour, so a neighbour that moves must
 * move the key as well as the chosen day. Weather stays out: it changes with every forecast run,
 * and a sentence written again for each cloud would cost a model call each time.
 */
function daysDigest(days: CalendarDay[]): string {
  const facts = days
    .map(
      (day) =>
        `${day.date} ${day.status} ${day.crowdLevel} ${day.todayCrowdLevel ?? "-"}`,
    )
    .join("\n");
  return createHash("sha256").update(facts).digest("base64url").slice(0, 16);
}

/**
 * Where a tip is kept. Today is in it: the sentence says "tomorrow" or "on Saturday" counted from
 * it. The days it reads too (`daysDigest`): once the forecast moves one of them, the sentence is
 * written again instead of standing for twelve hours over days that say something else.
 */
function tipKey(
  parkId: string,
  today: string,
  date: string,
  days: CalendarDay[],
  locale: AssistantLocale,
): string {
  return `parky-tip:v${PARKY_TIP_PROMPT_VERSION}:${parkId}:${today}:${date}:${daysDigest(days)}:${locale}`;
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
 * today, date, the levels of the days it reads, locale and prompt version every 12 hours; a
 * sentence that fails the checks is kept as `false` for as long, so a bad answer is not asked
 * again on every request.
 */
@Injectable()
export class ParkyTipService {
  private readonly logger = new Logger(ParkyTipService.name);

  constructor(
    private readonly moduleRef: ModuleRef,
    private readonly settings: AssistantSettingsService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /**
   * Null when there is nothing to say: see the route's 204. `today` is the park's, read once by
   * the caller that also compares it with the one the request names.
   */
  async tip(
    park: Park,
    date: string,
    locale: AssistantLocale,
    today: string = getCurrentDateInTimezone(park.timezone),
  ): Promise<ParkyTip | null> {
    if (!inHorizon(today, date)) return null;
    return this.served(park, today, date, locale);
  }

  /**
   * What the chat gets when a visitor taps the tip: the sentence served now in `locale`, read
   * through the cache like any request, and the day's facts as the tip reads them
   * (`parkyTipInput`), from one read of the calendar. The sentence is the server's, never the one
   * the browser says it showed; null where no tip is served now. Null outside the days a tip is
   * written for.
   */
  async dayTip(
    park: Park,
    date: string,
    locale: AssistantLocale,
  ): Promise<{ text: string | null; facts: string } | null> {
    const today = getCurrentDateInTimezone(park.timezone);
    if (!inHorizon(today, date)) return null;
    const days = await this.daysAround(park, today, date);
    const tip = await this.served(park, today, date, locale, days);
    return {
      text: tip?.text ?? null,
      facts: parkyTipInput(park.name, park.timezone, today, date, days),
    };
  }

  /**
   * The tip kept for these days, written on a miss. The days are read before the cache because
   * they are part of the key. They come from the month caches the calendar serves
   * (`calendar:month:v2`), which the warmup keeps for this horizon, so a tip already written costs
   * one MGET of one or two months; a current month past its two warm hours is rebuilt first, as
   * for any calendar read.
   */
  private async served(
    park: Park,
    today: string,
    date: string,
    locale: AssistantLocale,
    days?: CalendarDay[],
  ): Promise<ParkyTip | null> {
    // Off, keyless or past the day's budget: no tip, cached ones included.
    if (!(await this.settings.publicConfig()).enabled) return null;
    try {
      const around = days ?? (await this.daysAround(park, today, date));
      const rating = rateDay(around.find((d) => d.date === date));
      if (!rating) return null;
      const tip = await cachedJson<ParkyTip | false | null>(
        this.redis,
        tipKey(park.id, today, date, around, locale),
        CACHE_TTL_SECONDS,
        () => this.write(park, today, date, locale, rating, around),
      );
      return tip || null;
    } catch (error) {
      this.logger.warn(
        `Parky tip failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      return null;
    }
  }

  /** The calendar's days around `date` that the prompt sees, from today on. */
  private async daysAround(
    park: Park,
    today: string,
    date: string,
  ): Promise<CalendarDay[]> {
    const from = addIsoDays(date, -NEIGHBOUR_DAYS);
    const { fromDate, toDate } = parseDateRange(
      from < today ? today : from,
      addIsoDays(date, NEIGHBOUR_DAYS),
      { timezone: park.timezone },
    );
    const calendar = await this.moduleRef
      .get(CalendarService, { strict: false })
      .buildCalendarResponse(park, fromDate, toDate, "none");
    return calendar.days;
  }

  /** `false` is kept in the cache, null is not. */
  private async write(
    park: Park,
    today: string,
    date: string,
    locale: AssistantLocale,
    rating: DayRating,
    days: CalendarDay[],
  ): Promise<ParkyTip | false | null> {
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
          content: parkyTipInput(park.name, park.timezone, today, date, days),
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
    return { date, ...rating, text, promptVersion: PARKY_TIP_PROMPT_VERSION };
  }
}
