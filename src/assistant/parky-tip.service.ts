import { createHash } from "node:crypto";
import { Inject, Injectable, Logger } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import Anthropic from "@anthropic-ai/sdk";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../common/redis/redis.module";
import { cachedJson, readCachedJson } from "../common/cache/redis-json-cache";
import { parseDateRange } from "../common/utils/date-parsing.util";
import {
  addIsoDays,
  getCurrentDateInTimezone,
} from "../common/utils/date.util";
import type { CrowdLevel } from "../common/types/crowd-level.type";
import type { Park } from "../parks/entities/park.entity";
import type { CalendarDay } from "../parks/dto/integrated-calendar.dto";
import { CalendarService } from "../parks/services/calendar.service";
import { monthBounds, monthOf } from "../trip-regions/last-year-snapshot";
import { AssistantSettingsService } from "./assistant-settings.service";
import { ASSISTANT_MODEL } from "./assistant-chat.service";
import type { AssistantLocale } from "./assistant-locales";
import {
  PARKY_TIP_MAX_CHARS,
  PARKY_TIP_PROMPT_VERSION,
  parkyTipInput,
  parkyTipSystemPrompt,
  soFarToday,
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
  /**
   * On the tip for today only, the level so far the sentence was written from, so a client can
   * hide a sentence that says another morning than the „bisher" it shows. Absent on other days and
   * where today has none yet.
   */
  todayCrowdLevel?: CrowdLevel;
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
  todayCrowdLevel?: CrowdLevel;
}

/**
 * A day's crowd level and Parky's verdict on it, with the level so far where the tip for `date`
 * reads one (`soFarToday`); null for a closed or unrated day.
 */
function rateDay(
  day: CalendarDay | undefined,
  date: string,
  today: string,
): DayRating | null {
  if (!day || day.status === "CLOSED" || day.crowdLevel === "closed") {
    return null;
  }
  const verdict = parkyVerdict(day.crowdLevel);
  if (!verdict) return null;
  const soFar = soFarToday(day, date, today);
  return {
    crowdLevel: day.crowdLevel,
    verdict,
    ...(soFar && { todayCrowdLevel: soFar }),
  };
}

/** Whether a tip is written for `date`: from the park's today to the horizon. */
function inHorizon(today: string, date: string): boolean {
  return date >= today && date <= addIsoDays(today, PARKY_TIP_HORIZON_DAYS);
}

/**
 * What the sentence may say about each day it reads: the day's status and crowd level, and on the
 * tip for today the level so far (`soFarToday`). The sentence names a quieter neighbour, so a
 * neighbour that moves must move the key as well as the chosen day. Weather stays out: it changes
 * with every forecast run, and a sentence written again for each cloud would cost a model call
 * each time.
 */
function daysDigest(days: CalendarDay[], date: string, today: string): string {
  const facts = days
    .map(
      (day) =>
        `${day.date} ${day.status} ${day.crowdLevel} ${soFarToday(day, date, today) ?? "-"}`,
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
  return `parky-tip:v${PARKY_TIP_PROMPT_VERSION}:${parkId}:${today}:${date}:${daysDigest(days, date, today)}:${locale}`;
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
   * What the chat gets when a visitor taps the tip: the sentence kept for the day in `locale` and
   * the day's facts as the tip reads them (`parkyTipInput`), from one read of the calendar. The
   * sentence is only read, never written: the question would otherwise wait for a model call, and
   * a sentence written for it may not be the one the visitor was shown. Null text where none is
   * kept for the days as they stand now, or Parky is off. The sentence is the server's, never the
   * one the browser says it showed. Null outside the days a tip is written for.
   */
  async dayTip(
    park: Park,
    date: string,
    locale: AssistantLocale,
  ): Promise<{ text: string | null; facts: string } | null> {
    const today = getCurrentDateInTimezone(park.timezone);
    if (!inHorizon(today, date)) return null;
    const days = await this.daysAround(park, today, date);
    const kept = (await this.settings.publicConfig()).enabled
      ? await readCachedJson<ParkyTip | false>(
          this.redis,
          tipKey(park.id, today, date, days, locale),
        )
      : null;
    return {
      text: kept ? kept.text : null,
      facts: parkyTipInput(park.name, park.timezone, today, date, days),
    };
  }

  /**
   * The tip kept for these days, written on a miss. The days are read before the cache because
   * they are part of the key (`daysAround`), so a tip already written costs that calendar read and
   * one GET.
   */
  private async served(
    park: Park,
    today: string,
    date: string,
    locale: AssistantLocale,
  ): Promise<ParkyTip | null> {
    // Off, keyless or past the day's budget: no tip, cached ones included.
    if (!(await this.settings.publicConfig()).enabled) return null;
    try {
      const days = await this.daysAround(park, today, date);
      const rating = rateDay(
        days.find((d) => d.date === date),
        date,
        today,
      );
      if (!rating) return null;
      const tip = await cachedJson<ParkyTip | false | null>(
        this.redis,
        tipKey(park.id, today, date, days, locale),
        CACHE_TTL_SECONDS,
        () => this.write(park, today, date, locale, rating, days),
      );
      return tip || null;
    } catch (error) {
      this.logger.warn(
        `Parky tip failed: ${error instanceof Error ? error.message : String(error)}`,
      );
      return null;
    }
  }

  /**
   * The calendar's days around `date` that the prompt sees, from today on. Read as the whole
   * months that hold them, the way the grid reads them: the calendar keeps only whole months
   * (`calendar:month:v2`, which the warmup keeps for −1…+3 months), so a week asked for on its own
   * would be built from the database whenever its month is not cached, and kept nowhere. Whole
   * months come from one MGET of one or two of those caches; a month not cached is built once and
   * kept for the next reader, as for any calendar read.
   */
  private async daysAround(
    park: Park,
    today: string,
    date: string,
  ): Promise<CalendarDay[]> {
    const before = addIsoDays(date, -NEIGHBOUR_DAYS);
    const from = before < today ? today : before;
    const to = addIsoDays(date, NEIGHBOUR_DAYS);
    const { fromDate, toDate } = parseDateRange(
      monthBounds(monthOf(from)).first,
      monthBounds(monthOf(to)).last,
      { timezone: park.timezone },
    );
    const calendar = await this.moduleRef
      .get(CalendarService, { strict: false })
      .buildCalendarResponse(park, fromDate, toDate, "none");
    return calendar.days.filter((day) => day.date >= from && day.date <= to);
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
