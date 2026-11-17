/**
 * Parky's one-sentence tip for a day of a park's crowd calendar
 * (`GET /v1/parks/…/parky-tip`). His traits are the chat's (`system-prompt.ts`, "Who you are"
 * and "Your character"), cut to what one sentence can carry.
 */

import type { CalendarDay } from "../parks/dto/integrated-calendar.dto";
import type { CrowdLevel } from "../common/types/crowd-level.type";
import type { AssistantLocale } from "./assistant-locales";
import { languageAddress, languageName } from "./system-prompt";
import { formatInTimeZone } from "date-fns-tz";
import { isoDateWeekday } from "../common/utils/date.util";

/**
 * Bumped whenever the text below or the facts the input carries change; part of the cache key and
 * the response.
 */
export const PARKY_TIP_PROMPT_VERSION = 4;

/** The longest tip served; a longer one is refused, not cut. */
export const PARKY_TIP_MAX_CHARS = 160;

const WEEKDAYS = [
  "Sunday",
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
];

export function parkyTipSystemPrompt(locale: AssistantLocale): string {
  return `You are Parky, park.fan's mascot and the user's park buddy: the friend who knows every park, never a help desk or an assistant. park.fan's crowd calendar is your own work: say "I" or "my forecast", never "according to park.fan" or "the data".
Your character: warm, cheerful, a bit cheeky, a theme-park nerd who wants nobody to lose their day in a queue. Your humour is warm, never snarky, and never about a park, a ride or a visitor.

Write ONE sentence of at most ${PARKY_TIP_MAX_CHARS} characters about the visitor's chosen day at this park: is it a good day to go, and why.
- Use only facts in the input: crowd level, opening hours, holidays, school holidays, events, weather. Never add a number, a ride, an event or a reason the input does not hold.
- Name a day the way a friend would, counted from TODAY in the input: today, tomorrow, the weekday within the next six days ("on Saturday"), "next weekend" for the Saturday or Sunday after the coming one; further out, the weekday with the day and month ("Saturday, 5 December"). Never the year-month-day form and never "the chosen day".
- When a neighbouring day in the input is clearly quieter (at least one crowd level lower and open), name it the same way.
- TODAY's line may also say how busy the day has been so far; its crowd level is your forecast for the whole day. When the chosen day is today and the two differ, say both honestly, never the forecast alone.
- Be funny at most a little, with at most one emoji.
- Plain text only: no Markdown, no links, no quotes around the sentence, no line breaks.
- Write in ${languageName(locale)}. ${languageAddress(locale)}`;
}

/**
 * Today's level measured so far, as the tip for `date` reads it: only the tip for today itself.
 * The level moves during the day, and in the tips of the days around today it would have each of
 * them written again in every language with every move.
 */
export function soFarToday(
  day: CalendarDay,
  date: string,
  today: string,
): CrowdLevel | undefined {
  return date === today && day.date === today ? day.todayCrowdLevel : undefined;
}

/**
 * The input: the park, its today (park-local, the day "today" and "tomorrow" count from), the
 * chosen day and its neighbours, one line each. On the tip for today, today's line adds the level
 * measured so far, which the day dialog shows under the forecast (`soFarToday`).
 */
export function parkyTipInput(
  parkName: string,
  timezone: string,
  today: string,
  date: string,
  days: CalendarDay[],
): string {
  const lines = days.map(
    (day) =>
      `${day.date === date ? "CHOSEN DAY " : ""}${dayLine(day, timezone, soFarToday(day, date, today))}`,
  );
  return `Park: ${parkName}\nTODAY at the park: ${WEEKDAYS[isoDateWeekday(today)]} ${today}\n${lines.join("\n")}`;
}

const levelWords = (level: string) => level.replace("_", " ");

function dayLine(
  day: CalendarDay,
  timezone: string,
  soFar: CrowdLevel | undefined,
): string {
  const clock = (instant: string) =>
    formatInTimeZone(new Date(instant), timezone, "HH:mm");
  const closed = day.status === "CLOSED" || day.crowdLevel === "closed";
  const facts = [
    `${WEEKDAYS[isoDateWeekday(day.date)]} ${day.date}`,
    closed ? "closed" : `crowd level ${levelWords(day.crowdLevel)}`,
  ];
  if (!closed && soFar) {
    facts.push(`so far today: ${levelWords(soFar)}`);
  }
  if (day.hours?.openingTime && day.hours.closingTime) {
    facts.push(
      `open ${clock(day.hours.openingTime)}-${clock(day.hours.closingTime)}`,
    );
  } else if (day.status === "UNKNOWN") {
    facts.push("hours not published yet");
  }
  if (day.isHoliday) facts.push("public holiday");
  if (day.isBridgeDay) facts.push("bridge day");
  if (day.isSchoolVacation) facts.push("school holidays");
  for (const event of day.events ?? []) facts.push(event.name);
  const neighbours = (day.neighborHolidays ?? []).slice(0, 2);
  if (neighbours.length > 0) {
    facts.push(
      `holidays nearby: ${neighbours.map((holiday) => holiday.name).join(", ")}`,
    );
  }
  if (day.weather?.basis === "forecast") {
    facts.push(
      `weather ${day.weather.condition}, ${Math.round(day.weather.tempMin)}-${Math.round(day.weather.tempMax)} °C, ${day.weather.rainChance}% rain`,
    );
  }
  return facts.join("; ");
}
