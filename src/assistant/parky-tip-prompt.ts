/**
 * Parky's one-sentence tip for a day of a park's crowd calendar
 * (`GET /v1/parks/…/parky-tip`). His traits are the chat's (`system-prompt.ts`, "Who you are"
 * and "Your character"), cut to what one sentence can carry.
 */

import type { CalendarDay } from "../parks/dto/integrated-calendar.dto";
import type { AssistantLocale } from "./assistant-locales";
import { languageAddress, languageName } from "./system-prompt";
import { formatInTimeZone } from "date-fns-tz";
import { isoDateWeekday } from "../common/utils/date.util";

/** Bumped whenever the text below changes; part of the cache key and the response. */
export const PARKY_TIP_PROMPT_VERSION = 1;

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
- When a neighbouring day in the input is clearly quieter (at least one crowd level lower and open), name it by its weekday.
- Be funny at most a little, with at most one emoji.
- Plain text only: no Markdown, no links, no quotes around the sentence, no line breaks.
- Write in ${languageName(locale)}. ${languageAddress(locale)}`;
}

/** The input: the park, the chosen day and its neighbours, one line each. */
export function parkyTipInput(
  parkName: string,
  timezone: string,
  date: string,
  days: CalendarDay[],
): string {
  const lines = days.map(
    (day) =>
      `${day.date === date ? "CHOSEN DAY " : ""}${dayLine(day, timezone)}`,
  );
  return `Park: ${parkName}\n${lines.join("\n")}`;
}

function dayLine(day: CalendarDay, timezone: string): string {
  const clock = (instant: string) =>
    formatInTimeZone(new Date(instant), timezone, "HH:mm");
  const facts = [
    `${WEEKDAYS[isoDateWeekday(day.date)]} ${day.date}`,
    day.status === "CLOSED" || day.crowdLevel === "closed"
      ? "closed"
      : `crowd level ${day.crowdLevel.replace("_", " ")}`,
  ];
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
