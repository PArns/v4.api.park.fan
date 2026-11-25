import type { PushMessage } from "./push.service";
import type { PlanChange } from "./plan-change";

/**
 * What a notification says, in the six languages this site publishes.
 *
 * A table in this file rather than a framework, because this API has no i18n at
 * all — nothing else it returns is prose. Six locales times one message is a
 * table; installing `nestjs-i18n` and a message loader for it would be a
 * dependency and a build step for eleven strings.
 *
 * The frontend's own rule applies here and is worth restating because a
 * notification is the most quotable text this project produces: no line may read
 * as machine-written. So no aphorism, no closing maxim, no "Dein Plan wartet."
 * — the message names the ride, the minutes and the time, and stops. Anything
 * beyond that is decoration on a lock screen.
 *
 * One pattern for every kind:
 * - **title**: the thing and the number or time that decides ("Taron: 15
 *   Min."), never the kind's name and never the app's. A long name is cut, not
 *   the number behind it (`fit`).
 * - **subtitle**: the place, so two parks' notifications tell apart in a stack.
 * - **body**: the reason or the next number, a sentence that still reads when
 *   the lock screen cuts it after two lines.
 * No emoji, no exclamation mark, no dash as a pivot (the brand's writing
 * rules); times are the park's clock. The one greeting is the morning plan's
 * subtitle.
 *
 * The language is the SUBSCRIBER's, stored with the subscription, because the
 * job runs with no request to read an `Accept-Language` from. An unknown tag
 * falls back to English rather than to the park's country: somebody who set the
 * site to English and is standing in Brühl wants English.
 */

/** The longest title that still shows whole on a lock screen, roughly. */
export const TITLE_MAX_CHARS = 32;

/** `name` + `rest`, the name cut so that `rest` (the number) is never the part that falls off. */
export function fit(name: string, rest: string): string {
  const chars = [...name];
  // A name up to 16 characters is never cut, whatever follows it.
  const room = Math.max(16, TITLE_MAX_CHARS - [...rest].length);
  return chars.length <= room
    ? `${name}${rest}`
    : `${chars
        .slice(0, room - 1)
        .join("")
        .trimEnd()}…${rest}`;
}

/** One stack per park and park day: alerts, the plan and the weather of one visit belong together. */
export function parkDayThread(parkSlug: string, date: string): string {
  return `${parkSlug}:${date}`;
}

function baseOf(locale: string): string {
  return locale.split("-")[0]?.toLowerCase() ?? "en";
}

/** "de-AT" is German; anything this project has not translated reads English. */
function pick<T>(table: Record<string, T>, locale: string): T {
  return table[baseOf(locale)] ?? table.en;
}

/** The fields a producer may hand through to the device unchanged. */
type DeviceExtras = Pick<
  PushMessage,
  "photo" | "expiresAtMs" | "threadId" | "data"
>;

function extrasOf(n: DeviceExtras): DeviceExtras {
  return {
    ...(n.photo ? { photo: n.photo } : {}),
    ...(n.expiresAtMs ? { expiresAtMs: n.expiresAtMs } : {}),
    ...(n.threadId ? { threadId: n.threadId } : {}),
    ...(n.data ? { data: n.data } : {}),
  };
}

/**
 * The shape both `dueNotifications` (trip planner) and `dueShowNotifications`
 * (followed shows) produce. A show "starts", a plan block is "planned for":
 * the tag's prefix says which.
 */
export interface ScheduledStartCopy extends DeviceExtras {
  dedupeKey: string;
  parkName: string;
  /** What is starting: a ride's name, a show's name, or a free block's label. */
  what: string;
  inMinutes: number;
  /** Park-local `HH:mm`. */
  atTime: string;
  url: string;
}

interface StartWords {
  title: (minutes: number) => string;
  planned: (atTime: string) => string;
  starts: (atTime: string) => string;
}

const START_WORDS: Record<string, StartWords> = {
  de: {
    title: (m) => ` in ${m} Min.`,
    planned: (t) => `Geplant für ${t} Uhr.`,
    starts: (t) => `Beginn ${t} Uhr.`,
  },
  en: {
    title: (m) => ` in ${m} min`,
    planned: (t) => `Planned for ${t}.`,
    starts: (t) => `Starts at ${t}.`,
  },
  nl: {
    title: (m) => ` over ${m} min.`,
    planned: (t) => `Gepland om ${t} uur.`,
    starts: (t) => `Begint om ${t} uur.`,
  },
  fr: {
    title: (m) => ` dans ${m} min`,
    planned: (t) => `Prévu à ${t}.`,
    starts: (t) => `Début à ${t}.`,
  },
  es: {
    title: (m) => ` en ${m} min`,
    planned: (t) => `Previsto a las ${t}.`,
    starts: (t) => `Empieza a las ${t}.`,
  },
  it: {
    title: (m) => ` tra ${m} min`,
    planned: (t) => `Previsto alle ${t}.`,
    starts: (t) => `Inizia alle ${t}.`,
  },
};

/**
 * One notification, written for one subscriber.
 *
 * The `tag` is the dedupe key, which is what makes a phone that was in a pocket
 * for two hours surface ONE banner rather than four about moments that have all
 * passed. The push services honour it, and it costs nothing to send.
 */
export function writeMessage(
  notification: ScheduledStartCopy,
  locale: string,
): PushMessage {
  const words = pick(START_WORDS, locale);
  const isShow = notification.dedupeKey.startsWith("show-start:");
  return {
    title: fit(notification.what, words.title(notification.inMinutes)),
    subtitle: notification.parkName,
    body: (isShow ? words.starts : words.planned)(notification.atTime),
    url: notification.url,
    tag: notification.dedupeKey,
    ...extrasOf(notification),
  };
}

/** What a ride-alert notification needs — a wait time now, not a start time. */
export interface RideAlertCopy extends DeviceExtras {
  dedupeKey: string;
  attractionName: string;
  parkName: string;
  waitTime: number;
  /** The minutes the visitor asked to be told under. */
  thresholdMinutes: number;
  url: string;
}

const RIDE_ALERT_WORDS: Record<
  string,
  { title: (wait: number) => string; body: (threshold: number) => string }
> = {
  de: {
    title: (w) => `: ${w} Min.`,
    body: (t) => `Unter deiner Grenze von ${t} Min.`,
  },
  en: {
    title: (w) => `: ${w} min`,
    body: (t) => `Below your ${t} min limit.`,
  },
  nl: {
    title: (w) => `: ${w} min.`,
    body: (t) => `Onder je grens van ${t} min.`,
  },
  fr: {
    title: (w) => ` : ${w} min`,
    body: (t) => `Sous ta limite de ${t} min.`,
  },
  es: {
    title: (w) => `: ${w} min`,
    body: (t) => `Por debajo de tu límite de ${t} min.`,
  },
  it: {
    title: (w) => `: ${w} min`,
    body: (t) => `Sotto il tuo limite di ${t} min.`,
  },
};

export function writeRideAlertMessage(
  notification: RideAlertCopy,
  locale: string,
): PushMessage {
  const words = pick(RIDE_ALERT_WORDS, locale);
  return {
    title: fit(notification.attractionName, words.title(notification.waitTime)),
    subtitle: notification.parkName,
    body: words.body(notification.thresholdMinutes),
    url: notification.url,
    tag: notification.dedupeKey,
    ...extrasOf(notification),
  };
}

/** What a ride-reopens notification needs — no wait time, the ride is simply open again. */
export type RideReopenCopy = Omit<
  RideAlertCopy,
  "waitTime" | "thresholdMinutes"
>;

/** "Open again", in the words a `plan-change` line uses for the same event. */
const OPEN_AGAIN: Record<string, string> = {
  de: " ist wieder offen",
  en: " is open again",
  nl: " is weer open",
  fr: " a rouvert",
  es: " vuelve a estar abierta",
  it: " è di nuovo aperta",
};

/** The park is the whole second line; there is nothing else to say. */
export function writeRideReopenMessage(
  notification: RideReopenCopy,
  locale: string,
): PushMessage {
  return {
    title: fit(notification.attractionName, pick(OPEN_AGAIN, locale)),
    subtitle: notification.parkName,
    body: "",
    url: notification.url,
    tag: notification.dedupeKey,
    ...extrasOf(notification),
  };
}

/** What a `rain-soon` notification needs. */
export interface RainSoonCopy extends DeviceExtras {
  dedupeKey: string;
  parkName: string;
  /** Park-local `HH:mm` the rain is forecast to start. */
  atTime: string;
  /** Open covered rides, best first. Empty: the park is not ready to name any. */
  rides: Array<{ name: string; waitTime: number | null }>;
  url: string;
}

interface RainSoonWords {
  title: (atTime: string) => string;
  /** With its colon: French sets a space before it. */
  covered: string;
  min: string;
}

const RAIN_SOON_WORDS: Record<string, RainSoonWords> = {
  de: {
    title: (t) => `Regen ab etwa ${t} Uhr`,
    covered: "Überdacht und offen:",
    min: "Min.",
  },
  en: {
    title: (t) => `Rain from about ${t}`,
    covered: "Covered and open:",
    min: "min",
  },
  nl: {
    title: (t) => `Regen vanaf ongeveer ${t} uur`,
    covered: "Overdekt en open:",
    min: "min.",
  },
  fr: {
    title: (t) => `Pluie vers ${t}`,
    covered: "Couvert et ouvert :",
    min: "min",
  },
  es: {
    title: (t) => `Lluvia hacia las ${t}`,
    covered: "Cubiertas y abiertas:",
    min: "min",
  },
  it: {
    title: (t) => `Pioggia verso le ${t}`,
    covered: "Coperte e aperte:",
    min: "min",
  },
};

/**
 * One rain warning, written for one subscriber.
 *
 * The body names, when the park passed the covered-ride gate, up to three open
 * covered rides with their current wait. No wait posted means the name alone,
 * never "0 min". Names and numbers only: the list is the offer, nothing says
 * what the visitor should do.
 */
export function writeRainSoonMessage(
  notification: RainSoonCopy,
  locale: string,
): PushMessage {
  const words = pick(RAIN_SOON_WORDS, locale);
  const rides = notification.rides
    .map((r) =>
      r.waitTime === null ? r.name : `${r.name} (${r.waitTime} ${words.min})`,
    )
    .join(", ");
  return {
    title: words.title(notification.atTime),
    subtitle: notification.parkName,
    body: rides ? `${words.covered} ${rides}` : "",
    url: notification.url,
    tag: notification.dedupeKey,
    ...extrasOf(notification),
  };
}

/** What a `morning-plan` notification needs. */
export interface MorningPlanCopy extends DeviceExtras {
  dedupeKey: string;
  parkName: string;
  /** Park-local `HH:mm` the park opens. */
  opensAt: string;
  /** The first entries of the day, earliest first. */
  entries: Array<{ name: string; atTime: string }>;
  /** The day's crowd level; absent or `unknown` says nothing. */
  crowdLevel?: string | null;
  /** The day's high in °C. */
  tempMax?: number | null;
  /** Chance of rain, 0–100. */
  rainChance?: number | null;
  url: string;
}

interface MorningWords {
  title: (opensAt: string) => string;
  /** The one greeting in this file: the morning is not urgent, and it is Parky's. */
  greeting: string;
  crowd: (level: string) => string;
  levels: Record<string, string>;
  upTo: (temperature: string) => string;
  rain: (percent: string) => string;
  first: (name: string, atTime: string) => string;
  then: (name: string) => string;
}

const MORNING_WORDS: Record<string, MorningWords> = {
  de: {
    title: (t) => ` öffnet um ${t} Uhr`,
    greeting: "Guten Morgen",
    crowd: (l) => `Andrang ${l}`,
    levels: {
      very_low: "sehr niedrig",
      low: "niedrig",
      moderate: "normal",
      high: "hoch",
      very_high: "sehr hoch",
      extreme: "extrem",
    },
    upTo: (t) => `bis ${t}`,
    rain: (p) => `${p} Regenrisiko`,
    first: (n, t) => `Zuerst ${n} um ${t} Uhr`,
    then: (n) => `, dann ${n}`,
  },
  en: {
    title: (t) => ` opens at ${t}`,
    greeting: "Good morning",
    crowd: (l) => `Crowds ${l}`,
    levels: {
      very_low: "very low",
      low: "low",
      moderate: "normal",
      high: "high",
      very_high: "very high",
      extreme: "extreme",
    },
    upTo: (t) => `up to ${t}`,
    rain: (p) => `${p} chance of rain`,
    first: (n, t) => `First ${n} at ${t}`,
    then: (n) => `, then ${n}`,
  },
  nl: {
    title: (t) => ` opent om ${t} uur`,
    greeting: "Goedemorgen",
    crowd: (l) => `Drukte ${l}`,
    levels: {
      very_low: "zeer laag",
      low: "laag",
      moderate: "normaal",
      high: "hoog",
      very_high: "zeer hoog",
      extreme: "extreem",
    },
    upTo: (t) => `tot ${t}`,
    rain: (p) => `${p} kans op regen`,
    first: (n, t) => `Eerst ${n} om ${t} uur`,
    then: (n) => `, daarna ${n}`,
  },
  fr: {
    title: (t) => ` ouvre à ${t}`,
    greeting: "Bonjour",
    crowd: (l) => `Affluence ${l}`,
    levels: {
      very_low: "très faible",
      low: "faible",
      moderate: "normale",
      high: "élevée",
      very_high: "très élevée",
      extreme: "extrême",
    },
    upTo: (t) => `jusqu’à ${t}`,
    rain: (p) => `${p} de risque de pluie`,
    first: (n, t) => `D’abord ${n} à ${t}`,
    then: (n) => `, puis ${n}`,
  },
  es: {
    title: (t) => ` abre a las ${t}`,
    greeting: "Buenos días",
    crowd: (l) => `Afluencia ${l}`,
    levels: {
      very_low: "muy baja",
      low: "baja",
      moderate: "normal",
      high: "alta",
      very_high: "muy alta",
      extreme: "extrema",
    },
    upTo: (t) => `hasta ${t}`,
    rain: (p) => `${p} de probabilidad de lluvia`,
    first: (n, t) => `Primero ${n} a las ${t}`,
    then: (n) => `, luego ${n}`,
  },
  it: {
    title: (t) => ` apre alle ${t}`,
    greeting: "Buongiorno",
    crowd: (l) => `Affluenza ${l}`,
    levels: {
      very_low: "molto bassa",
      low: "bassa",
      moderate: "normale",
      high: "alta",
      very_high: "molto alta",
      extreme: "estrema",
    },
    upTo: (t) => `fino a ${t}`,
    rain: (p) => `${p} di probabilità di pioggia`,
    first: (n, t) => `Prima ${n} alle ${t}`,
    then: (n) => `, poi ${n}`,
  },
};

/** The day sentence opens with whichever fact is known. */
function capitalize(text: string): string {
  return text.charAt(0).toLocaleUpperCase() + text.slice(1);
}

/** `14 °C` and `20 %` as the reader's language writes them. */
function formatUnit(
  value: number,
  locale: string,
  options: Intl.NumberFormatOptions,
): string {
  const base = baseOf(locale);
  return new Intl.NumberFormat(MORNING_WORDS[base] ? base : "en", {
    maximumFractionDigits: 0,
    ...options,
  }).format(value);
}

/**
 * The morning summary of a park day, written for one subscriber.
 *
 * The title is the opening time. The body's first sentence is the day (crowd
 * level, high, chance of rain; whatever is known), its second the first two
 * entries of the plan. No advice and no send-off.
 */
export function writeMorningPlanMessage(
  notification: MorningPlanCopy,
  locale: string,
): PushMessage {
  const words = pick(MORNING_WORDS, locale);
  const level = words.levels[notification.crowdLevel ?? ""];
  const day = [
    level ? words.crowd(level) : null,
    typeof notification.tempMax === "number"
      ? words.upTo(
          formatUnit(notification.tempMax, locale, {
            style: "unit",
            unit: "celsius",
          }),
        )
      : null,
    typeof notification.rainChance === "number"
      ? words.rain(
          formatUnit(notification.rainChance / 100, locale, {
            style: "percent",
          }),
        )
      : null,
  ].filter((part): part is string => part !== null);
  const [first, second] = notification.entries;
  const plan = first
    ? `${words.first(first.name, first.atTime)}${second ? words.then(second.name) : ""}.`
    : null;
  return {
    title: fit(notification.parkName, words.title(notification.opensAt)),
    subtitle: words.greeting,
    body: [day.length > 0 ? `${capitalize(day.join(", "))}.` : null, plan]
      .filter(Boolean)
      .join(" "),
    url: notification.url,
    tag: notification.dedupeKey,
    ...extrasOf(notification),
  };
}

/** What a `plan-change` digest needs. */
export interface PlanChangeCopy extends DeviceExtras {
  dedupeKey: string;
  parkName: string;
  /** The plan day, park-local `yyyy-MM-dd`. */
  date: string;
  isToday: boolean;
  changes: PlanChange[];
  url: string;
}

interface PlanChangeWords {
  /** The second line: whose plan, where and which day. */
  title: (park: string, day: string) => string;
  today: string;
  more: (n: number) => string;
  line: (c: PlanChange) => string;
}

const PLAN_CHANGE_WORDS: Record<string, PlanChangeWords> = {
  de: {
    title: (p, d) => `Dein Plan · ${p}, ${d}`,
    today: "heute",
    more: (n) => `und ${n} weitere Änderung${n === 1 ? "" : "en"}`,
    line: (c) => {
      switch (c.type) {
        case "hours":
          return c.from === "closed"
            ? `Doch geöffnet: ${c.to} Uhr`
            : `Öffnungszeiten jetzt ${c.to} Uhr (vorher ${c.from})`;
        case "closed":
          return "Der Park bleibt an diesem Tag geschlossen";
        case "show-moved":
          return `${c.name} jetzt um ${c.to} Uhr`;
        case "show-cancelled":
          return `${c.name} fällt aus`;
        case "ride-down":
          return `${c.name} ist gerade außer Betrieb`;
        case "ride-up":
          return `${c.name}${OPEN_AGAIN.de}`;
        case "wait":
          return `${c.name} um ${c.at} Uhr: jetzt etwa ${c.to} Min. statt ${c.from}`;
      }
    },
  },
  en: {
    title: (p, d) => `Your plan · ${p}, ${d}`,
    today: "today",
    more: (n) => `and ${n} more change${n === 1 ? "" : "s"}`,
    line: (c) => {
      switch (c.type) {
        case "hours":
          return c.from === "closed"
            ? `Open after all: ${c.to}`
            : `Hours now ${c.to} (were ${c.from})`;
        case "closed":
          return "The park is closed that day";
        case "show-moved":
          return `${c.name} now at ${c.to}`;
        case "show-cancelled":
          return `${c.name} is cancelled`;
        case "ride-down":
          return `${c.name} is down right now`;
        case "ride-up":
          return `${c.name}${OPEN_AGAIN.en}`;
        case "wait":
          return `${c.name} at ${c.at}: now about ${c.to} min instead of ${c.from}`;
      }
    },
  },
  nl: {
    title: (p, d) => `Je plan · ${p}, ${d}`,
    today: "vandaag",
    more: (n) => `en nog ${n} wijziging${n === 1 ? "" : "en"}`,
    line: (c) => {
      switch (c.type) {
        case "hours":
          return c.from === "closed"
            ? `Toch open: ${c.to}`
            : `Openingstijden nu ${c.to} (was ${c.from})`;
        case "closed":
          return "Het park is die dag gesloten";
        case "show-moved":
          return `${c.name} nu om ${c.to}`;
        case "show-cancelled":
          return `${c.name} gaat niet door`;
        case "ride-down":
          return `${c.name} is nu buiten bedrijf`;
        case "ride-up":
          return `${c.name}${OPEN_AGAIN.nl}`;
        case "wait":
          return `${c.name} om ${c.at}: nu ongeveer ${c.to} min. in plaats van ${c.from}`;
      }
    },
  },
  fr: {
    title: (p, d) => `Ton plan · ${p}, ${d}`,
    today: "aujourd’hui",
    more: (n) =>
      `et ${n} autre${n === 1 ? "" : "s"} changement${n === 1 ? "" : "s"}`,
    line: (c) => {
      switch (c.type) {
        case "hours":
          return c.from === "closed"
            ? `Finalement ouvert : ${c.to}`
            : `Horaires désormais ${c.to} (avant ${c.from})`;
        case "closed":
          return "Le parc est fermé ce jour-là";
        case "show-moved":
          return `${c.name} désormais à ${c.to}`;
        case "show-cancelled":
          return `${c.name} est annulé`;
        case "ride-down":
          return `${c.name} est à l’arrêt`;
        case "ride-up":
          return `${c.name}${OPEN_AGAIN.fr}`;
        case "wait":
          return `${c.name} à ${c.at} : environ ${c.to} min au lieu de ${c.from}`;
      }
    },
  },
  es: {
    title: (p, d) => `Tu plan · ${p}, ${d}`,
    today: "hoy",
    more: (n) => `y ${n} cambio${n === 1 ? "" : "s"} más`,
    line: (c) => {
      switch (c.type) {
        case "hours":
          return c.from === "closed"
            ? `Abre al final: ${c.to}`
            : `Horario ahora ${c.to} (antes ${c.from})`;
        case "closed":
          return "El parque cierra ese día";
        case "show-moved":
          return `${c.name} ahora a las ${c.to}`;
        case "show-cancelled":
          return `${c.name} se cancela`;
        case "ride-down":
          return `${c.name} está parada ahora mismo`;
        case "ride-up":
          return `${c.name}${OPEN_AGAIN.es}`;
        case "wait":
          return `${c.name} a las ${c.at}: ahora unos ${c.to} min en vez de ${c.from}`;
      }
    },
  },
  it: {
    title: (p, d) => `Il tuo piano · ${p}, ${d}`,
    today: "oggi",
    more: (n) => `e ${n} altr${n === 1 ? "a modifica" : "e modifiche"}`,
    line: (c) => {
      switch (c.type) {
        case "hours":
          return c.from === "closed"
            ? `Aperto comunque: ${c.to}`
            : `Orari ora ${c.to} (prima ${c.from})`;
        case "closed":
          return "Il parco è chiuso quel giorno";
        case "show-moved":
          return `${c.name} ora alle ${c.to}`;
        case "show-cancelled":
          return `${c.name} è annullato`;
        case "ride-down":
          return `${c.name} è ferma in questo momento`;
        case "ride-up":
          return `${c.name}${OPEN_AGAIN.it}`;
        case "wait":
          return `${c.name} alle ${c.at}: ora circa ${c.to} min invece di ${c.from}`;
      }
    },
  },
};

/** How many changes a digest names before it says how many more there are. */
export const PLAN_CHANGE_MAX_LINES = 3;

/** A name this long still leaves a title's verb on the lock screen. */
const PLAN_CHANGE_TITLE_NAME_CHARS = 24;

/**
 * One `plan-change` digest, written for one subscriber. The first change IS the
 * title (never "your plan changed"), whose plan and which day the second
 * line, the other changes one per line, the rest counted.
 */
export function writePlanChangeMessage(
  copy: PlanChangeCopy,
  locale: string,
): PushMessage {
  const base = baseOf(locale);
  const words = PLAN_CHANGE_WORDS[base] ?? PLAN_CHANGE_WORDS.en;
  const day = copy.isToday ? words.today : shortDate(copy.date, base);
  const [first, ...others] = copy.changes;
  const lines = others.slice(0, PLAN_CHANGE_MAX_LINES - 1).map(words.line);
  const rest = others.length - lines.length;
  if (rest > 0) lines.push(words.more(rest));
  return {
    title: first ? words.line(shortName(first)) : "",
    subtitle: words.title(copy.parkName, day),
    body: lines.join("\n"),
    url: copy.url,
    tag: copy.dedupeKey,
    kind: "plan-change",
    ...extrasOf(copy),
  };
}

function shortName(change: PlanChange): PlanChange {
  if (!("name" in change)) return change;
  const chars = [...change.name];
  return chars.length <= PLAN_CHANGE_TITLE_NAME_CHARS
    ? change
    : {
        ...change,
        name: `${chars
          .slice(0, PLAN_CHANGE_TITLE_NAME_CHARS - 1)
          .join("")
          .trimEnd()}…`,
      };
}

/** `Sa., 28.11.` in the subscriber's language; the date is a calendar day, so read in UTC. */
function shortDate(date: string, locale: string): string {
  try {
    // English is read on both sides of the Atlantic: "Sat 28 Nov", never 11/28.
    const english = locale === "en" || !PLAN_CHANGE_WORDS[locale];
    return new Intl.DateTimeFormat(english ? "en-GB" : locale, {
      weekday: "short",
      day: "numeric",
      month: english ? "short" : "numeric",
      timeZone: "UTC",
    }).format(new Date(`${date}T12:00:00Z`));
  } catch {
    return date;
  }
}
