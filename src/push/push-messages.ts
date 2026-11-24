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
 * The language is the SUBSCRIBER's, stored with the subscription, because the
 * job runs with no request to read an `Accept-Language` from. An unknown tag
 * falls back to English rather than to the park's country: somebody who set the
 * site to English and is standing in Brühl wants English.
 */

/**
 * The shape both `dueNotifications` (trip planner) and `dueShowNotifications`
 * (followed shows) produce — "something starts soon" has one sentence
 * regardless of what that something is, so `PlannedNotification`'s extra
 * `topic` field (which this file never reads — only the processor's
 * subscriber-gating does) is not part of the contract here.
 */
export interface ScheduledStartCopy {
  dedupeKey: string;
  parkName: string;
  /** What is starting: a ride's name, a show's name, or a free block's label. */
  what: string;
  inMinutes: number;
  /** Park-local `HH:mm`. */
  atTime: string;
  url: string;
}

type MessageWriter = (notification: ScheduledStartCopy) => {
  title: string;
  body: string;
};

/**
 * Resolve a locale to a writer and assemble the `PushMessage`.
 *
 * Shared by `writeMessage` and `writeRideAlertMessage`, which used to carry
 * this identically: "de-AT" and "de-CH" are German, so matching the base tag
 * rather than the whole string is the difference between a German
 * notification and an English one for every visitor whose browser reports a
 * region. Every table here defines `en`, which is the fallback for anything
 * this project has not translated.
 */
function writeFromTable<T extends { dedupeKey: string; url: string }>(
  writers: Record<string, (notification: T) => { title: string; body: string }>,
  notification: T,
  locale: string,
): PushMessage {
  const base = locale.split("-")[0]?.toLowerCase() ?? "en";
  const writer = writers[base] ?? writers.en;
  const { title, body } = writer(notification);
  return { title, body, url: notification.url, tag: notification.dedupeKey };
}

const WRITERS: Record<string, MessageWriter> = {
  de: (n) => ({
    title: `In ${n.inMinutes} Min.: ${n.what}`,
    body: `${n.atTime} Uhr, ${n.parkName}`,
  }),
  en: (n) => ({
    title: `In ${n.inMinutes} min: ${n.what}`,
    body: `${n.atTime}, ${n.parkName}`,
  }),
  nl: (n) => ({
    title: `Over ${n.inMinutes} min.: ${n.what}`,
    body: `${n.atTime}, ${n.parkName}`,
  }),
  fr: (n) => ({
    title: `Dans ${n.inMinutes} min : ${n.what}`,
    body: `${n.atTime}, ${n.parkName}`,
  }),
  es: (n) => ({
    title: `En ${n.inMinutes} min: ${n.what}`,
    body: `${n.atTime}, ${n.parkName}`,
  }),
  it: (n) => ({
    title: `Tra ${n.inMinutes} min: ${n.what}`,
    body: `${n.atTime}, ${n.parkName}`,
  }),
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
  return writeFromTable(WRITERS, notification, locale);
}

/** What a ride-alert notification needs — a wait time now, not a start time. */
export interface RideAlertCopy {
  dedupeKey: string;
  attractionName: string;
  parkName: string;
  waitTime: number;
  url: string;
}

type RideAlertWriter = (notification: RideAlertCopy) => {
  title: string;
  body: string;
};

const RIDE_ALERT_WRITERS: Record<string, RideAlertWriter> = {
  de: (n) => ({
    title: `${n.attractionName}: nur noch ${n.waitTime} Min.`,
    body: n.parkName,
  }),
  en: (n) => ({
    title: `${n.attractionName}: only ${n.waitTime} min now`,
    body: n.parkName,
  }),
  nl: (n) => ({
    title: `${n.attractionName}: nog maar ${n.waitTime} min.`,
    body: n.parkName,
  }),
  fr: (n) => ({
    title: `${n.attractionName} : plus que ${n.waitTime} min`,
    body: n.parkName,
  }),
  es: (n) => ({
    title: `${n.attractionName}: solo ${n.waitTime} min`,
    body: n.parkName,
  }),
  it: (n) => ({
    title: `${n.attractionName}: solo ${n.waitTime} min`,
    body: n.parkName,
  }),
};

export function writeRideAlertMessage(
  notification: RideAlertCopy,
  locale: string,
): PushMessage {
  return writeFromTable(RIDE_ALERT_WRITERS, notification, locale);
}

/** What a ride-reopens notification needs — no wait time, the ride is simply open again. */
export type RideReopenCopy = Omit<RideAlertCopy, "waitTime">;

const RIDE_REOPEN_WRITERS: Record<
  string,
  (notification: RideReopenCopy) => { title: string; body: string }
> = {
  de: (n) => ({
    title: `${n.attractionName} ist wieder offen`,
    body: n.parkName,
  }),
  en: (n) => ({ title: `${n.attractionName} is open again`, body: n.parkName }),
  nl: (n) => ({ title: `${n.attractionName} is weer open`, body: n.parkName }),
  fr: (n) => ({ title: `${n.attractionName} a rouvert`, body: n.parkName }),
  es: (n) => ({
    title: `${n.attractionName} vuelve a estar abierta`,
    body: n.parkName,
  }),
  it: (n) => ({
    title: `${n.attractionName} è di nuovo aperta`,
    body: n.parkName,
  }),
};

export function writeRideReopenMessage(
  notification: RideReopenCopy,
  locale: string,
): PushMessage {
  return writeFromTable(RIDE_REOPEN_WRITERS, notification, locale);
}

/** What a `rain-soon` notification needs. */
export interface RainSoonCopy {
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
  covered: string;
  min: string;
}

const RAIN_SOON_WORDS: Record<string, RainSoonWords> = {
  de: {
    title: (t) => `Regen ab etwa ${t} Uhr`,
    covered: "Überdacht und offen",
    min: "Min.",
  },
  en: {
    title: (t) => `Rain from about ${t}`,
    covered: "Covered and open",
    min: "min",
  },
  nl: {
    title: (t) => `Regen vanaf ongeveer ${t}`,
    covered: "Overdekt en open",
    min: "min.",
  },
  fr: {
    title: (t) => `Pluie vers ${t}`,
    covered: "Couvert et ouvert",
    min: "min",
  },
  es: {
    title: (t) => `Lluvia hacia las ${t}`,
    covered: "Cubiertas y abiertas",
    min: "min",
  },
  it: {
    title: (t) => `Pioggia verso le ${t}`,
    covered: "Coperte e aperte",
    min: "min",
  },
};

/**
 * One rain warning, written for one subscriber.
 *
 * The body names the park and, when the park passed the covered-ride gate, up to
 * three open covered rides with their current wait. No wait posted means the
 * name alone, never "0 min". Names and numbers only, like the other messages
 * here: nothing says what the visitor should do.
 */
export function writeRainSoonMessage(
  notification: RainSoonCopy,
  locale: string,
): PushMessage {
  const base = locale.split("-")[0]?.toLowerCase() ?? "en";
  const words = RAIN_SOON_WORDS[base] ?? RAIN_SOON_WORDS.en;
  const rides = notification.rides
    .map((r) =>
      r.waitTime === null ? r.name : `${r.name} (${r.waitTime} ${words.min})`,
    )
    .join(", ");
  return {
    title: words.title(notification.atTime),
    body: rides
      ? `${notification.parkName}. ${words.covered}: ${rides}`
      : notification.parkName,
    url: notification.url,
    tag: notification.dedupeKey,
  };
}

/** What a `morning-plan` notification needs. */
export interface MorningPlanCopy {
  dedupeKey: string;
  parkName: string;
  /** Park-local `HH:mm` the park opens. */
  opensAt: string;
  /** The first entries of the day, earliest first. */
  entries: Array<{ name: string; atTime: string }>;
  url: string;
}

const MORNING_PLAN_TITLES: Record<
  string,
  (parkName: string, opensAt: string) => string
> = {
  de: (p, t) => `${p} öffnet um ${t} Uhr`,
  en: (p, t) => `${p} opens at ${t}`,
  nl: (p, t) => `${p} opent om ${t}`,
  fr: (p, t) => `${p} ouvre à ${t}`,
  es: (p, t) => `${p} abre a las ${t}`,
  it: (p, t) => `${p} apre alle ${t}`,
};

/**
 * The morning summary of a park day, written for one subscriber.
 *
 * The body is the first entries as `HH:mm Name`, one per line, and nothing
 * else: no greeting and no advice, like the other messages here.
 */
export function writeMorningPlanMessage(
  notification: MorningPlanCopy,
  locale: string,
): PushMessage {
  const base = locale.split("-")[0]?.toLowerCase() ?? "en";
  const title = MORNING_PLAN_TITLES[base] ?? MORNING_PLAN_TITLES.en;
  return {
    title: title(notification.parkName, notification.opensAt),
    body: notification.entries.map((e) => `${e.atTime} ${e.name}`).join("\n"),
    url: notification.url,
    tag: notification.dedupeKey,
  };
}

/** What a `plan-change` digest needs. */
export interface PlanChangeCopy {
  dedupeKey: string;
  parkName: string;
  /** The plan day, park-local `yyyy-MM-dd`. */
  date: string;
  isToday: boolean;
  changes: PlanChange[];
  url: string;
}

interface PlanChangeWords {
  title: (park: string, day: string) => string;
  today: string;
  more: (n: number) => string;
  line: (c: PlanChange) => string;
}

const PLAN_CHANGE_WORDS: Record<string, PlanChangeWords> = {
  de: {
    title: (p, d) => `Dein Plan: ${p}, ${d}`,
    today: "heute",
    more: (n) => `und ${n} weitere Änderung${n === 1 ? "" : "en"}`,
    line: (c) => {
      switch (c.type) {
        case "hours":
          return c.from === "closed"
            ? `Doch geöffnet: ${c.to} Uhr`
            : `Öffnungszeiten jetzt ${c.to} Uhr (vorher ${c.from})`;
        case "closed":
          return "Der Park hat an diesem Tag geschlossen";
        case "show-moved":
          return `${c.name} jetzt um ${c.to} Uhr`;
        case "show-cancelled":
          return `${c.name} fällt aus`;
        case "ride-down":
          return `${c.name} ist gerade außer Betrieb`;
        case "ride-up":
          return `${c.name} fährt wieder`;
        case "wait":
          return `${c.name} um ${c.at} Uhr: jetzt etwa ${c.to} Min. statt ${c.from}`;
      }
    },
  },
  en: {
    title: (p, d) => `Your plan: ${p}, ${d}`,
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
          return `${c.name} is running again`;
        case "wait":
          return `${c.name} at ${c.at}: now about ${c.to} min instead of ${c.from}`;
      }
    },
  },
  nl: {
    title: (p, d) => `Je plan: ${p}, ${d}`,
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
          return `${c.name} draait weer`;
        case "wait":
          return `${c.name} om ${c.at}: nu ongeveer ${c.to} min. in plaats van ${c.from}`;
      }
    },
  },
  fr: {
    title: (p, d) => `Ton plan : ${p}, ${d}`,
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
          return `${c.name} fonctionne à nouveau`;
        case "wait":
          return `${c.name} à ${c.at} : environ ${c.to} min au lieu de ${c.from}`;
      }
    },
  },
  es: {
    title: (p, d) => `Tu plan: ${p}, ${d}`,
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
          return `${c.name} vuelve a funcionar`;
        case "wait":
          return `${c.name} a las ${c.at}: ahora unos ${c.to} min en vez de ${c.from}`;
      }
    },
  },
  it: {
    title: (p, d) => `Il tuo piano: ${p}, ${d}`,
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
          return `${c.name} funziona di nuovo`;
        case "wait":
          return `${c.name} alle ${c.at}: ora circa ${c.to} min invece di ${c.from}`;
      }
    },
  },
};

/** How many changes a digest names before it says how many more there are. */
export const PLAN_CHANGE_MAX_LINES = 3;

/**
 * One `plan-change` digest, written for one subscriber: the park and day as
 * title, one line per change, the rest counted.
 */
export function writePlanChangeMessage(
  copy: PlanChangeCopy,
  locale: string,
): PushMessage {
  const base = locale.split("-")[0]?.toLowerCase() ?? "en";
  const words = PLAN_CHANGE_WORDS[base] ?? PLAN_CHANGE_WORDS.en;
  const day = copy.isToday ? words.today : shortDate(copy.date, base);
  const lines = copy.changes.slice(0, PLAN_CHANGE_MAX_LINES).map(words.line);
  const rest = copy.changes.length - lines.length;
  if (rest > 0) lines.push(words.more(rest));
  return {
    title: words.title(copy.parkName, day),
    body: lines.join("\n"),
    url: copy.url,
    tag: copy.dedupeKey,
    kind: "plan-change",
  };
}

/** `Sa., 28.11.` in the subscriber's language; the date is a calendar day, so read in UTC. */
function shortDate(date: string, locale: string): string {
  try {
    return new Intl.DateTimeFormat(PLAN_CHANGE_WORDS[locale] ? locale : "en", {
      weekday: "short",
      day: "numeric",
      month: "numeric",
      timeZone: "UTC",
    }).format(new Date(`${date}T12:00:00Z`));
  } catch {
    return date;
  }
}
