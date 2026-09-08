import type { PushMessage } from "./push.service";

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
