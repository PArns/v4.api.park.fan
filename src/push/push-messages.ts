import type { PlannedNotification } from "./notification-planner";
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

type MessageWriter = (notification: PlannedNotification) => {
  title: string;
  body: string;
};

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
  notification: PlannedNotification,
  locale: string,
): PushMessage {
  // "de-AT" and "de-CH" are German. Matching the base tag rather than the whole
  // string is the difference between a German notification and an English one
  // for every visitor whose browser reports a region.
  const base = locale.split("-")[0]?.toLowerCase() ?? "en";
  const writer = WRITERS[base] ?? WRITERS.en;
  const { title, body } = writer(notification);

  return { title, body, url: notification.url, tag: notification.dedupeKey };
}
