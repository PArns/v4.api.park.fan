import { formatInTimeZone } from "date-fns-tz";
import type { PushMessage } from "./push.service";
import type { DevicePushKind } from "./push-config";
import { photoKeyOf } from "./push-media";
import {
  parkDayThread,
  writeMessage,
  writeMorningPlanMessage,
  writePlanChangeMessage,
  writeRainSoonMessage,
  writeRideAlertMessage,
  writeRideReopenMessage,
} from "./push-messages";
import {
  POST_FEEDS,
  fetchExportPosts,
  newsLocaleOf,
  writeNewsMessage,
  type NewsExportPost,
} from "../queues/services/news-push.service";

/**
 * A device's self-test (`POST /v1/push/devices/test`): one sample of a kind,
 * written by the same writer the real trigger uses, so title, pictogram,
 * subtitle, thread, category, level, photo and expiry are the real ones.
 *
 * The data is a fixed visit to Phantasialand (Taron, its real photo from the
 * media index), its times read from the park's clock now; news and guides are
 * the newest real post of the reader's export, so a tap opens an article that
 * exists. Nothing in the visible text says "test": the point is to see what a
 * real one looks like. The marker is in the payload: `test: "true"`, `test=1`
 * on the `url` (all the app's tap and "Umplanen" hand on), and every id an
 * action button would write with is `"test"` (`attractionId`, `entry`), so
 * even an app that ignores the marker changes nothing real.
 */
export const PUSH_SELF_TEST_KINDS = [
  "ride-wait",
  "ride-reopen",
  "next-up",
  "plan-change",
  "rain-soon",
  "morning-plan",
  "news",
  "guides",
] as const satisfies readonly DevicePushKind[];
export type PushSelfTestKind = (typeof PUSH_SELF_TEST_KINDS)[number];

/** `PUSH_SELF_TEST=false` closes the route (404): a switch for abuse that needs no release. */
export function isPushSelfTestEnabled(): boolean {
  return (process.env.PUSH_SELF_TEST ?? "true").trim() !== "false";
}

const ZONE = "Europe/Berlin";
const PARK = "Phantasialand";
const SLUG = "phantasialand";
const GEO = "europe/germany/bruehl/phantasialand";
const RIDE_URL = `/parks/${GEO}/taron`;
const PARK_URL = `/parks/${GEO}`;

/** Park-local `HH:mm`, `minutes` from now, rounded up to `step`. */
function clock(nowMs: number, minutes: number, step = 5): string {
  const at = Math.ceil((nowMs + minutes * 60_000) / (step * 60_000));
  return formatInTimeZone(new Date(at * step * 60_000), ZONE, "HH:mm");
}

export async function selfTestMessage(
  kind: PushSelfTestKind,
  locale: string,
  nowMs: number,
  posts: (
    feed: (typeof POST_FEEDS)[number],
    locale: string,
  ) => Promise<NewsExportPost[]> = fetchExportPosts,
): Promise<PushMessage> {
  const today = formatInTimeZone(new Date(nowMs), ZONE, "yyyy-MM-dd");
  const thread = parkDayThread(SLUG, today);
  const planUrl = `/trip-planner?park=${encodeURIComponent(GEO)}&date=${today}`;
  const ride = {
    dedupeKey: "",
    attractionName: "Taron",
    parkName: PARK,
    url: RIDE_URL,
    photo: photoKeyOf(RIDE_URL),
    threadId: thread,
    data: { attractionId: "test" },
  };
  const written = await (async (): Promise<PushMessage> => {
    switch (kind) {
      case "ride-wait":
        return writeRideAlertMessage(
          { ...ride, waitTime: 15, thresholdMinutes: 20 },
          locale,
        );
      case "ride-reopen":
        return writeRideReopenMessage(ride, locale);
      case "next-up":
        return writeMessage(
          {
            dedupeKey: "",
            parkName: PARK,
            what: "Taron",
            inMinutes: 10,
            atTime: clock(nowMs, 10),
            url: planUrl,
            expiresAtMs: nowMs + 10 * 60_000,
            threadId: thread,
            photo: `${GEO}/taron`,
            data: { entry: "test", date: today, park: GEO, ride: "taron" },
          },
          locale,
        );
      case "plan-change":
        return writePlanChangeMessage(
          {
            dedupeKey: "",
            parkName: PARK,
            date: today,
            isToday: true,
            changes: [
              { type: "ride-down", name: "Taron" },
              {
                type: "wait",
                name: "Black Mamba",
                at: clock(nowMs, 60, 15),
                from: 25,
                to: 45,
              },
            ],
            url: planUrl,
            expiresAtMs: nowMs + 60 * 60_000,
            threadId: thread,
            data: { park: GEO, date: today },
          },
          locale,
        );
      case "rain-soon":
        return writeRainSoonMessage(
          {
            dedupeKey: "",
            parkName: PARK,
            atTime: clock(nowMs, 20),
            rides: [
              { name: "Maus au Chocolat", waitTime: 15 },
              { name: "Fly", waitTime: 20 },
            ],
            url: PARK_URL,
            expiresAtMs: nowMs + 20 * 60_000,
            threadId: thread,
          },
          locale,
        );
      case "morning-plan":
        return writeMorningPlanMessage(
          {
            dedupeKey: "",
            parkName: PARK,
            opensAt: clock(nowMs, 60, 30),
            entries: [
              { name: "Taron", atTime: clock(nowMs, 90, 30) },
              { name: "Black Mamba", atTime: clock(nowMs, 120, 30) },
            ],
            crowdLevel: "low",
            tempMax: 14,
            rainChance: 20,
            url: PARK_URL,
            photo: GEO,
            expiresAtMs: nowMs + 60 * 60_000,
            threadId: thread,
          },
          locale,
        );
      case "news":
      case "guides": {
        const feed = POST_FEEDS.find((f) => f.kind === kind) ?? POST_FEEDS[0];
        const list = await posts(feed, newsLocaleOf(locale)).catch(() => []);
        const newest = [...list].sort((a, b) =>
          b.publishedAt.localeCompare(a.publishedAt),
        )[0];
        // An unreadable export still shows the kind; its tap opens the list.
        return writeNewsMessage(
          newest ?? { slug: "", title: PARK, publishedAt: "" },
          feed,
          locale,
        );
      }
    }
  })();
  return {
    ...written,
    // Its own collapse id: a test replaces the last test, never a real banner.
    tag: `${kind}:test`,
    kind,
    // The app's tap and "Umplanen" forward only the url: this is how they know.
    url: `${written.url}${written.url.includes("?") ? "&" : "?"}test=1`,
    data: { ...written.data, test: "true" },
  };
}
