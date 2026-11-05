import { z } from "zod";
import { ASSISTANT_LOCALES } from "./system-prompt";
import {
  RIDE_ALERT_THRESHOLD_MAX,
  RIDE_ALERT_THRESHOLD_MIN,
} from "../ride-alerts/dto/ride-alert.dto";

/** A park.fan path: slug segments only, no query, host or scheme. */
const pagePath = z
  .string()
  .max(300)
  .regex(/^\/[a-z0-9\-/]*$/, "must be a path of slugs");

const slug = z
  .string()
  .max(160)
  .regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/);

/** A name the browser typed or stored, on one line: it reaches the model as data. */
const plannerName = z
  .string()
  .max(80)
  .transform((value) => value.replace(/\s+/g, " ").trim());

/** A planner entry's id, as the browser made it. */
export const plannerEntryId = z
  .string()
  .min(1)
  .max(64)
  .regex(/^[A-Za-z0-9_-]+$/);

/** HH:MM on the park's clock, as the planner stores it. */
export const plannerClock = z.string().regex(/^([01]\d|2[0-3]):[0-5]\d$/);

/** What the planner knows about the party, per day. */
export const plannerPrefs = z.object({
  riderHeightCm: z.number().int().min(50).max(250).optional(),
  avoidWet: z.boolean().optional(),
  earlyEntry: z.boolean().optional(),
});

/**
 * The visitor's trip planner as the browser holds it, sent with every question so Parky can read
 * and change it. Not strict below the top: a field the planner adds later must not fail the chat;
 * unknown keys are dropped.
 */
export const plannerSnapshotSchema = z.object({
  active: z.object({ park: slug, date: z.iso.date() }).optional(),
  days: z
    .array(
      z.object({
        park: z.object({ slug, name: plannerName }),
        date: z.iso.date(),
        prefs: plannerPrefs.optional(),
        entries: z
          .array(
            z.object({
              id: plannerEntryId,
              kind: z.enum(["ride", "show", "block"]),
              name: plannerName,
              rideSlug: slug.optional(),
              start: plannerClock.optional(),
              durationMinutes: z.number().int().min(1).max(720).optional(),
              done: z.boolean().optional(),
            }),
          )
          .max(40),
      }),
    )
    .max(6),
});
export type PlannerSnapshot = z.infer<typeof plannerSnapshotSchema>;
export type PlannerDay = PlannerSnapshot["days"][number];

/**
 * The browser's push state and the alerts it holds, sent with every question. Not strict below
 * the top, like the planner. At most 100 of each, the push endpoints' own cap per browser.
 */
export const alertsSnapshotSchema = z.object({
  push: z.enum(["ready", "needs-permission", "unavailable"]),
  rides: z
    .array(
      z.object({
        park: slug,
        rideSlug: slug,
        name: plannerName,
        kind: z.enum(["wait_below", "reopen"]),
        thresholdMinutes: z
          .number()
          .int()
          .min(RIDE_ALERT_THRESHOLD_MIN)
          .max(RIDE_ALERT_THRESHOLD_MAX)
          .optional(),
      }),
    )
    .max(100)
    .default([]),
  shows: z
    .array(
      z.object({
        park: slug,
        showSlug: slug,
        name: plannerName,
        start: z.string().max(40).optional(),
      }),
    )
    .max(100)
    .default([]),
});
export type AlertsSnapshot = z.infer<typeof alertsSnapshotSchema>;

/** The kinds a visitor can favourite, as `set_favorite` and the snapshot name them. */
export const FAVORITE_ENTITIES = [
  "park",
  "attraction",
  "show",
  "restaurant",
] as const;
export type FavoriteEntity = (typeof FAVORITE_ENTITIES)[number];

const favoriteIds = z.array(z.uuid()).max(50).default([]);

/** The ids of the visitor's favourites, as the browser stores them. */
export const favoritesSnapshotSchema = z.object({
  parks: favoriteIds,
  attractions: favoriteIds,
  shows: favoriteIds,
  restaurants: favoriteIds,
});
export type FavoritesSnapshot = z.infer<typeof favoritesSnapshotSchema>;

/** The token `POST /v1/assistant/session` hands out after the challenge. */
const sessionToken = z.string().max(200);

/** The challenge's answer: a solved Turnstile token. */
export const sessionRequestSchema = z.strictObject({
  turnstileToken: z.string().min(1).max(2048),
});

/**
 * One question. The history is not sent: the server keeps it, so a visitor
 * cannot put words into Parky's mouth by sending made-up earlier answers.
 */
export const chatRequestSchema = z.strictObject({
  conversationId: z.uuid().optional(),
  locale: z.enum(ASSISTANT_LOCALES),
  pagePath,
  message: z.string().trim().min(1).max(1000),
  sessionToken,
  /** The browser's IANA zone, e.g. Europe/Berlin; an unknown one counts as UTC. */
  timeZone: z
    .string()
    .max(64)
    .regex(/^[A-Za-z_+\-/0-9]+$/)
    .optional(),
  /**
   * Sent only after the visitor pressed „Standort teilen" in the chat; rounded here to two
   * decimals (about a kilometre), never stored or logged.
   */
  location: z
    .strictObject({
      lat: z.number().min(-90).max(90),
      lng: z.number().min(-180).max(180),
    })
    .transform(({ lat, lng }) => ({
      lat: Math.round(lat * 100) / 100,
      lng: Math.round(lng * 100) / 100,
    }))
    .optional(),
  planner: plannerSnapshotSchema.optional(),
  alerts: alertsSnapshotSchema.optional(),
  favorites: favoritesSnapshotSchema.optional(),
});
export type ChatRequest = z.infer<typeof chatRequestSchema>;

export const ratingRequestSchema = z.strictObject({
  conversationId: z.uuid(),
  messageId: z.uuid(),
  rating: z.union([z.literal(1), z.literal(-1)]),
  sessionToken,
});

export const feedbackRequestSchema = z.strictObject({
  message: z.string().trim().min(3).max(2000),
  email: z.email().max(254).optional().or(z.literal("")),
  locale: z.enum(ASSISTANT_LOCALES),
  pagePath,
  conversationId: z.uuid().optional(),
  sessionToken,
});

export const settingsUpdateSchema = z.strictObject({
  enabled: z.boolean().optional(),
  rolloutPercent: z.number().int().min(0).max(100).optional(),
  dailyTokenBudget: z.number().int().min(0).max(1_000_000_000).optional(),
  apiKey: z
    .string()
    .trim()
    .regex(/^sk-ant-[A-Za-z0-9_-]{20,}$/, "is not a Claude API key")
    .nullable()
    .optional(),
});

export const feedbackStatusSchema = z.strictObject({
  status: z.enum(["new", "done", "archived"]),
});
