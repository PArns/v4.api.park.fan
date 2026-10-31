import { z } from "zod";
import { ASSISTANT_LOCALES } from "./system-prompt";

/** A park.fan path: slug segments only, no query, host or scheme. */
const pagePath = z
  .string()
  .max(300)
  .regex(/^\/[a-z0-9\-/]*$/, "must be a path of slugs");

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
