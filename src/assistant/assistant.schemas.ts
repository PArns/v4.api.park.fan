import { z } from "zod";
import { ASSISTANT_LOCALES } from "./assistant-locales";
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

/** A name the browser typed or stored, on one line and cut short: it reaches the model as data. */
const plannerName = z
  .string()
  .max(2000)
  .transform((value) => value.replace(/\s+/g, " ").trim().slice(0, 80));

/**
 * The browser's snapshots come from its own storage (a cookie, localStorage), which an old
 * version or a hand-edited value fills with anything. A bad entry is dropped and a long list cut,
 * so a stale cookie costs Parky one entry, never the whole chat.
 */
/** The most live trip regions a request names. */
export const LIVE_REGIONS_MAX = 20;

function lenientList<T extends z.ZodType>(item: T, max: number) {
  // Optional first, so a missing list reaches the transform and comes out empty.
  return z
    .unknown()
    .optional()
    .transform((value): z.output<T>[] =>
      (Array.isArray(value) ? value : [])
        .map((entry) => item.safeParse(entry))
        .filter((parsed) => parsed.success)
        .map((parsed) => parsed.data as z.output<T>)
        .slice(0, max),
    );
}

/** A value that does not parse counts as absent instead of failing the request. */
function lenient<T extends z.ZodType>(schema: T) {
  return (
    z
      .unknown()
      .transform((value): z.output<T> | undefined => {
        const parsed = schema.safeParse(value);
        return parsed.success ? (parsed.data as z.output<T>) : undefined;
      })
      // Optional, so the key is optional in the parsed type as well.
      .optional()
  );
}

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

/** The planned days a question carries in full: the active one and the nearest. */
export const PLANNER_DETAILED_DAYS = 8;
/** Entries per detailed day. */
export const PLANNER_DAY_ENTRIES = 40;
/** The other planned days a question names by park and date only. */
export const PLANNER_INDEX_DAYS = 60;

/**
 * The visitor's day planner as the browser holds it, sent with every question so Parky can read
 * and change it. Unknown keys are dropped, a bad day or entry too (see {@link lenientList}): at
 * most {@link PLANNER_DETAILED_DAYS} days of {@link PLANNER_DAY_ENTRIES} entries in full, and the
 * rest of the planned days from today on in `more`, by park and date.
 */
export const plannerSnapshotSchema = z.object({
  active: lenient(z.object({ park: slug, date: z.iso.date() })),
  days: lenientList(
    z.object({
      park: z.object({ slug, name: plannerName }),
      date: z.iso.date(),
      prefs: lenient(plannerPrefs),
      entries: lenientList(
        z.object({
          id: plannerEntryId,
          kind: z.enum(["ride", "show", "block"]),
          name: plannerName,
          rideSlug: lenient(slug),
          showSlug: lenient(slug),
          start: lenient(plannerClock),
          /** Park-local minutes since midnight, past 1440 on a day that runs past midnight. */
          startMinute: lenient(z.number().int().min(0).max(2880)),
          durationMinutes: lenient(z.number().int().min(1).max(720)),
          done: lenient(z.boolean()),
        }),
        PLANNER_DAY_ENTRIES,
      ),
    }),
    PLANNER_DETAILED_DAYS,
  ),
  more: lenientList(
    z.object({
      park: z.object({ slug, name: plannerName }),
      date: z.iso.date(),
      entries: z.number().int().min(0).max(500),
      done: lenient(z.number().int().min(0).max(500)),
      reserved: lenient(z.boolean()),
      riderHeightCm: lenient(plannerPrefs.shape.riderHeightCm),
    }),
    PLANNER_INDEX_DAYS,
  ),
});
type ParsedPlanner = z.infer<typeof plannerSnapshotSchema>;
/** The snapshot as the tools read it; `more` is optional for snapshots built by hand. */
export type PlannerSnapshot = Omit<ParsedPlanner, "more"> &
  Partial<Pick<ParsedPlanner, "more">>;
export type PlannerDay = PlannerSnapshot["days"][number];
export type PlannerIndexDay = ParsedPlanner["more"][number];

/** The longest trip query a question carries: a 21-day trip with pins and six tickets fits. */
export const TRIP_QUERY_MAX = 1200;

/** What the browser remembers of the party for a trip: how many, and the children's heights. */
export const tripPartySchema = z.object({
  size: lenient(z.number().int().min(1).max(30)),
  heightsCm: lenientList(z.number().int().min(50).max(250), 10),
});
export type TripParty = z.infer<typeof tripPartySchema>;

/**
 * The visitor's trips, one per region: the hub planner's own query (its whole state, in the plan
 * route's names) and whether that planner is on screen now. The query is checked here only for
 * its characters; the tools read it through the plan route's own normalization.
 */
export const tripsSnapshotSchema = lenientList(
  z.object({
    region: slug,
    query: z
      .string()
      .max(TRIP_QUERY_MAX)
      .regex(/^[A-Za-z0-9=&:,@._%+?-]*$/),
    onScreen: lenient(z.boolean()),
    party: lenient(tripPartySchema),
  }),
  4,
);
export type TripSnapshot = z.infer<typeof tripsSnapshotSchema>[number];

/**
 * The interview goes from simple to detailed: the core steps first, in this order, then the plan
 * at once, then the refinements, which Parky offers as replies and the visitor may pick or leave
 * (`refine`). A visitor who stops after the core has a plan.
 */
export const TRIP_INTERVIEW_CORE = ["region", "dates", "parks"] as const;
/** A trip's refinements, offered once its plan stands. */
export const TRIP_INTERVIEW_REFINE = [
  "party",
  "tickets",
  "hotel",
  "pace",
  "file",
] as const;
/** A park day's core: the park and date, the must-dos. */
export const DAY_INTERVIEW_CORE = ["day", "mustdo"] as const;
/** A park day's refinements, offered once the day is planned. */
export const DAY_INTERVIEW_REFINE = ["heights", "shows", "arrival"] as const;
/** The phase after the core: the plan stands, the refinements are on offer. */
export const INTERVIEW_REFINE = "refine";
const interviewStep = z.enum([
  ...new Set([
    ...TRIP_INTERVIEW_CORE,
    ...TRIP_INTERVIEW_REFINE,
    ...DAY_INTERVIEW_CORE,
    ...DAY_INTERVIEW_REFINE,
    INTERVIEW_REFINE,
  ]),
] as [string, ...string[]]);
export type InterviewStep =
  | (typeof TRIP_INTERVIEW_CORE)[number]
  | (typeof TRIP_INTERVIEW_REFINE)[number]
  | (typeof DAY_INTERVIEW_CORE)[number]
  | (typeof DAY_INTERVIEW_REFINE)[number]
  | typeof INTERVIEW_REFINE;

/**
 * Parky's interview as the browser holds it beside the chat: which kind, the step being asked,
 * the steps answered or skipped, and the must-do rides of a trip, which wait for its filing into
 * the day planner. Everything else the visitor answered is in the planners already.
 */
export const interviewSnapshotSchema = z.object({
  kind: z.enum(["trip", "day"]),
  region: lenient(slug),
  park: lenient(slug),
  date: lenient(z.iso.date()),
  step: interviewStep,
  answered: lenientList(interviewStep, 12),
  skipped: lenientList(interviewStep, 12),
  mustDoRides: lenientList(
    z.object({ park: slug, rides: lenientList(z.uuid(), 12) }),
    8,
  ),
  source: z.enum(["planner", "chat"]).catch("chat"),
});
export type InterviewSnapshot = z.infer<typeof interviewSnapshotSchema>;

/**
 * The browser's push state and the alerts it holds, sent with every question. Lenient like the
 * planner; at most 100 of each, the push endpoints' own cap per browser. An unknown push state
 * reads as "unavailable", so Parky offers nothing it cannot deliver.
 */
export const alertsSnapshotSchema = z.object({
  push: z
    .unknown()
    .optional()
    .transform((value) =>
      value === "ready" || value === "needs-permission"
        ? value
        : ("unavailable" as const),
    ),
  rides: lenientList(
    z.object({
      park: slug,
      rideSlug: slug,
      name: plannerName,
      kind: z.enum(["wait_below", "reopen"]),
      thresholdMinutes: lenient(
        z
          .number()
          .int()
          .min(RIDE_ALERT_THRESHOLD_MIN)
          .max(RIDE_ALERT_THRESHOLD_MAX),
      ),
    }),
    100,
  ),
  shows: lenientList(
    z.object({
      park: slug,
      showSlug: slug,
      name: plannerName,
      start: lenient(z.string().max(40)),
    }),
    100,
  ),
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

const favoriteIds = lenientList(z.uuid(), 50);

/** The ids of the visitor's favourites, as the browser's cookie holds them; lenient like the rest. */
export const favoritesSnapshotSchema = z.object({
  parks: favoriteIds,
  attractions: favoriteIds,
  shows: favoriteIds,
  restaurants: favoriteIds,
});
export type FavoritesSnapshot = z.infer<typeof favoritesSnapshotSchema>;

/**
 * The most rides a question names as ridden. The browser keeps them for every park the visitor
 * ever marked, and only the parks of this chat matter, so a long list is cut rather than refused.
 */
export const RIDDEN_SNAPSHOT_MAX = 300;

/** The ids of the rides the visitor marked as ridden, as the browser keeps them. */
export const riddenSnapshotSchema = lenientList(z.uuid(), RIDDEN_SNAPSHOT_MAX);

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
  planner: lenient(plannerSnapshotSchema),
  /**
   * The line the chat opened with, sent with a chat's first question only: a greeting can name the
   * user's next planned day, and "yes" may answer it. One line, at most 300 characters; data.
   */
  opener: lenient(
    z
      .string()
      .max(2000)
      .transform((value) => value.replace(/\s+/g, " ").trim().slice(0, 300))
      .pipe(z.string().min(1)),
  ),
  trips: tripsSnapshotSchema.optional(),
  /**
   * The trip regions live on park.fan, as the site's region registry marks them: the one switch a
   * region flips at go-live, which also opens its hub. Parky plans, changes and links these only.
   * Absent (a widget from before 2.21.0): every region, as before.
   */
  liveRegions: lenient(
    z
      .array(z.string().regex(/^[a-z0-9-]{1,40}$/))
      .max(LIVE_REGIONS_MAX)
      .transform((slugs) => [...new Set(slugs)]),
  ),
  interview: lenient(interviewSnapshotSchema),
  alerts: lenient(alertsSnapshotSchema),
  favorites: lenient(favoritesSnapshotSchema),
  ridden: riddenSnapshotSchema.optional(),
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
