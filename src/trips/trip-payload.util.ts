/**
 * What this API will accept as a trip, and what it will not.
 *
 * `POST /v1/trips` is the first unauthenticated write endpoint in this API.
 * Anything that takes JSON from the open internet and hands it back on a URL is
 * a free key-value store — for a phishing page's payload, for a tracker's
 * bounce, for whatever needs a domain with a good reputation in front of it —
 * and it *will* be found. The defence is not a schema validator over every
 * field, which would put the planner's own shape in two repositories and
 * guarantee they disagree at the next release. It is a floor: the thing has to
 * be shaped like a plan, and it has to be small.
 *
 * Pure, and separate from the service, so the rule can be tested against a real
 * payload rather than through a controller.
 */

/**
 * The cap, in bytes of UTF-8.
 *
 * A large plan — ten parks, ten days each, twenty entries a day, every entry
 * carrying a ride name and a label — measures well under 100 KB. 256 KB leaves
 * room for a planner that grows without leaving room for a file host.
 */
export const TRIP_MAX_BYTES = 256 * 1024;

/** No plan has this many parks in it, and a store abusing the endpoint would. */
const MAX_PARKS = 60;

/** Sixty days is the planner's own horizon; a park's days cannot outrun it by much. */
const MAX_DAYS_PER_PARK = 400;

const MAX_ENTRIES_PER_DAY = 200;

export interface TripPayloadVerdict {
  ok: boolean;
  /** Why not, for the 400. Never echoes the payload back. */
  reason?: string;
}

/** Bytes of UTF-8 the payload occupies once serialised. */
export function tripPayloadBytes(payload: unknown): number {
  return Buffer.byteLength(JSON.stringify(payload ?? null), "utf8");
}

/**
 * Whether this is a plan.
 *
 * Checks the skeleton the planner has always written and nothing below it: a
 * version, a map of parks, each with a slug and a map of days, each day with a
 * date and a list of entries carrying an id and a start minute. Everything a
 * plan may additionally hold — a ride's name, a custom block's icon, the park's
 * timezone — is passed through untouched, because this API does not own that
 * shape and pretending to would break the planner on the day it grows a field.
 *
 * The counts are not really about validity. They are about a payload that
 * passes every structural check and is still a megabyte of nested nonsense.
 */
export function checkTripPayload(payload: unknown): TripPayloadVerdict {
  if (
    payload === null ||
    typeof payload !== "object" ||
    Array.isArray(payload)
  ) {
    return { ok: false, reason: "not an object" };
  }

  const bytes = tripPayloadBytes(payload);
  if (bytes > TRIP_MAX_BYTES) {
    return { ok: false, reason: `too large (${bytes} bytes)` };
  }

  const trip = payload as Record<string, unknown>;
  if (typeof trip.version !== "number") {
    return { ok: false, reason: "no version" };
  }

  const parks = trip.parks;
  if (parks === null || typeof parks !== "object" || Array.isArray(parks)) {
    return { ok: false, reason: "no parks" };
  }

  const parkEntries = Object.entries(parks as Record<string, unknown>);
  if (parkEntries.length > MAX_PARKS) {
    return { ok: false, reason: "too many parks" };
  }

  for (const [slug, raw] of parkEntries) {
    if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
      return { ok: false, reason: `park ${slug} is not an object` };
    }
    const park = raw as Record<string, unknown>;
    if (typeof park.slug !== "string" || park.slug.length === 0) {
      return { ok: false, reason: `park ${slug} has no slug` };
    }

    const days = park.days;
    // A park with no days is legal: the planner writes one the moment somebody
    // opens a day, before anything is in it.
    if (days === undefined || days === null) continue;
    if (typeof days !== "object" || Array.isArray(days)) {
      return { ok: false, reason: `park ${slug} has a malformed days map` };
    }

    const dayEntries = Object.entries(days as Record<string, unknown>);
    if (dayEntries.length > MAX_DAYS_PER_PARK) {
      return { ok: false, reason: `park ${slug} has too many days` };
    }

    for (const [key, rawDay] of dayEntries) {
      if (
        rawDay === null ||
        typeof rawDay !== "object" ||
        Array.isArray(rawDay)
      ) {
        return { ok: false, reason: `day ${key} is not an object` };
      }
      const day = rawDay as Record<string, unknown>;
      if (
        typeof day.date !== "string" ||
        !/^\d{4}-\d{2}-\d{2}$/.test(day.date)
      ) {
        return { ok: false, reason: `day ${key} has no date` };
      }
      const entries = day.entries;
      if (entries === undefined || entries === null) continue;
      if (!Array.isArray(entries)) {
        return { ok: false, reason: `day ${key} has a malformed entry list` };
      }
      if (entries.length > MAX_ENTRIES_PER_DAY) {
        return { ok: false, reason: `day ${key} has too many entries` };
      }
      for (const rawEntry of entries) {
        if (
          rawEntry === null ||
          typeof rawEntry !== "object" ||
          Array.isArray(rawEntry)
        ) {
          return { ok: false, reason: `day ${key} has a malformed entry` };
        }
        const entry = rawEntry as Record<string, unknown>;
        if (typeof entry.id !== "string" || entry.id.length === 0) {
          return { ok: false, reason: `day ${key} has an entry with no id` };
        }
        if (
          typeof entry.startMinute !== "number" ||
          !Number.isFinite(entry.startMinute)
        ) {
          return { ok: false, reason: `day ${key} has an entry with no start` };
        }
      }
    }
  }

  return { ok: true };
}
