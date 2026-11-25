/**
 * The photo a notification carries on a device.
 *
 * The media bucket's `index.json` (the file the app itself reads) maps a
 * park's or ride's geo path to an image id and lists each image's widths. The
 * server resolves the URL rather than the app's service extension: the
 * extension is a separate process without the app's cached index, and here the
 * licence rule and the off switch change without an app release.
 *
 * Never throws and never blocks a send for long: no index, no photo.
 */

/** The parts of `index.json` this reads. */
export interface MediaIndex {
  pattern?: string;
  parks?: Record<string, string>;
  rides?: Record<string, string>;
  images?: Record<
    string,
    { v?: string; widths?: number[]; attribution?: boolean }
  >;
}

/** Wide enough for the expanded notification on any iPhone, far below Apple's 10 MB for an image. */
export const PUSH_IMAGE_MIN_WIDTH = 1000;

const INDEX_TTL_MS = 30 * 60_000;
/** After a failed read, sends go out without photos this long before the next try. */
const RETRY_MS = 5 * 60_000;
const FETCH_TIMEOUT_MS = 4000;

function mediaBase(): string {
  return process.env.NEWS_EXPORT_BASE ?? "https://media.park.fan";
}

/**
 * The URL for `key`: a ride's geo path (`europe/germany/bruehl/phantasialand/taron`),
 * a park's, or an image id. `null` when there is no photo, or one that needs
 * its credit shown next to it (`attribution`): a notification has no place
 * for the line. The smallest width from `PUSH_IMAGE_MIN_WIDTH` up, else the largest.
 */
export function pickPushImage(
  index: MediaIndex,
  key: string,
  base = mediaBase(),
): string | null {
  const id = index.rides?.[key] ?? index.parks?.[key] ?? key;
  const image = index.images?.[id];
  const widths = (image?.widths ?? [])
    .filter((w) => Number.isInteger(w) && w > 0)
    .sort((a, b) => a - b);
  if (!image?.v || image.attribution !== false || widths.length === 0) {
    return null;
  }
  // Ids and versions are path segments of our own bucket; anything else is not a photo.
  if (!/^[a-z0-9/_-]+$/i.test(id) || !/^[a-z0-9]+$/i.test(image.v)) return null;
  const width =
    widths.find((w) => w >= PUSH_IMAGE_MIN_WIDTH) ?? widths[widths.length - 1];
  const file = (index.pattern ?? "{id}/{v}-{width}.webp")
    .replace("{id}", id)
    .replace("{v}", image.v)
    .replace("{width}", String(width));
  return `${base}/${file}`;
}

let cached: { index: MediaIndex | null; until: number } | null = null;

/** The photo URL for `key`, from the index kept for half an hour. `PUSH_IMAGES=false` sends none. */
export async function pushImageUrl(
  key: string,
  nowMs = Date.now(),
): Promise<string | null> {
  if ((process.env.PUSH_IMAGES ?? "true").trim() === "false") return null;
  if (!cached || cached.until <= nowMs) {
    // Claimed before the read, so a burst of sends makes one request.
    const previous = cached?.index ?? null;
    cached = { index: previous, until: nowMs + RETRY_MS };
    try {
      const response = await fetch(`${mediaBase()}/index.json`, {
        signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
      });
      if (response.ok) {
        cached = {
          index: (await response.json()) as MediaIndex,
          until: nowMs + INDEX_TTL_MS,
        };
      }
    } catch {
      // The older index, if any, serves until the next try.
    }
  }
  try {
    return cached.index ? pickPushImage(cached.index, key) : null;
  } catch {
    return null;
  }
}

/** For specs: forget the index. */
export function resetPushMediaCache(): void {
  cached = null;
}

/** `/parks/<geo path>[/<ride>][#…]` → the geo path, the media index's key. */
export function photoKeyOf(url: string): string | undefined {
  const path = url.split(/[?#]/)[0];
  return path.startsWith("/parks/") ? path.slice("/parks/".length) : undefined;
}
