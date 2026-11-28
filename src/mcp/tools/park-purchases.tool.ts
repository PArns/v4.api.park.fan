import { z } from "zod";
import { DataSource, IsNull } from "typeorm";
import { defineMcpTool } from "../mcp-tool";
import { isAmbiguous, nameKey, resolvePark } from "./park-ref";
import { ParksService } from "../../parks/parks.service";
import { ScheduleItemDto } from "../../parks/dto/schedule-item.dto";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { resolveAttractionName } from "../../attractions/utils/curated-attraction-facts.util";
import { parseDateRange } from "../../common/utils/date-parsing.util";
import {
  addIsoDays,
  getCurrentDateInTimezone,
  isRealIsoDate,
  ISO_DATE_PATTERN,
} from "../../common/utils/date.util";

/** The source publishes about 30 days ahead; two months covers a late look. */
const MAX_DAYS = 60;

/** `/schedule`'s own default window: today and 30 days on. */
const DEFAULT_DAYS = 31;

/**
 * A ride name shorter than this is too common to find inside a product's name
 * ("Lightning Lane for …") without naming the wrong ride.
 */
const MIN_RIDE_KEY = 4;

/**
 * One purchase as the schedule row stores what ThemeParks.wiki sent. The
 * column is the source's JSON, so each item is checked and one that does not
 * fit is left out rather than passed on.
 */
const sourcePurchase = z.object({
  name: z.string().min(1).max(300),
  type: z.string().max(40).optional(),
  price: z
    .object({
      amount: z.number(),
      currency: z.string().min(1).max(10),
      formatted: z.string().max(40).optional(),
    })
    .nullish(),
  available: z.boolean().optional(),
});
type SourcePurchase = z.infer<typeof sourcePurchase>;

/** A live ride of the park, by the names a product may call it. */
type Ride = Pick<Attraction, "id" | "name" | "curatedName">;

/**
 * The price a reader is shown, or null where the source sent a placeholder
 * (amount 0, "Unknown") because the fee is not known: the website's rule for
 * the same products.
 */
function priceOf(price: SourcePurchase["price"]) {
  if (!price || price.amount <= 0) return null;
  const formatted = price.formatted?.trim();
  if (!formatted || formatted.toLowerCase() === "unknown") return null;
  return { amount: price.amount, currency: price.currency, formatted };
}

/**
 * The live ride a per-ride product is sold for: the longest ride name found
 * inside the product's ("Lightning Lane for TRON Lightcycle / Run"), by the
 * curated name or the synced one. The source names the ride and gives no id of
 * ours, so a product whose ride is not matched says null.
 */
export function rideOfProduct(
  product: string,
  rides: readonly Ride[],
): { id: string; name: string } | null {
  const key = nameKey(product);
  let best: Ride | null = null;
  let bestLength = 0;
  for (const ride of rides) {
    for (const name of [resolveAttractionName(ride), ride.name]) {
      const rideKey = nameKey(name);
      if (
        rideKey.length >= MIN_RIDE_KEY &&
        rideKey.length > bestLength &&
        key.includes(rideKey)
      ) {
        best = ride;
        bestLength = rideKey.length;
      }
    }
  }
  return best ? { id: best.id, name: resolveAttractionName(best) } : null;
}

/** A day's products for the model: packages first, then the per-ride ones A→Z. */
function productsOf(items: readonly SourcePurchase[], rides: readonly Ride[]) {
  return [...items]
    .sort((a, b) => {
      const rank = (item: SourcePurchase) => (item.type === "PACKAGE" ? 0 : 1);
      return rank(a) - rank(b) || a.name.localeCompare(b.name);
    })
    .map((item) => ({
      name: item.name,
      type: item.type ?? null,
      price: priceOf(item.price),
      available: item.available ?? null,
      ...(item.type === "ATTRACTION" && {
        attraction: rideOfProduct(item.name, rides),
      }),
    }));
}

export const parkPurchasesTool = defineMcpTool({
  name: "park_purchases",
  title: "Paid queue products per day",
  description:
    "The paid queue products one park sells per day: Disney's Lightning Lane Multi Pass and " +
    "Premier Pass, a Lightning Lane for a single ride, and the like, as ThemeParks.wiki sends " +
    "them with the opening hours (the schedule's `purchases`, which park.fan's day sheet reads " +
    "too). Per park-local day: each product's name, type (PACKAGE for a pass across rides, " +
    "ATTRACTION for one ride), price (amount in the currency's minor unit as the source sends " +
    "it, e.g. 1600 for $16.00, with currency and the source's formatted text; null where the " +
    "source has no real price), available (false: sold out that day; null: not said) and, for " +
    "a per-ride product, the ride it is for (id and name, matched by name; null when no live " +
    "ride matches). Days without products are left out, and most parks have none; the source " +
    "reaches about 30 days ahead. Pass the park id from find_parks, or its slug when that is " +
    "unambiguous.",
  input: {
    park: z
      .string()
      .min(2)
      .max(120)
      .describe(
        'Park id (uuid) from find_parks, or a slug like "magic-kingdom-park"',
      ),
    from: z
      .string()
      .regex(ISO_DATE_PATTERN, "YYYY-MM-DD")
      .refine(isRealIsoDate, "a day the calendar has")
      .optional()
      .describe("First park-local day, YYYY-MM-DD; the park's today if absent"),
    days: z
      .number()
      .int()
      .min(1)
      .max(MAX_DAYS)
      .default(DEFAULT_DAYS)
      .describe("Days from `from` on, `from` included"),
  },
  async handler({ park: reference, from, days }, ctx) {
    const found = await resolvePark(ctx, reference);
    if (isAmbiguous(found)) return found;
    const park = found;

    const first = from ?? getCurrentDateInTimezone(park.timezone);
    const last = addIsoDays(first, days - 1);
    const { fromDate, toDate } = parseDateRange(first, last, {
      timezone: park.timezone,
    });
    // The rows `/schedule` serves, read by the same query.
    const rows = await ctx
      .get(ParksService)
      .getSchedule(park.id, fromDate, toDate);

    // A day can have several rows; the products sit on one, so the first
    // row that carries any is the day's, as the website reads them.
    const byDate = new Map<string, SourcePurchase[]>();
    for (const row of rows) {
      const { date, purchases } = ScheduleItemDto.fromEntity(row);
      // getSchedule binds instants to a DATE column, which Postgres reads in
      // UTC, so it hands back a day beyond either edge of the park's window.
      if (date < first || date > last) continue;
      if (byDate.has(date) || !Array.isArray(purchases)) continue;
      const items = purchases.flatMap((item: unknown) => {
        const parsed = sourcePurchase.safeParse(item);
        return parsed.success ? [parsed.data] : [];
      });
      if (items.length > 0) byDate.set(date, items);
    }

    const perRide = [...byDate.values()].some((items) =>
      items.some((item) => item.type === "ATTRACTION"),
    );
    const rides: Ride[] = perRide
      ? await ctx
          .get(DataSource)
          .getRepository(Attraction)
          .find({
            where: { parkId: park.id, retiredAt: IsNull() },
            select: { id: true, name: true, curatedName: true },
          })
      : [];

    return {
      park: { id: park.id, name: park.name, timezone: park.timezone },
      from: first,
      to: last,
      days: [...byDate.entries()]
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([date, items]) => ({ date, products: productsOf(items, rides) })),
    };
  },
});
