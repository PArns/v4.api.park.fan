/**
 * Resolves the paid queue-jump product a ride sells, out of the two rows it is
 * spread across.
 *
 * The name is a park-wide brand and lives on the park; the flag and the price
 * live on the attraction. That split is what keeps "QuickPass" typed once
 * rather than forty times, and it is the reason this is a function: a `??`
 * chain reaching across two entities would be copied into the attraction DTO,
 * the park's attraction list and the admin, and the three copies would drift.
 * `resolveCuratedFacts` next door exists after exactly that happened.
 *
 * No sync writes any of it. No feed we ingest publishes queue-jump products —
 * they live in park apps and on ticket pages — so every value here is
 * hand-written and comes from the park's own pages.
 */

/** What a ride's product is called when nobody has named it. */
export const DEFAULT_FAST_PASS_NAME = "Fast Pass";

/**
 * The currencies a park's prices may be quoted in.
 *
 * A closed list rather than free text: the code reaches the frontend and is
 * handed to `Intl.NumberFormat`, which throws on an invalid one — a typo in
 * the admin would blank a ride page rather than show an odd currency.
 */
export const SUPPORTED_CURRENCIES = [
  "EUR",
  "USD",
  "GBP",
  "CHF",
  "DKK",
  "SEK",
  "NOK",
  "PLN",
  "CZK",
  "HUF",
  "CAD",
  "AUD",
  "JPY",
  "CNY",
  "KRW",
  "AED",
  "MXN",
  "BRL",
] as const;

export type SupportedCurrency = (typeof SUPPORTED_CURRENCIES)[number];

export interface FastPassAttractionSource {
  hasFastPass?: boolean | null;
  fastPassName?: string | null;
  fastPassPrice?: number | null;
}

export interface FastPassParkSource {
  curatedFastPassName?: string | null;
  curatedCurrency?: string | null;
  curatedFastPassTermId?: string | null;
  curatedFastPassPriceFrom?: number | null;
}

export interface ResolvedFastPass {
  /** Never empty — the park's brand, the ride's override, or "Fast Pass". */
  name: string;
  /**
   * What it costs. **0 means free** — do not test this field for truthiness.
   *
   * Null is "unknown", which includes the products priced per day by the
   * operator. See `resolvePrice` for all three states.
   */
  price: number | null;
  /** ISO-4217. Null means the price is null too. */
  currency: SupportedCurrency | null;
  /**
   * The park's entry price for the product, where it is not priced per ride.
   *
   * Null whenever `price` is set: they answer the same question, and a chip
   * carrying both a price and an "ab" price says one of them is wrong. This is
   * what nearly every park has, because nearly every park sells one pass for
   * the visit rather than one per ride.
   */
  priceFrom: number | null;
  /**
   * The glossary term explaining the product, for a link out of the chip.
   *
   * From the park even when a single ride overrides the name: Disney's Single
   * Pass and Multi Pass are two products under one glossary entry, and a ride
   * with a different label still buys the same idea.
   */
  termId: string | null;
}

function cleaned(value: string | null | undefined): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * The park's currency, or null.
 *
 * Tolerant of case because an editor types "eur" as readily as "EUR", strict
 * about the list for the reason `SUPPORTED_CURRENCIES` gives.
 */
export function resolveCurrency(
  park: FastPassParkSource | null | undefined,
): SupportedCurrency | null {
  const code = cleaned(park?.curatedCurrency)?.toUpperCase();
  if (!code) return null;
  return (SUPPORTED_CURRENCIES as readonly string[]).includes(code)
    ? (code as SupportedCurrency)
    : null;
}

/**
 * Three states, and the third is why this is not a plain number.
 *
 * **Empty** is "nobody has priced it" — and stays empty on the products the
 * operator prices per day (Disney, Universal), where a frozen number would be
 * wrong most days.
 *
 * **Zero is free**, a positive claim and not a missing value: Europa-Park's
 * Virtual Line is a queue-jump product included with admission, and "there is
 * one and it costs nothing" is a different fact from "there is one and we do
 * not know the price". The same reading a curated height of 0 gets next door,
 * for the same reason — see `resolveHeight`.
 *
 * **A positive price needs a currency.** A ride page rendering a bare "12"
 * states nothing, so the number is withheld until somebody has said what it is
 * denominated in. Free needs none: nothing costs 0 EUR differently from 0 USD.
 *
 * The `Number()` is not defensive noise. The column is `double precision` so
 * TypeORM hands over a number, but the same rows are read by raw queries and
 * projections that return strings, and this function is the one place that
 * knows what the value means.
 */
function resolvePrice(
  raw: number | string | null | undefined,
  currency: SupportedCurrency | null,
): number | null {
  if (raw === null || raw === undefined || raw === "") return null;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) return null;
  if (value === 0) return 0;
  if (currency === null) return null;
  return Math.round(value * 100) / 100;
}

/**
 * The product this ride sells, or null when it sells none we can vouch for.
 *
 * `null` and a curated `false` both come out as null, and that is deliberate:
 * "no source has told us" and "we checked and there is none" are different
 * facts to an editor and the same absence to a visitor. Publishing the second
 * as "kein QuickPass" would be our own bookkeeping presented as the park's
 * statement — see the rule in CLAUDE.md §4. The admin keeps all three states.
 */
export function resolveFastPass(
  attraction: FastPassAttractionSource,
  park: FastPassParkSource | null | undefined,
): ResolvedFastPass | null {
  if (attraction.hasFastPass !== true) return null;

  const currency = resolveCurrency(park);
  const price = resolvePrice(attraction.fastPassPrice, currency);

  return {
    name:
      cleaned(attraction.fastPassName) ??
      cleaned(park?.curatedFastPassName) ??
      DEFAULT_FAST_PASS_NAME,
    price,
    // Suppressed once the ride has a price of its own: "QuickPass: 12 € (ab
    // 25 €)" is not more information, it is two answers to one question.
    priceFrom:
      price === null
        ? resolvePrice(park?.curatedFastPassPriceFrom, currency)
        : null,
    currency,
    termId: cleaned(park?.curatedFastPassTermId),
  };
}
