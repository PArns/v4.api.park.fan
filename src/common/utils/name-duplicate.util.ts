import { generateSlug } from "./slug.util";

/**
 * The two columns the choice is allowed to read.
 *
 * Deliberately no status, no queue rows, no timestamps: the surviving slug is a
 * public URL, and anything that changes between two crawls makes it flicker.
 * See `chooseNameDuplicateWinner`.
 */
export interface NameDuplicateRow {
  slug: string;
  name: string;
}

/** The counter `generateUniqueSlug` appends when a slug is already taken. */
const SLUG_COUNTER = /-[0-9]+$/;

/**
 * Where a slug sits in the ranking. Lower wins, compared element by element.
 *
 * 1. **No `-N` counter.** A counter means `generateUniqueSlug` handed this row
 *    the leftover name, so the row without one holds the slug the park was
 *    first published under — and the one the sitemap has been advertising.
 *    Measured 2026-09-26 against production: this alone settles 42 of the 45
 *    duplicate name groups.
 * 2. **The slug `generateSlug` would produce for this name.** Settles groups
 *    where every candidate carries a counter (Walibi Holland's "Walibi Express
 *    Station 2" against `walibi-express-station-2-2`) and groups where none
 *    does — Sea World serves "Wally the Walrus" from `wally-the-walrus` rather
 *    than from `castaway-bay-sky-climb`, which is a different ride's name.
 * 3. **The slug itself, ascending.** Only so the order is total. Without it a
 *    remainder is decided by the order the rows arrive in, and that is the
 *    property this whole function exists to remove.
 */
function rank(row: NameDuplicateRow): [number, number, string] {
  let canonical: string | null = null;
  try {
    canonical = generateSlug(row.name);
  } catch {
    // `generateSlug` throws on a name that slugifies to nothing. Then stage 2
    // simply has nothing to say and stage 3 decides.
    canonical = null;
  }

  return [
    SLUG_COUNTER.test(row.slug) ? 1 : 0,
    canonical !== null && row.slug === canonical ? 0 : 1,
    row.slug,
  ];
}

/**
 * Whether `candidate` should replace `incumbent` as the row served for a name.
 *
 * The park payload serves one row per attraction name, and the attraction
 * sitemap has to name the same slug or it advertises a 404. That only holds if
 * both sides decide the same way, so both call this — there is one rule and not
 * two.
 *
 * It used to be "prefer OPERATING, then prefer coordinates", which reads the
 * live feed: whichever row happened to be open when the payload was built kept
 * the name, so the surviving slug changed during the day. Measured 2026-09-26
 * over all 201 parks in the sitemap: 48 sitemap slugs had no row in the park
 * payload, exactly the 48 losers of the 45 duplicate name groups, and which 48
 * they were moved with the feed (`docs/seo/analysis.md`: ~400 flickering 404s).
 *
 * Nothing is lost by dropping the status: the choice is between two rows of the
 * same ride under the same name, and a reader who wants the other row's live
 * data has no way to ask for it either way. Whether the two rows ARE one ride
 * is a curation question and not one a grouping key can answer — see
 * `docs/architecture/attraction-status-and-seasonality.md` §4a.
 */
export function outranksNameDuplicate(
  candidate: NameDuplicateRow,
  incumbent: NameDuplicateRow,
): boolean {
  const a = rank(candidate);
  const b = rank(incumbent);

  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return a[i] < b[i];
  }

  return false;
}

/**
 * The row that survives a group of same-name rows, order-independently.
 *
 * Returns `undefined` for an empty group so a caller can tell "nothing here"
 * from "the first one".
 */
export function chooseNameDuplicateWinner<T extends NameDuplicateRow>(
  rows: readonly T[],
): T | undefined {
  return rows.reduce<T | undefined>(
    (best, row) =>
      best === undefined || outranksNameDuplicate(row, best) ? row : best,
    undefined,
  );
}
