import {
  type CuratedFactsSource,
  type ResolvedCuratedFacts,
  resolveCuratedFacts,
} from "./curated-attraction-facts.util";

/**
 * The rule that files a Halloween maze as `MAZE` without anybody typing it.
 *
 * `attraction_kind` is a hand-written verdict (see the type), and this is the
 * one corner of it a rule can reach: a walk-through horror attraction that
 * runs during a Halloween event leaves three marks at once — an event-horror
 * name, a season, and a season that falls in autumn. None of the three is
 * enough on its own. The name is not, because `Haunted Mansion` and
 * `Geisterbahn` run all year; the season is not, because a water park's whole
 * slide inventory is seasonal and stops in September.
 *
 * ## It only ever fills a blank
 *
 * The rule writes where `attraction_kind IS NULL` and nowhere else. An editor's
 * verdict is never overwritten, including the verdict "this is a RIDE" on
 * something whose name sounds like a maze. That is also what makes a wrong
 * guess cheap to correct: one admin write, and this rule never comes back to
 * it.
 *
 * ## What it reaches, measured
 *
 * On 2026-10-06 the rule selected 9 rows in 2 parks against production, all 9
 * genuine mazes. The limit is not the name list: of the 9 mazes a person had
 * already filed by hand at Movie Park Germany, 7 resolve as NOT seasonal, so
 * this rule would not have found them. `detect-seasonal` wants ≥ 20 observed
 * OPERATING rows and a current CLOSED from the feed, and a maze that runs
 * twelve evenings in October and then loses its wiki id reaches neither. The
 * remainder is hand work (PAR-727), and a pattern that keeps turning up there
 * belongs in the token list below rather than in another round of writes.
 */

/** Months a Halloween season can fall in, when no park season says otherwise. */
export const MAZE_SEASON_MONTHS: readonly number[] = [9, 10, 11];

/**
 * Event-horror words, matched at the start of a word in the resolved name.
 *
 * Calibrated against all 7,575 production attraction names on 2026-10-06, not
 * guessed, and two findings shaped it:
 *
 * - **The anchor is the start of a word.** Matched anywhere, `fear` claimed
 *   Liseberg's `AtmosFear` — a 116 m drop tower — and `evil` claimed every
 *   `D-evil`: Jersey Devil Coaster, Dare Devil Dive, Daredevil Falls, Lil'
 *   Devil Coaster, 8 rows in all. Anchored, both fall away and `Fear Acres`
 *   and `Evil Dead Burn` stay. Suffixes are still free, which is the point —
 *   `haunt` has to reach `Haunted`, `scare` has to reach `Scarezone`.
 * - **`voodoo` is deliberately absent.** All four rows carrying it are rides:
 *   Voodoo Drop (Six Flags America), Voodoo (Flamingo Land), Voodoo Bayou
 *   (Kennywood) and Voodoo Express — a Kentucky Kingdom water slide. A word
 *   that names a theme rather than an event does not belong here, however
 *   Halloween it sounds.
 *
 * Tokens stay plain lowercase letters: they are pasted into one alternation,
 * and a token carrying regex punctuation would change the pattern instead of
 * extending it. The spec holds that shape.
 */
export const MAZE_NAME_TOKENS: readonly string[] = [
  // English event horror
  "haunt",
  "scare",
  "scary",
  "fright",
  "horror",
  "nightmare",
  "slasher",
  "slaughter",
  "massacre",
  "murder",
  "torment",
  "zombie",
  "undead",
  "evil",
  "fear",
  "kill",
  "maze",
  "halloween",
  // German, Dutch, French, Spanish, Italian
  "spuk",
  "grusel",
  "gruwel",
  "hexenhaus",
  "horreur",
  "terror",
  "terreur",
  "terrore",
  "cauchemar",
  "pesadilla",
];

/**
 * The same pattern as a POSIX regex, for the rare caller that has to ask this
 * in SQL rather than over a loaded row.
 *
 * Nothing in this repository uses it yet — the rule runs in TypeScript over
 * rows it has already loaded, so there is one implementation of the match and
 * no twin to drift. It is exported because the measurement queries behind the
 * token list above need the identical pattern to stay reproducible.
 */
export function mazeNamePatternSql(): string {
  return `(^|[^[:alnum:]])(${MAZE_NAME_TOKENS.join("|")})`;
}

/** Whether a name carries an event-horror word at the start of a word. */
export function isMazeName(name: string): boolean {
  return MAZE_NAME_TOKENS.some((token) =>
    new RegExp(`(^|[^\\p{L}\\p{N}])${token}`, "iu").test(name),
  );
}

/**
 * Which months this attraction's season is known to touch, or null.
 *
 * Two sources, in this order, because only 18 of 1,098 seasonal rows in
 * production carry a month list at all:
 *
 * 1. the resolved month list, curated before detected;
 * 2. failing that, the month of `seasonOutSince` — the last park-local day the
 *    detector saw the ride running, which is the only thing a flagged ride
 *    with no months can say about itself.
 *
 * Null when neither exists. 633 seasonal rows are in that state, and the rule
 * refuses them: "seasonal, and we do not know when" is not evidence of
 * Halloween, and guessing here would file every winter-closed water slide as a
 * maze.
 */
export function mazeSeasonEvidenceMonths(
  facts: Pick<
    ResolvedCuratedFacts,
    "isSeasonal" | "seasonMonths" | "seasonOutSince"
  >,
): number[] | null {
  if (!facts.isSeasonal) return null;
  if (facts.seasonMonths && facts.seasonMonths.length > 0) {
    return [...facts.seasonMonths];
  }
  if (!facts.seasonOutSince) return null;
  const month = Number(facts.seasonOutSince.slice(5, 7));
  return Number.isInteger(month) && month >= 1 && month <= 12 ? [month] : null;
}

/**
 * Whether the rule files this row as `MAZE`.
 *
 * All three conditions, and the caller has already established that
 * `attraction_kind` is null.
 *
 * @param facts - Resolved curated facts for the row (curated before detected).
 * @param halloweenMonths - Months any non-cancelled `halloween` park season of
 *   this attraction's park spans. Empty for almost every park: `park_seasons`
 *   held no row at all on 2026-10-06, so this branch carries nothing until the
 *   first Halloween season is curated. It is here because a park's own dates
 *   beat a fixed window as soon as they exist — Europa-Park's Horror Nights
 *   reach into the last days of November.
 */
export function isMazeByRule(
  facts: Pick<
    ResolvedCuratedFacts,
    "name" | "isSeasonal" | "seasonMonths" | "seasonOutSince"
  >,
  halloweenMonths: readonly number[] = [],
): boolean {
  if (!isMazeName(facts.name)) return false;
  const evidence = mazeSeasonEvidenceMonths(facts);
  if (!evidence) return false;
  const window = new Set([...MAZE_SEASON_MONTHS, ...halloweenMonths]);
  return evidence.some((month) => window.has(month));
}

/** A row as the rule's query loads it, before the curated facts are resolved. */
export interface MazeRuleRow extends CuratedFactsSource {
  id: string;
  parkId: string;
  attractionKind: string | null;
}

/**
 * The ids the rule would write, out of the rows it was handed.
 *
 * Separate from the query so the decision is testable without a database, and
 * so `attraction_kind IS NULL` is asserted here as well as in the SQL — the
 * guarantee is cheap to state twice and expensive to lose once.
 */
export function selectMazeRows(
  rows: readonly MazeRuleRow[],
  halloweenMonthsByPark: ReadonlyMap<string, readonly number[]>,
): string[] {
  return rows
    .filter(
      (row) =>
        row.attractionKind == null &&
        isMazeByRule(
          resolveCuratedFacts(row),
          halloweenMonthsByPark.get(row.parkId) ?? [],
        ),
    )
    .map((row) => row.id);
}
