import type {
  AttractionKind,
  AttractionKindSource,
} from "../../common/types/attraction-kind.type";
import { isRideOfType } from "./ride-facts.util";

/**
 * The rule that gives an attraction nobody judged a kind of its own (PAR-1453).
 *
 * `attraction_kind` is the editor's verdict and stays that; this rule writes
 * `derived_attraction_kind` beside it, and the API serves the editor's value
 * first. A derived value is recomputed on every run, so a better rule replaces
 * every derived value and never a curated one.
 *
 * The evidence, strongest first, and the first one that speaks decides:
 *
 * 1. **The ride profile's glossary type ids.** A person chose them, so they
 *    are the best evidence short of the kind itself. Transport before ride (a
 *    park railway is also `family`), ride before walkthrough and show (Mystery
 *    Castle is a `drop-tower` with a `pre-show`). Tags that say nothing about
 *    the kind (`animatronics`, `pre-show`, `headliner`, `scare-maze` …) are
 *    skipped, so the Enchanted Tiki Room (`animatronics`, `live-show`) is a
 *    show.
 * 2. **A published minimum height.** A height limit is a ride-safety fact the
 *    park prints; shows and games do not carry one.
 * 3. **The name, and only for a row that is not a headliner.** The trap the
 *    kind's type describes — `Big Thunder Mountain Railroad` sounds like
 *    transport — sits in the headliners: 17 of the 151 transport-sounding
 *    names were headliners and 14 of those were coasters or dark rides
 *    (2026-09-22). A headliner without a tag or a height stays unknown.
 *
 * **Queues are not evidence.** "It posts a wait, so it is a ride" is wrong at
 * Magic Kingdom, where Mickey's PhilharMagic, The Hall of Presidents, the
 * Laugh Floor and Country Bear post 10–15 minute waits and Swiss Family
 * Treehouse posts 5 (park payload, 2026-11-27). Nothing else separates them,
 * so a row with only a queue stays unknown rather than becoming a ride.
 *
 * Never `MAZE`: that verdict needs a season and stays with the maze rule.
 * Null means the rule could not tell, and the data-quality report lists it.
 */

/** Ids of the glossary's `attractions` category that make a thing transport. */
const TRANSPORT_TYPE_IDS: ReadonlySet<string> = new Set([
  "park-railway",
  "monorail",
  "sky-ride",
]);

/**
 * Ride systems of the `attractions` category; every coaster term is one too
 * (`isRideOfType(…, "coaster")`). `people-mover` is a loop you ride, not a way
 * to get somewhere, so it is a ride.
 */
const RIDE_TYPE_IDS: ReadonlySet<string> = new Set([
  "dark-ride",
  "suspended-dark-ride",
  "interactive-dark-ride",
  "trackless-ride",
  "omnimover",
  "motion-simulator",
  "flying-theater",
  "madhouse",
  "flat-ride",
  "water-ride",
  "drop-tower",
  "log-flume",
  "river-rapids",
  "shoot-the-chute",
  "boat-ride",
  "swing-ride",
  "pendulum-ride",
  "top-spin",
  "break-dance",
  "enterprise",
  "ranger",
  "condor",
  "troika",
  "carousel",
  "teacups",
  "ferris-wheel",
  "bumper-cars",
  "observation-tower",
  "people-mover",
]);

const WALKTHROUGH_TYPE_IDS: ReadonlySet<string> = new Set([
  "walkthrough",
  "funhouse",
]);

/** A 4D cinema is a film you sit in, so a show unless a ride tag says more. */
const SHOW_TYPE_IDS: ReadonlySet<string> = new Set([
  "live-show",
  "parade",
  "nighttime-spectacular",
  "4d-cinema",
]);

/**
 * Name words, tried in this order, at word boundaries, case-insensitive.
 * Games first, because a shooting gallery is a game whatever else its name
 * says ("Frontierland Shootin' Arcade").
 */
const NAME_RULES: ReadonlyArray<readonly [AttractionKind, RegExp]> = [
  [
    "GAME",
    /\b(arcade|midway|games?|spielhalle|schie(ß|ss)(bude|stand)|shooting gallery|shootin)\b/i,
  ],
  [
    "SHOW",
    /\b(show|theat(er|re)|kino|cinema|musical|jamboree|revue|spectacular|parade|fireworks|feuerwerk|4-?d)\b/i,
  ],
  [
    "WALKTHROUGH",
    /\b(walk-?through|trail|treehouse|labyrinth|irrgarten|mirror maze|spiegelkabinett)\b/i,
  ],
  [
    "TRANSPORT",
    /\b(railroad|railway|monorail|cable ?car|gondola|seilbahn|skyway|sky ?ride|parkbahn|eisenbahn)\b/i,
  ],
];

export interface AttractionKindEvidence {
  /** The resolved name (`resolveAttractionName`). */
  name: string;
  /** The ride profile's glossary type ids; empty without a profile. */
  types: readonly string[];
  /** The resolved minimum height (`resolveHeight`); null when none is known. */
  minimumHeight: number | null;
  /** Whether the row is a tier-1 or tier-2 headliner of its park. */
  isHeadliner: boolean;
}

/** The kind the evidence supports, or null when it does not settle one. */
export function deriveAttractionKind(
  evidence: AttractionKindEvidence,
): AttractionKind | null {
  const { types } = evidence;
  if (types.some((id) => TRANSPORT_TYPE_IDS.has(id))) return "TRANSPORT";
  if (
    isRideOfType(types, "coaster") ||
    types.some((id) => RIDE_TYPE_IDS.has(id))
  )
    return "RIDE";
  if (types.some((id) => WALKTHROUGH_TYPE_IDS.has(id))) return "WALKTHROUGH";
  if (types.some((id) => SHOW_TYPE_IDS.has(id))) return "SHOW";
  if (evidence.minimumHeight != null && evidence.minimumHeight > 0)
    return "RIDE";
  if (evidence.isHeadliner) return null;
  for (const [kind, pattern] of NAME_RULES) {
    if (pattern.test(evidence.name)) return kind;
  }
  return null;
}

/**
 * The kind a reader is served: the editor's, else the rule's, with which of
 * the two it is. Both DTO mappers read it, so the two payloads cannot differ.
 */
export function resolveAttractionKind(attraction: {
  attractionKind?: AttractionKind | null;
  derivedAttractionKind?: AttractionKind | null;
}): {
  attractionKind: AttractionKind | null;
  attractionKindSource: AttractionKindSource | null;
} {
  if (attraction.attractionKind) {
    return {
      attractionKind: attraction.attractionKind,
      attractionKindSource: "curated",
    };
  }
  if (attraction.derivedAttractionKind) {
    return {
      attractionKind: attraction.derivedAttractionKind,
      attractionKindSource: "derived",
    };
  }
  return { attractionKind: null, attractionKindSource: null };
}
