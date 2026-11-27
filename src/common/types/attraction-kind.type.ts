/**
 * What an attraction fundamentally is, decided by a person — or, where nobody
 * did, by the rule in `attractions/utils/attraction-kind-rule` (PAR-1453),
 * which writes its own column and never this verdict.
 *
 * Not the upstream's `attraction_type`. That column is free text from
 * ThemeParks.wiki/Queue-Times, and it is empty: one of ~7,400 rows carries a
 * value (measured 2026-09-22). Nothing can be derived from it, and nothing can
 * be derived from the name either — `Big Thunder Mountain Railroad`,
 * `Seven Dwarfs Mine Train` and `Ghost Train` are roller coasters and dark
 * rides that merely sound like transport. Of 151 transport-sounding
 * attractions, 17 are headliners and 3 of those are actually transport
 * systems. So this is a hand-written verdict about what a thing is FOR, which
 * is exactly the kind of judgement a name-match cannot make.
 *
 * `TRANSPORT` is the value the field exists for: a railway station, a cable
 * car, a monorail. Their wait times are a function of the timetable — a train
 * every 20 minutes builds a queue whether or not anybody wants to ride — so a
 * measured wait means something different there than on a coaster.
 *
 * `MAZE` is a walk-through horror attraction with scare actors that runs only
 * during a Halloween event (a "Halloween Horror Nights" house, a "Scarefest"
 * maze). Year-round haunted attractions stay what they were: a Haunted Mansion
 * or a Geisterbahn is a dark ride (`RIDE`), a permanent walk-through is
 * `WALKTHROUGH`. The test is the event, not the theme.
 *
 * `GAME` is something you play rather than ride or watch: a midway game, an
 * arcade, a shooting gallery. It came last (PAR-1453), so a client that knows
 * only the first five must read an unknown value as "unknown", never fail.
 *
 * The other three were here from the start deliberately. A single-valued enum
 * is a boolean wearing a costume, and the categories the curated-type
 * docstring already names as upstream's failure modes (water rides filed as
 * ATTRACTION, walkthroughs filed as RIDE) are the ones an editor needs to be
 * able to say instead.
 *
 * Null is not `RIDE`. Many attractions are unjudged, and a default of
 * "it's a ride" would be our own bookkeeping served as a statement about the
 * park — the rule in docs/rules/absent-facts.md. Read it as "nobody decided",
 * never as a category.
 */
export const ATTRACTION_KIND_VALUES = [
  "RIDE",
  "TRANSPORT",
  "SHOW",
  "WALKTHROUGH",
  "MAZE",
  "GAME",
] as const;

export type AttractionKind = (typeof ATTRACTION_KIND_VALUES)[number];

/**
 * Who decided a served kind: an editor (`attraction_kind`, final) or the rule
 * (`derived_attraction_kind`, recomputed nightly and replaced by a better
 * rule). Null when neither has a value.
 */
export type AttractionKindSource = "curated" | "derived";
