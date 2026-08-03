import { ApiProperty } from "@nestjs/swagger";
import { AttractionRideProfile } from "../entities/attraction-ride-profile.entity";
import type { AttractionWithTerm } from "../services/ride-profile.service";
import {
  mergeRideStats,
  resolveRideStatsAttribution,
} from "../utils/ride-stats.util";

/**
 * Who supplied the numbers, ready to render.
 *
 * Present only when an outside source is owed a credit — a ride whose figures
 * are all hand-curated has none, and the field is null rather than a label
 * pointing nowhere. Clients render it when it is there and show nothing when
 * it is not; nobody rebuilds the rule or the URL.
 */
export class RideStatsAttributionDto {
  @ApiProperty({
    example: "Wikidata",
    description:
      "Source name to credit. Localize the sentence around it, not the name.",
  })
  label: string;

  @ApiProperty({
    example: "https://www.wikidata.org/wiki/Q319081",
    description:
      "The record the numbers are stated on. Absolute, ready to link.",
  })
  url: string;
}

/**
 * A ride's measurements as served, metric.
 *
 * Merged field by field from the hand-curated seed and the Wikidata (CC0)
 * import, curated winning. Every measurement is independently nullable — a ride
 * is listed the moment one number is known, not once all four are — so read
 * each one defensively.
 */
export class RideStatsDto {
  @ApiProperty({
    example: 80,
    nullable: true,
    description: "Top speed in km/h.",
  })
  topSpeedKmh: number | null;

  @ApiProperty({
    example: 26,
    nullable: true,
    description: "Highest point in metres.",
  })
  heightM: number | null;

  @ApiProperty({
    example: 768,
    nullable: true,
    description: "Track length in metres.",
  })
  lengthM: number | null;

  @ApiProperty({
    example: 112,
    nullable: true,
    description: "Ride duration in seconds.",
  })
  durationSeconds: number | null;

  @ApiProperty({
    example: "mixed",
    enum: ["curated", "wikidata", "mixed"],
    description:
      "Which side of the merge the surviving values came from. Provenance for " +
      "anyone who wants it — to render the credit line, use `attribution`.",
  })
  source: "curated" | "wikidata" | "mixed";

  @ApiProperty({
    example: "Q319081",
    nullable: true,
    description:
      "Wikidata entity id, set only when an imported value survived the merge. " +
      "`attribution.url` already points at it; this is the bare id.",
  })
  sourceId: string | null;

  @ApiProperty({
    type: RideStatsAttributionDto,
    nullable: true,
    description:
      "Credit line for the outside source, or null when every number is " +
      "hand-curated and no outside source is owed one. Render it when present; " +
      "do not derive it from `source`/`sourceId`.",
  })
  attribution: RideStatsAttributionDto | null;
}

/**
 * The curated "what this ride is and what it does" payload.
 *
 * Every id in here is a **glossary term id**. The frontend owns the glossary,
 * so it resolves each id to a localized name and a link; an id it does not
 * know is skipped rather than rendered raw.
 */
export class RideProfileDto {
  @ApiProperty({
    type: [String],
    example: ["lifthill", "first-drop", "vertical-loop", "zero-g-roll"],
    description:
      "Glossary term ids of the track figures, in ride order. Repeats are meaningful. Empty for rides without track figures.",
  })
  elements: string[];

  @ApiProperty({
    type: [String],
    example: ["inverted-coaster", "terrain-coaster"],
    description:
      "Glossary term ids describing the ride type (coasters / attractions categories). Unordered.",
  })
  types: string[];

  @ApiProperty({
    example: "Bolliger & Mabillard",
    nullable: true,
    description: "Manufacturer display name. Null when unknown.",
  })
  manufacturer: string | null;

  @ApiProperty({
    example: "b-and-m",
    nullable: true,
    description:
      "Glossary term id of the manufacturer. Null means render `manufacturer` as plain text with no link.",
  })
  manufacturerTermId: string | null;

  @ApiProperty({
    example: "Inverted Coaster",
    nullable: true,
    description: "The manufacturer's own model name.",
  })
  model: string | null;

  @ApiProperty({
    example: 2006,
    nullable: true,
    description: "Year the ride opened to the public.",
  })
  openedYear: number | null;

  @ApiProperty({
    example: 4,
    nullable: true,
    description:
      "Inversions as the park publishes them. May legitimately differ from the element list.",
  })
  inversions: number | null;

  @ApiProperty({
    type: RideStatsDto,
    nullable: true,
    description:
      "Measurements, metric. Null for rides we have no measurement of at all; " +
      "inside it every field is independently nullable. Credit the numbers via " +
      "`attribution` — it is null exactly when nobody outside is owed one.",
  })
  stats: RideStatsDto | null;
}

export function mapRideProfile(
  profile: AttractionRideProfile | null | undefined,
): RideProfileDto | null {
  if (!profile) return null;
  const stats = mergeRideStats(profile.curatedStats, profile.stats);
  return {
    elements: profile.elements ?? [],
    types: profile.types ?? [],
    manufacturer: profile.manufacturerName,
    manufacturerTermId: profile.manufacturerTermId,
    model: profile.model,
    openedYear: profile.openedYear,
    inversions: profile.inversions,
    stats: stats && {
      ...stats,
      attribution: resolveRideStatsAttribution(stats),
    },
  };
}

/** One ride in the glossary → rides direction. */
export class TermAttractionDto {
  @ApiProperty({ example: "Black Mamba" })
  name: string;

  @ApiProperty({ example: "black-mamba" })
  slug: string;

  @ApiProperty({ example: "Phantasialand" })
  parkName: string;

  @ApiProperty({
    example: "/v1/parks/europe/germany/bruehl/phantasialand/black-mamba",
    description:
      "API path of the ride. The frontend builds its own localized URL from the same geo segments.",
  })
  url: string;

  @ApiProperty({ example: "europe" })
  continentSlug: string;

  @ApiProperty({ example: "germany" })
  countrySlug: string;

  @ApiProperty({ example: "bruehl" })
  citySlug: string;

  @ApiProperty({ example: "phantasialand" })
  parkSlug: string;

  @ApiProperty({
    example: "element",
    enum: ["element", "type", "manufacturer"],
    description: "Where the term matched on this ride.",
  })
  kind: "element" | "type" | "manufacturer";

  @ApiProperty({ example: 2006, nullable: true })
  openedYear: number | null;

  @ApiProperty({
    example: 75,
    nullable: true,
    description:
      "Typical peak wait in minutes (P90 over 548 days). Null when the ride has no baseline yet. This is a long-run average, not a live wait time.",
  })
  typicalPeakWait: number | null;

  @ApiProperty({
    example: true,
    description: "Whether this ride is one of its park's headliners.",
  })
  isHeadliner: boolean;
}

export function mapTermAttraction(row: AttractionWithTerm): TermAttractionDto {
  const geo = `${row.continentSlug}/${row.countrySlug}/${row.citySlug}/${row.parkSlug}`;
  return {
    name: row.attractionName,
    slug: row.attractionSlug,
    parkName: row.parkName,
    // The attraction detail route is
    // `parks/:continent/:country/:city/:parkSlug/attractions/:attractionSlug`
    // — without the `attractions` segment this resolves to the park route's
    // sibling and 404s.
    url: `/v1/parks/${geo}/attractions/${row.attractionSlug}`,
    continentSlug: row.continentSlug,
    countrySlug: row.countrySlug,
    citySlug: row.citySlug,
    parkSlug: row.parkSlug,
    kind: row.kind,
    openedYear: row.openedYear,
    typicalPeakWait: row.typicalPeakWait,
    isHeadliner: row.isHeadliner,
  };
}
