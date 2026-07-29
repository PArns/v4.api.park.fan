import { ApiProperty } from "@nestjs/swagger";
import {
  AttractionRideProfile,
  type RideStats,
} from "../entities/attraction-ride-profile.entity";
import type { AttractionWithTerm } from "../services/ride-profile.service";

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
    nullable: true,
    description:
      "Measured facts imported from RCDB (metric): lengthM, heightM, dropM, elevationM, " +
      "topSpeedKmh, durationSeconds, gForce, verticalAngleDeg, inversions, capacityPerHour, " +
      "ridersPerTrain, designer, builder, trainManufacturer, restraints — plus source and " +
      "sourceId. Null for rides we hold no RCDB id for. Individual fields are null when RCDB " +
      "omits them, so read every one defensively.",
    example: {
      lengthM: 768,
      heightM: 26.2,
      dropM: 27,
      topSpeedKmh: 80,
      durationSeconds: 140,
      gForce: 4,
      inversions: 4,
      source: "rcdb",
      sourceId: 3117,
    },
  })
  stats: RideStats | null;
}

export function mapRideProfile(
  profile: AttractionRideProfile | null | undefined,
): RideProfileDto | null {
  if (!profile) return null;
  return {
    elements: profile.elements ?? [],
    types: profile.types ?? [],
    manufacturer: profile.manufacturerName,
    manufacturerTermId: profile.manufacturerTermId,
    model: profile.model,
    openedYear: profile.openedYear,
    inversions: profile.inversions,
    stats: profile.stats ?? null,
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
