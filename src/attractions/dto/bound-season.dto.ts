import { ApiProperty } from "@nestjs/swagger";
import {
  PARK_SEASON_BINDING_KINDS,
  PARK_SEASON_STATUSES,
  type ParkSeasonKind,
  type ParkSeasonStatus,
} from "../../parks/entities/park-season.entity";
import type { SeasonBinding } from "../../common/types/season-binding.type";

/**
 * The park season a ride is bound to: the ride runs only on its days, so
 * outside them `isCurrentlyInSeason` is `false` and a client says "only during
 * <name>" rather than "closed".
 *
 * Present only where a season of a binding kind names the ride and the ride's
 * own curated season does not override it; absent for nearly every ride. Of
 * several seasons (one per year, say) it is the one whose bounds hold the
 * park's today, else the next to start, else the last to end. The fields are
 * those of `/v1/parks/…/seasons`; `name` and `dates` are stripped on the wire
 * when null (`ExcludeNullInterceptor`).
 */
export class BoundSeasonDto implements SeasonBinding {
  @ApiProperty({ description: "The season's id, as `/seasons` serves it" })
  id: string;

  @ApiProperty({
    description:
      "The season kind; only the kinds that bind a ride appear here. Expect a new one",
    enum: PARK_SEASON_BINDING_KINDS,
    example: "winter",
  })
  kind: ParkSeasonKind;

  @ApiProperty({
    description:
      "The park's own name for the season, untranslated. Absent → the label for `kind`",
    example: "Wintertraum",
    required: false,
    nullable: true,
  })
  name: string | null;

  @ApiProperty({
    description:
      "How settled the dates are; `expected` binds too, a cancelled season never does",
    enum: PARK_SEASON_STATUSES,
    example: "confirmed",
  })
  status: ParkSeasonStatus;

  @ApiProperty({
    description: "First day, park-local, inclusive",
    example: "2026-11-20",
  })
  startDate: string;

  @ApiProperty({
    description: "Last day, park-local, inclusive",
    example: "2027-01-18",
  })
  endDate: string;

  @ApiProperty({
    description:
      "The days the season runs, sorted. Absent: every day between the bounds",
    required: false,
    nullable: true,
    type: [String],
  })
  dates: string[] | null;
}

/** The Swagger block of `boundSeason` on every attraction payload. */
export const BOUND_SEASON_PROPERTY = {
  description:
    "The park season this ride runs in, when a season binds it: outside its " +
    "days `isCurrentlyInSeason` is false and `seasonMonths` null. Absent when " +
    "no season binds the ride.",
  required: false,
  type: BoundSeasonDto,
} as const;
