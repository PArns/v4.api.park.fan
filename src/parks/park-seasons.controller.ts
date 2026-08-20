import {
  Controller,
  Get,
  NotFoundException,
  Param,
  Query,
} from "@nestjs/common";
import { ApiOperation, ApiResponse, ApiTags } from "@nestjs/swagger";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Park } from "./entities/park.entity";
import { ParkSeason } from "./entities/park-season.entity";
import { ParkSeasonService } from "./services/park-season.service";

/**
 * The public read side of a park's seasons.
 *
 * A separate endpoint rather than a field on the park payload, and the reason
 * is the API budget: the park response is fetched for every park page and
 * re-polled every five minutes for as long as a tab stays open, while seasons
 * change a handful of times a year. Putting them in it would pay for them on
 * every poll of every park, including the majority that have none on file.
 * Seasons are day-stable, so they belong in a request the page makes once and
 * caches for a day.
 */
@ApiTags("parks")
@Controller("parks")
export class ParkSeasonsController {
  constructor(
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    private readonly seasons: ParkSeasonService,
  ) {}

  @Get(":continent/:country/:city/:park/seasons")
  @ApiOperation({
    summary: "A park's named seasons and events",
    description:
      "Halloween, Christmas, winter closures, maintenance windows. `dates` is " +
      "null when the season runs every day between its bounds, and a list of " +
      "the days it actually runs when it does not — Walibi Holland's Fright " +
      "Nights are weekends plus three single dates, and rendering that as a " +
      "range would tell a visitor the park is haunted on a Tuesday.",
  })
  @ApiResponse({
    status: 200,
    description: "Seasons overlapping the requested window",
  })
  async forPark(
    @Param("continent") continentSlug: string,
    @Param("country") countrySlug: string,
    @Param("city") citySlug: string,
    @Param("park") slug: string,
    @Query("from") from?: string,
    @Query("to") to?: string,
  ): Promise<{
    parkId: string;
    from: string;
    to: string;
    seasons: ParkSeason[];
  }> {
    const park = await this.parks.findOne({
      where: { continentSlug, countrySlug, citySlug, slug },
      select: { id: true },
    });
    if (!park) throw new NotFoundException("Park not found");

    // Defaults to a year from today: long enough that a visitor planning next
    // Halloween in August sees it, short enough that a park with a decade of
    // history does not answer with all of it.
    const today = new Date();
    const defaultFrom = today.toISOString().slice(0, 10);
    const defaultTo = new Date(today.getTime() + 365 * 86_400_000)
      .toISOString()
      .slice(0, 10);

    const rangeFrom = isDate(from) ? from : defaultFrom;
    const rangeTo = isDate(to) ? to : defaultTo;

    return {
      parkId: park.id,
      from: rangeFrom,
      to: rangeTo,
      seasons: await this.seasons.forParkInRange(park.id, rangeFrom, rangeTo),
    };
  }
}

function isDate(value: string | undefined): value is string {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
}
