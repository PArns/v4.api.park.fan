import { Controller, Get, NotFoundException, Param } from "@nestjs/common";
import { ApiOperation, ApiResponse, ApiTags } from "@nestjs/swagger";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { OpenRoute } from "../api-keys/api-permission.decorator";
import { Park } from "../parks/entities/park.entity";
import { FacilitiesService, type PublicFacilities } from "./facilities.service";

/**
 * A park's toilets, drinking water and sights from OpenStreetMap.
 *
 * Its own endpoint, like the seasons: the points change a few times a year
 * and the park payload is polled every five minutes. And an open route on
 * purpose: this answer is OpenStreetMap data under the ODbL, which allows no
 * further terms on it (4.7, 4.8), so it sits behind no data key and none of
 * park.fan's data terms. The global five-minute cache policy applies.
 */
@ApiTags("parks")
@Controller("parks")
export class FacilitiesController {
  constructor(
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    private readonly facilities: FacilitiesService,
  ) {}

  @Get(":continent/:country/:city/:park/facilities")
  @OpenRoute("OpenStreetMap data under the ODbL, which allows no added terms")
  @ApiOperation({
    summary: "A park's facilities from OpenStreetMap",
    description:
      "Toilets, drinking water and sights (named figure bins and artworks) inside the " +
      "park's outline, © OpenStreetMap contributors, ODbL 1.0. A client ignores a `type` " +
      "it does not know. `available: false` with an empty list means nobody has " +
      "reviewed this park yet, not that it has no toilets: show no layer. " +
      "Whoever shows the points credits OpenStreetMap as `attribution` says. " +
      "404 while the feature is switched off (`FACILITIES_ENABLED`).",
  })
  @ApiResponse({ status: 200, description: "The reviewed facilities" })
  @ApiResponse({ status: 404, description: "Unknown park, or feature off" })
  async forPark(
    @Param("continent") continentSlug: string,
    @Param("country") countrySlug: string,
    @Param("city") citySlug: string,
    @Param("park") slug: string,
  ): Promise<PublicFacilities> {
    const park = await this.parks.findOne({
      where: { continentSlug, countrySlug, citySlug, slug },
      select: { id: true },
    });
    const answer = park && (await this.facilities.publicFor(park.id));
    if (!answer) throw new NotFoundException("Not found");
    return answer;
  }
}
