import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Park } from "./entities/park.entity";
import { ParkSeason } from "./entities/park-season.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { ParkSeasonService } from "./services/park-season.service";
import { ParkSeasonsController } from "./park-seasons.controller";

/**
 * Park seasons, kept out of ParksModule on purpose.
 *
 * ParksModule is the centre of a large dependency graph — attractions, queues,
 * analytics, ML, weather — and seasons need three repositories. Putting them in
 * their own module means the admin's editing surface can depend on seasons
 * without pulling that graph in behind it, and means this feature can be read
 * end to end in two files.
 */
@Module({
  imports: [TypeOrmModule.forFeature([Park, ParkSeason, Attraction])],
  controllers: [ParkSeasonsController],
  providers: [ParkSeasonService],
  exports: [ParkSeasonService],
})
export class ParkSeasonsModule {}
