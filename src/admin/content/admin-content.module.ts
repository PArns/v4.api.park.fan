import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { BullModule } from "@nestjs/bull";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { AttractionRideProfile } from "../../attractions/entities/attraction-ride-profile.entity";
import { Park } from "../../parks/entities/park.entity";
import { RedisModule } from "../../common/redis/redis.module";
import { RevalidationModule } from "../../common/revalidation/revalidation.module";
import { AdminContentController } from "./admin-content.controller";
import { AdminCurationService } from "./admin-curation.service";
import { AdminRideProfileService } from "./admin-ride-profile.service";
import { ParkSeasonsModule } from "../../parks/park-seasons.module";

/**
 * The editing half of the admin.
 *
 * Its own module rather than more providers on AdminModule: that one pulls in
 * thirteen Bull queues and the whole parks + attractions + monitoring graph to
 * trigger jobs, and none of that is needed to write a curated column. This one
 * needs four repositories, Redis, the revalidation webhook and the one queue
 * that carries the delayed cache sweep.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([Park, Attraction, AttractionRideProfile]),
    RedisModule,
    RevalidationModule,
    // The queue CuratedDataProcessor already listens on for `revalidate-parks`.
    // Its name is a leftover from the removed seed and is kept because Bull
    // keys repeatable jobs by queue name.
    BullModule.registerQueue({ name: "manual-metadata" }),
    ParkSeasonsModule,
  ],
  controllers: [AdminContentController],
  providers: [AdminCurationService, AdminRideProfileService],
  exports: [AdminCurationService],
})
export class AdminContentModule {}
