import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { BullModule } from "@nestjs/bull";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { AttractionRideProfile } from "../../attractions/entities/attraction-ride-profile.entity";
import { Park } from "../../parks/entities/park.entity";
import { ParkSlugAlias } from "../../parks/entities/park-slug-alias.entity";
import { AttractionSlugAlias } from "../../attractions/entities/attraction-slug-alias.entity";
import { RedisModule } from "../../common/redis/redis.module";
import { RevalidationModule } from "../../common/revalidation/revalidation.module";
import { AdminContentController } from "./admin-content.controller";
import { AdminCurationService } from "./admin-curation.service";
import { AdminRedirectService } from "./admin-redirect.service";
import { AdminRedirectsController } from "./admin-redirects.controller";
import { AdminRideProfileService } from "./admin-ride-profile.service";
import { GlossaryTermIdsService } from "./glossary-term-ids.service";
import { ParkSeasonsModule } from "../../parks/park-seasons.module";
import { AssistantModule } from "../../assistant/assistant.module";
import { FunFactTranslationService } from "./fun-fact-translation.service";
import { FunFactTranslationProcessor } from "./fun-fact-translation.processor";
import { EftelingMaintenanceSyncProcessor } from "./efteling-maintenance-sync.processor";

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
    TypeOrmModule.forFeature([
      Park,
      Attraction,
      AttractionRideProfile,
      ParkSlugAlias,
      AttractionSlugAlias,
    ]),
    RedisModule,
    RevalidationModule,
    // The queue CuratedDataProcessor already listens on for `revalidate-parks`.
    // Its name is a leftover from the removed seed and is kept because Bull
    // keys repeatable jobs by queue name.
    BullModule.registerQueue({ name: "manual-metadata" }),
    ParkSeasonsModule,
    // Parky's key, model and daily budget, for the fun-fact translation.
    AssistantModule,
  ],
  controllers: [AdminContentController, AdminRedirectsController],
  providers: [
    AdminCurationService,
    AdminRedirectService,
    AdminRideProfileService,
    GlossaryTermIdsService,
    FunFactTranslationService,
    FunFactTranslationProcessor,
    EftelingMaintenanceSyncProcessor,
  ],
  exports: [AdminCurationService],
})
export class AdminContentModule {}
