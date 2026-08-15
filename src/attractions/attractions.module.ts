import { Module, forwardRef } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Attraction } from "./entities/attraction.entity";
import { AttractionsService } from "./attractions.service";
import { AttractionIntegrationService } from "./services/attraction-integration.service";
import { ThemeParksModule } from "../external-apis/themeparks/themeparks.module";
import { ParksModule } from "../parks/parks.module";
import { QueueDataModule } from "../queue-data/queue-data.module";
import { AnalyticsModule } from "../analytics/analytics.module";
import { MLModule } from "../ml/ml.module";
import { RedisModule } from "../common/redis/redis.module";
import { QueueTimesModule } from "../external-apis/queue-times/queue-times.module";
import { WartezeitenModule } from "../external-apis/wartezeiten/wartezeiten.module";
import { HolidaysModule } from "../holidays/holidays.module";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { PopularityModule } from "../popularity/popularity.module";
import { RevalidationModule } from "../common/revalidation/revalidation.module";
import { AttractionMergeService } from "./services/attraction-merge.service";
import { ManualMetadataService } from "./services/manual-metadata.service";
import { RideProfileService } from "./services/ride-profile.service";
import { RideProfileAuditService } from "./services/ride-profile-audit.service";
import { RideStatsService } from "./services/ride-stats.service";
import { WikidataClient } from "../external-apis/wikidata/wikidata.client";
import { GlossaryRidesController } from "./glossary-rides.controller";
import { AttractionRideProfile } from "./entities/attraction-ride-profile.entity";

@Module({
  imports: [
    TypeOrmModule.forFeature([
      Attraction,
      AttractionRideProfile,
      QueueData,
      ScheduleEntry,
    ]),
    ThemeParksModule,
    forwardRef(() => ParksModule),
    forwardRef(() => QueueDataModule),
    AnalyticsModule,
    MLModule,
    RedisModule,
    PopularityModule,
    QueueTimesModule,
    WartezeitenModule,
    HolidaysModule,
    RevalidationModule,
  ],
  controllers: [GlossaryRidesController],
  providers: [
    AttractionsService,
    AttractionIntegrationService,
    AttractionMergeService,
    ManualMetadataService,
    RideProfileService,
    RideProfileAuditService,
    RideStatsService,
    WikidataClient,
  ],
  exports: [
    AttractionsService,
    AttractionIntegrationService,
    AttractionMergeService,
    ManualMetadataService,
    RideProfileService,
    RideProfileAuditService,
    RideStatsService,
  ],
})
export class AttractionsModule {}
