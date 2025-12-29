import { Module, forwardRef } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Attraction } from "./entities/attraction.entity";
import { AttractionsService } from "./attractions.service";
import { AttractionsController } from "./attractions.controller";
import { AttractionIntegrationService } from "./services/attraction-integration.service";
import { ThemeParksModule } from "../external-apis/themeparks/themeparks.module";
import { ParksModule } from "../parks/parks.module";
import { QueueDataModule } from "../queue-data/queue-data.module";
import { AnalyticsModule } from "../analytics/analytics.module";
import { MLModule } from "../ml/ml.module";
import { RedisModule } from "../common/redis/redis.module";
import { QueueTimesModule } from "../external-apis/queue-times/queue-times.module";
import { WartezeitenModule } from "../external-apis/wartezeiten/wartezeiten.module";

@Module({
  imports: [
    TypeOrmModule.forFeature([Attraction]),
    ThemeParksModule,
    forwardRef(() => ParksModule),
    forwardRef(() => QueueDataModule),
    AnalyticsModule,
    MLModule,
    RedisModule,
    QueueTimesModule,
    WartezeitenModule,
  ],
  controllers: [AttractionsController],
  providers: [AttractionsService, AttractionIntegrationService],
  exports: [AttractionsService, AttractionIntegrationService],
})
export class AttractionsModule { }
