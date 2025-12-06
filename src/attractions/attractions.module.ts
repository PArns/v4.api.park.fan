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

@Module({
  imports: [
    TypeOrmModule.forFeature([Attraction]),
    ThemeParksModule,
    forwardRef(() => ParksModule),
    forwardRef(() => QueueDataModule),
    AnalyticsModule,
    MLModule,
    RedisModule,
  ],
  controllers: [AttractionsController],
  providers: [AttractionsService, AttractionIntegrationService],
  exports: [AttractionsService],
})
export class AttractionsModule {}
