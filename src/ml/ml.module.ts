import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { MLService } from "./ml.service";
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { MLModelService } from "./services/ml-model.service";
import { MLDashboardService } from "./services/ml-dashboard.service";
import { PredictionDeviationService } from "./services/prediction-deviation.service";
import { MLController } from "./controllers/ml.controller";
import { MLHealthController } from "./controllers/ml-health.controller";
import {
  WaitTimePrediction,
  MLModel,
  ParkOccupancy,
  PredictionAccuracy,
} from "./entities";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Park } from "../parks/entities/park.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";

import { WeatherModule } from "../external-apis/weather/weather.module";
import { ParksModule } from "../parks/parks.module";
import { AnalyticsModule } from "../analytics/analytics.module";
import { forwardRef } from "@nestjs/common";

@Module({
  imports: [
    WeatherModule,
    AnalyticsModule,
    forwardRef(() => ParksModule),
    TypeOrmModule.forFeature([
      WaitTimePrediction,
      MLModel,
      ParkOccupancy,
      PredictionAccuracy,
      Attraction,
      QueueData,
      Park, // For JOIN queries in getTopBottomPerformers
      ScheduleEntry, // For Phase 2 feature context
    ]),
  ],
  providers: [
    MLService,
    PredictionAccuracyService,
    MLModelService,
    MLDashboardService,
    PredictionDeviationService,
  ],
  controllers: [MLController, MLHealthController],
  exports: [
    MLService,
    PredictionAccuracyService,
    MLModelService,
    MLDashboardService,
    PredictionDeviationService,
  ],
})
export class MLModule { }
