import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { MLService } from "./ml.service";
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { MLModelService } from "./services/ml-model.service";
import { MLDashboardService } from "./services/ml-dashboard.service";
import { PredictionAccuracyController } from "./controllers/prediction-accuracy.controller";
import { MLDashboardController } from "./controllers/ml-dashboard.controller";
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

import { WeatherModule } from "../external-apis/weather/weather.module";

@Module({
  imports: [
    WeatherModule,
    TypeOrmModule.forFeature([
      WaitTimePrediction,
      MLModel,
      ParkOccupancy,
      PredictionAccuracy,
      Attraction,
      QueueData,
      Park, // For JOIN queries in getTopBottomPerformers
    ]),
  ],
  providers: [
    MLService,
    PredictionAccuracyService,
    MLModelService,
    MLDashboardService,
  ],
  controllers: [
    PredictionAccuracyController,
    MLDashboardController,
    MLHealthController,
  ],
  exports: [
    MLService,
    PredictionAccuracyService,
    MLModelService,
    MLDashboardService,
  ],
})
export class MLModule { }
