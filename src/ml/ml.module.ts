import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { MLService } from "./ml.service";
import { PredictionAccuracyService } from "./services/prediction-accuracy.service";
import { MLModelService } from "./services/ml-model.service";
import { MLDashboardService } from "./services/ml-dashboard.service";
import { PredictionDeviationService } from "./services/prediction-deviation.service";
import { MLDriftMonitoringService } from "./services/ml-drift-monitoring.service";
import { MLFeatureDriftService } from "./services/ml-feature-drift.service";
import { MLAlertService } from "./services/ml-alert.service";
import { MLAnomalyDetectionService } from "./services/ml-anomaly-detection.service";
import { PredictionLeadSnapshotService } from "./services/prediction-lead-snapshot.service";
import { MLController } from "./controllers/ml.controller";
import { MLHealthController } from "./controllers/ml-health.controller";
import { MLMonitoringController } from "./controllers/ml-monitoring.controller";
import {
  WaitTimePrediction,
  MLModel,
  ParkOccupancy,
  PredictionAccuracy,
  PredictionLeadSnapshot,
} from "./entities";
import { AttractionAccuracyStats } from "./entities/attraction-accuracy-stats.entity";
import { MLFeatureStats } from "./entities/ml-feature-stats.entity";
import { MLFeatureDrift } from "./entities/ml-feature-drift.entity";
import { MLAlert } from "./entities/ml-alert.entity";
import { MLPredictionAnomaly } from "./entities/ml-prediction-anomaly.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Park } from "../parks/entities/park.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";

import { WeatherModule } from "../external-apis/weather/weather.module";
import { ParksModule } from "../parks/parks.module";
import { AnalyticsModule } from "../analytics/analytics.module";
import { forwardRef } from "@nestjs/common";
import { HolidaysModule } from "../holidays/holidays.module";
import { BullModule } from "@nestjs/bull";
import { AdminAuthModule } from "../admin/auth/admin-auth.module";

@Module({
  imports: [
    BullModule.registerQueue({
      name: "ml-training",
    }),
    WeatherModule,
    AnalyticsModule,
    // The four write endpoints on MLMonitoringController carry
    // `AdminAuthGuard`, and Nest resolves a controller's guards from the
    // module that declares the controller. AdminAuthModule is @Global, so the
    // full application graph would supply it via AdminModule — but only once
    // something imports it, which is not true of a test or a script that pulls
    // MLModule on its own. Importing it here makes the module carry its own
    // dependency instead of relying on somebody else having loaded it.
    AdminAuthModule,
    forwardRef(() => ParksModule),
    TypeOrmModule.forFeature([
      WaitTimePrediction,
      MLModel,
      ParkOccupancy,
      PredictionAccuracy,
      PredictionLeadSnapshot,
      AttractionAccuracyStats,
      MLFeatureStats,
      MLFeatureDrift,
      MLAlert,
      MLPredictionAnomaly,
      Attraction,
      QueueData,
      Park, // For JOIN queries in getTopBottomPerformers
      ScheduleEntry, // For Phase 2 feature context
    ]),
    forwardRef(() => HolidaysModule),
  ],
  providers: [
    MLService,
    PredictionAccuracyService,
    MLModelService,
    MLDashboardService,
    PredictionDeviationService,
    MLDriftMonitoringService,
    MLFeatureDriftService,
    MLAlertService,
    MLAnomalyDetectionService,
    PredictionLeadSnapshotService,
  ],
  controllers: [MLController, MLHealthController, MLMonitoringController],
  exports: [
    MLService,
    PredictionAccuracyService,
    MLModelService,
    MLDashboardService,
    PredictionDeviationService,
    MLFeatureDriftService,
    MLAlertService,
    MLAnomalyDetectionService,
    PredictionLeadSnapshotService,
  ],
})
export class MLModule {}
