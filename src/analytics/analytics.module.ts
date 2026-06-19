import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { AnalyticsService } from "./analytics.service";
import { ParkHistoricalStatsService } from "./park-historical-stats.service";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import { Attraction } from "../attractions/entities/attraction.entity";

import { AnalyticsController } from "./analytics.controller";

import { Park } from "../parks/entities/park.entity";
import { Show } from "../shows/entities/show.entity";
import { Restaurant } from "../restaurants/entities/restaurant.entity";
import { WeatherData } from "../parks/entities/weather-data.entity";
import { ScheduleEntry } from "../parks/entities/schedule-entry.entity";
import { RestaurantLiveData } from "../restaurants/entities/restaurant-live-data.entity";
import { ShowLiveData } from "../shows/entities/show-live-data.entity";
import { PredictionAccuracy } from "../ml/entities/prediction-accuracy.entity";
import { WaitTimePrediction } from "../ml/entities/wait-time-prediction.entity";
import { QueueDataAggregate } from "./entities/queue-data-aggregate.entity";
import { ParkDailyStats } from "../stats/entities/park-daily-stats.entity";
import { HeadlinerAttraction } from "./entities/headliner-attraction.entity";
import { ParkP50Baseline } from "./entities/park-p50-baseline.entity";
import { AttractionP50Baseline } from "./entities/attraction-p50-baseline.entity";
import { ParkP90Baseline } from "./entities/park-p90-baseline.entity";
import { AttractionP90Baseline } from "./entities/attraction-p90-baseline.entity";
import { AttractionHourlyHistory } from "./entities/attraction-hourly-history.entity";
import { AttractionRopeDrop } from "./entities/attraction-rope-drop.entity";
import { AttractionTypicalWaits } from "./entities/attraction-typical-waits.entity";

@Module({
  imports: [
    TypeOrmModule.forFeature([
      QueueData,
      Attraction,
      Park,
      Show,
      Restaurant,
      WeatherData,
      ScheduleEntry,
      RestaurantLiveData,
      ShowLiveData,
      PredictionAccuracy,
      WaitTimePrediction,
      QueueDataAggregate,
      ParkDailyStats,
      // P50/P90 Baseline System
      HeadlinerAttraction,
      ParkP50Baseline,
      AttractionP50Baseline,
      ParkP90Baseline,
      AttractionP90Baseline,
      AttractionHourlyHistory,
      AttractionRopeDrop,
      AttractionTypicalWaits,
    ]),
  ],
  providers: [AnalyticsService, ParkHistoricalStatsService],
  controllers: [AnalyticsController],
  exports: [AnalyticsService, ParkHistoricalStatsService],
})
export class AnalyticsModule {}
