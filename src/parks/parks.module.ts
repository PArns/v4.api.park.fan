import { Module, forwardRef } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { ParksController } from "./parks.controller";
import { ParksService } from "./parks.service";
import { WeatherService } from "./weather.service";
import { ParkIntegrationService } from "./services/park-integration.service";
import { ParkEnrichmentService } from "./services/park-enrichment.service";
import { CalendarService } from "./services/calendar.service";
import { Park } from "./entities/park.entity";
import { WeatherData } from "./entities/weather-data.entity";
import { ScheduleEntry } from "./entities/schedule-entry.entity";
import { ThemeParksModule } from "../external-apis/themeparks/themeparks.module";
import { DestinationsModule } from "../destinations/destinations.module";
import { AttractionsModule } from "../attractions/attractions.module";
import { ShowsModule } from "../shows/shows.module";
import { RestaurantsModule } from "../restaurants/restaurants.module";
import { QueueDataModule } from "../queue-data/queue-data.module";
import { AnalyticsModule } from "../analytics/analytics.module";
import { MLModule } from "../ml/ml.module";
import { GeocodingModule } from "../external-apis/geocoding/geocoding.module";
import { RedisModule } from "../common/redis/redis.module";
import { WeatherModule } from "../external-apis/weather/weather.module";
import { HolidaysModule } from "../holidays/holidays.module";
import { QueueTimesModule } from "../external-apis/queue-times/queue-times.module";
import { WartezeitenModule } from "../external-apis/wartezeiten/wartezeiten.module";
import { AttractionAccuracyStats } from "../ml/entities/attraction-accuracy-stats.entity";

@Module({
  imports: [
    TypeOrmModule.forFeature([
      Park,
      WeatherData,
      ScheduleEntry,
      AttractionAccuracyStats,
    ]),
    ThemeParksModule,
    DestinationsModule,
    forwardRef(() => AttractionsModule),
    forwardRef(() => ShowsModule),
    forwardRef(() => RestaurantsModule),
    forwardRef(() => QueueDataModule),
    forwardRef(() => AnalyticsModule),
    forwardRef(() => MLModule),
    GeocodingModule,
    RedisModule,
    WeatherModule,
    forwardRef(() => HolidaysModule),
    QueueTimesModule,
    WartezeitenModule,
  ],
  controllers: [ParksController],
  providers: [
    ParksService,
    WeatherService,
    ParkIntegrationService,
    ParkEnrichmentService,
    CalendarService,
  ],
  exports: [
    ParksService,
    WeatherService,
    ParkIntegrationService,
    TypeOrmModule, // CRITICAL: Export TypeOrmModule so other modules can inject ParkRepository
    ParkEnrichmentService,
    CalendarService,
  ],
})
export class ParksModule {}
