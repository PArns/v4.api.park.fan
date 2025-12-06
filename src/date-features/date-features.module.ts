import { Module } from "@nestjs/common";
import { DateFeaturesService } from "./date-features.service";
import { HolidaysModule } from "../holidays/holidays.module";

/**
 * Date Features Module
 *
 * Provides region-specific date features for ML predictions:
 * - Weekend detection (varies by country)
 * - Holiday detection (via HolidaysService)
 * - Peak day identification (weekends + holidays)
 *
 * Used by analytics and ML services to correlate attendance patterns
 * with calendar features (weekends, holidays, special events).
 */
@Module({
  imports: [HolidaysModule],
  providers: [DateFeaturesService],
  exports: [DateFeaturesService],
})
export class DateFeaturesModule {}
