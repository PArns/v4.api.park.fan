import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Show } from "./entities/show.entity";
import { ShowLiveData } from "./entities/show-live-data.entity";
import { ShowSchedulePattern } from "./entities/show-schedule-pattern.entity";
import { ShowsService } from "./shows.service";

/**
 * Shows Module
 *
 * Handles show entities and showtimes.
 * Example: "Festival of the Lion King", "Fantasmic!"
 *
 * Phase 6: Shows & Restaurants
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([Show, ShowLiveData, ShowSchedulePattern]),
  ],
  controllers: [],
  providers: [ShowsService],
  exports: [ShowsService],
})
export class ShowsModule {}
