import { Module, forwardRef } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Show } from "./entities/show.entity";
import { ShowLiveData } from "./entities/show-live-data.entity";
import { ShowsService } from "./shows.service";
import { ThemeParksModule } from "../external-apis/themeparks/themeparks.module";
import { ParksModule } from "../parks/parks.module";

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
    TypeOrmModule.forFeature([Show, ShowLiveData]),
    ThemeParksModule,
    forwardRef(() => ParksModule),
  ],
  controllers: [],
  providers: [ShowsService],
  exports: [ShowsService],
})
export class ShowsModule {}
