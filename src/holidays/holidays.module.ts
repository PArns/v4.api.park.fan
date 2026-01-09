import { Module, forwardRef } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Holiday } from "./entities/holiday.entity";
import { HolidaysService } from "./holidays.service";
import { NagerDateModule } from "../external-apis/nager-date/nager-date.module";
import { ParksModule } from "../parks/parks.module";
import { RedisModule } from "../common/redis/redis.module";

/**
 * Holidays Module
 *
 * Manages holiday data for ML predictions.
 * Holidays significantly impact park attendance and wait times.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([Holiday]),
    NagerDateModule,
    forwardRef(() => ParksModule), // forwardRef to avoid circular dependency
    RedisModule,
  ],
  controllers: [],
  providers: [HolidaysService],
  exports: [HolidaysService],
})
export class HolidaysModule {}
