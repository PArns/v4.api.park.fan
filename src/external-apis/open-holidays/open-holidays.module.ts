import { Module } from "@nestjs/common";
import { HttpModule } from "@nestjs/axios";
import { OpenHolidaysClient } from "./open-holidays.client";

@Module({
  imports: [HttpModule],
  providers: [OpenHolidaysClient],
  exports: [OpenHolidaysClient],
})
export class OpenHolidaysModule {}
