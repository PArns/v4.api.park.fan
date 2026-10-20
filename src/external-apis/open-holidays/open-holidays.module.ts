import { Module } from "@nestjs/common";
import { OpenHolidaysClient } from "./open-holidays.client";

@Module({
  providers: [OpenHolidaysClient],
  exports: [OpenHolidaysClient],
})
export class OpenHolidaysModule {}
