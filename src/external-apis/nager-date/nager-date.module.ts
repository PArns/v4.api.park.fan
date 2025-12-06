import { Module } from "@nestjs/common";
import { NagerDateClient } from "./nager-date.client";

/**
 * Nager.Date API Module
 *
 * Provides holiday data from the free Nager.Date API.
 * Used for ML predictions (holidays correlate with park attendance).
 */
@Module({
  providers: [NagerDateClient],
  exports: [NagerDateClient],
})
export class NagerDateModule {}
