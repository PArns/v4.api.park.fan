import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bull";
import { bullQueue } from "../queues/queue-registrations";
import { DataQualityMonitorService } from "./data-quality-monitor.service";
import { DataQualityProcessor } from "./data-quality.processor";
import { DisplayConsistencyService } from "./display-consistency/display-consistency.service";
import { DisplayConsistencyProcessor } from "./display-consistency/display-consistency.processor";

/**
 * Detectors for failures that stayed invisible for weeks: a scheduled job that
 * runs and throws, a park losing a block of attractions from its feed, and a
 * park scheduled open that no source has reported on in a month; and the check
 * that every surface showing a park-day shows the same value where it must.
 */
@Module({
  imports: [BullModule.registerQueue(bullQueue("analytics"))],
  providers: [
    DataQualityMonitorService,
    DataQualityProcessor,
    DisplayConsistencyService,
    DisplayConsistencyProcessor,
  ],
  exports: [DataQualityMonitorService, DisplayConsistencyService],
})
export class MonitoringModule {}
