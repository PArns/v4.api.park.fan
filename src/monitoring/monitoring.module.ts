import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bull";
import { DataQualityMonitorService } from "./data-quality-monitor.service";
import { DataQualityProcessor } from "./data-quality.processor";

/**
 * Detectors for failures that stayed invisible for weeks: a scheduled job that
 * runs and throws, and a park losing a block of attractions from its feed.
 */
@Module({
  imports: [BullModule.registerQueue({ name: "analytics" })],
  providers: [DataQualityMonitorService, DataQualityProcessor],
  exports: [DataQualityMonitorService],
})
export class MonitoringModule {}
