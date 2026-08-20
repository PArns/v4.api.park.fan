import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bull";
import { RedisModule } from "../common/redis/redis.module";
import { ParksModule } from "../parks/parks.module";
import { AttractionsModule } from "../attractions/attractions.module";
import { AdminController } from "./admin.controller";
import { AdminAuthModule } from "./auth/admin-auth.module";
import { SystemHealthService } from "./system-health.service";
import { MonitoringModule } from "../monitoring/monitoring.module";

@Module({
  imports: [
    AdminAuthModule,
    MonitoringModule,
    RedisModule,
    ParksModule,
    AttractionsModule,
    BullModule.registerQueue({ name: "holidays" }),
    BullModule.registerQueue({ name: "park-metadata" }),
    BullModule.registerQueue({ name: "park-enrichment" }),
    BullModule.registerQueue({ name: "ml-training" }),
    BullModule.registerQueue({ name: "wait-times" }),
    BullModule.registerQueue({ name: "children-metadata" }),
    BullModule.registerQueue({ name: "six-flags-heights" }),
    BullModule.registerQueue({ name: "ride-stats" }),
    BullModule.registerQueue({ name: "manual-metadata" }),
    BullModule.registerQueue({ name: "prediction-accuracy" }),
    BullModule.registerQueue({ name: "analytics" }),
    BullModule.registerQueue({ name: "pcn-shadow" }),
    BullModule.registerQueue({ name: "shape-shadow" }),
  ],
  controllers: [AdminController],
  providers: [SystemHealthService],
})
export class AdminModule {}
