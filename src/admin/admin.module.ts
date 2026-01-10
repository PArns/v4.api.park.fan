import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bull";
import { RedisModule } from "../common/redis/redis.module";
import { ParksModule } from "../parks/parks.module";
import { AdminController } from "./admin.controller";

@Module({
  imports: [
    RedisModule,
    ParksModule,
    BullModule.registerQueue({ name: "holidays" }),
    BullModule.registerQueue({ name: "park-metadata" }),
    BullModule.registerQueue({ name: "park-enrichment" }),
    BullModule.registerQueue({ name: "ml-training" }),
    BullModule.registerQueue({ name: "wait-times" }),
    BullModule.registerQueue({ name: "children-metadata" }),
  ],
  controllers: [AdminController],
})
export class AdminModule {}
