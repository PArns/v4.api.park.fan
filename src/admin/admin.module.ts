import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bull";
import { RedisModule } from "../common/redis/redis.module";
import { AdminController } from "./admin.controller";

@Module({
  imports: [
    RedisModule,
    BullModule.registerQueue({ name: "holidays" }),
    BullModule.registerQueue({ name: "park-metadata" }),
    BullModule.registerQueue({ name: "ml-training" }),
    BullModule.registerQueue({ name: "wait-times" }),
    BullModule.registerQueue({ name: "children-metadata" }),
  ],
  controllers: [AdminController],
})
export class AdminModule {}
