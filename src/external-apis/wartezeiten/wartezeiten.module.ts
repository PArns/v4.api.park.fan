import { Module } from "@nestjs/common";
import { WartezeitenClient } from "./wartezeiten.client";
import { WartezeitenDataSource } from "./wartezeiten-data-source";
import { RedisModule } from "../../common/redis/redis.module";

@Module({
  imports: [RedisModule],
  providers: [WartezeitenClient, WartezeitenDataSource],
  exports: [WartezeitenClient, WartezeitenDataSource],
})
export class WartezeitenModule {}
