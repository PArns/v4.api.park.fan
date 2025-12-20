import { Module } from "@nestjs/common";
import { ThemeParksClient } from "./themeparks.client";
import { ThemeParksMapper } from "./themeparks.mapper";
import { RedisModule } from "../../common/redis/redis.module";

@Module({
  imports: [RedisModule],
  providers: [ThemeParksClient, ThemeParksMapper],
  exports: [ThemeParksClient, ThemeParksMapper],
})
export class ThemeParksModule {}
