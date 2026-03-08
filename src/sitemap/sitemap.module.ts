import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { Attraction } from "../attractions/entities/attraction.entity";
import { RedisModule } from "../common/redis/redis.module";
import { SitemapController } from "./sitemap.controller";
import { SitemapService } from "./sitemap.service";

@Module({
  imports: [TypeOrmModule.forFeature([Attraction]), RedisModule],
  controllers: [SitemapController],
  providers: [SitemapService],
})
export class SitemapModule {}
