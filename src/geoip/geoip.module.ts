import { Module, Global } from "@nestjs/common";
import { ConfigModule } from "@nestjs/config";
import { GeoipService } from "./geoip.service";

/**
 * GeoIP Module – MaxMind GeoLite2-City for IP → city coordinates.
 * Used by the nearby endpoint when no lat/lng are provided.
 */
@Global()
@Module({
  imports: [ConfigModule],
  providers: [GeoipService],
  exports: [GeoipService],
})
export class GeoipModule {}
