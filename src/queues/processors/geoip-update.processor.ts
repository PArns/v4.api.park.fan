import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { GeoipService } from "../../geoip/geoip.service";

/**
 * GeoIP Update Processor
 *
 * Downloads MaxMind GeoLite2-City and replaces the local MMDB.
 * Scheduled every 48 hours. Requires GEOIP_MAXMIND_ACCOUNT_ID and GEOIP_MAXMIND_LICENSE_KEY.
 */
@Processor("geoip-update")
export class GeoipUpdateProcessor {
  private readonly logger = new Logger(GeoipUpdateProcessor.name);

  constructor(private readonly geoipService: GeoipService) {}

  @Process("update-geolite2-city")
  async handleUpdate(_job: Job): Promise<void> {
    this.logger.log("Updating GeoLite2-City database...");
    try {
      await this.geoipService.downloadAndReplace();
      this.logger.log("GeoLite2-City update complete.");
    } catch (err) {
      this.logger.error(`GeoLite2-City update failed: ${err}`);
      throw err;
    }
  }
}
