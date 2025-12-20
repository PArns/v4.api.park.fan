import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { getCountryISO } from "../../common/country-mapping";
import { COUNTRY_INFLUENCES } from "../../common/country-influences";

/**
 * ParkEnrichmentProcessor
 *
 * Enriches park data after sync:
 * - Converts country names to ISO codes
 * - Populates influencingCountries for cross-border tourism
 *
 * Runs after park-metadata sync to ensure data consistency
 */
@Processor("park-enrichment")
export class ParkEnrichmentProcessor {
  private readonly logger = new Logger(ParkEnrichmentProcessor.name);

  constructor(
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
  ) {}

  @Process("enrich-all")
  async handleEnrichAll(_job: Job): Promise<void> {
    return this.enrichAllParks();
  }

  /**
   * Enrich all parks with ISO codes and influencing countries
   */
  private async enrichAllParks(): Promise<void> {
    this.logger.log(
      "🌍 Enriching parks with ISO codes and influencing countries...",
    );

    const parks = await this.parkRepository.find({
      select: [
        "id",
        "name",
        "country",
        "countryCode",
        "region",
        "regionCode",
        "latitude",
        "longitude",
        "influencingCountries",
        "metadataRetryCount",
      ],
    });

    let updatedCountryCodes = 0;
    let updatedInfluences = 0;
    let retryAttempts = 0;

    for (const park of parks) {
      let needsUpdate = false;
      const updates: Partial<Park> = {};

      // 1. Check for missing critical metadata (Self-Healing)
      // If countryCode is missing but coordinates exist, we should try to fix it
      const isMissingCriticalData =
        !park.countryCode || !park.regionCode || !park.city;

      if (
        isMissingCriticalData &&
        park.latitude &&
        park.longitude &&
        park.metadataRetryCount < 3
      ) {
        this.logger.log(
          `Attempting self-healing enrichment for ${park.name} (Attempt ${park.metadataRetryCount + 1}/3)...`,
        );
        retryAttempts++;

        try {
          // Increment retry count to eventually stop if Google API keeps failing to return the country
          updates.metadataRetryCount = (park.metadataRetryCount || 0) + 1;
          needsUpdate = true;

          this.logger.debug(
            `Marked ${park.name} for geocoding retry (attempt ${updates.metadataRetryCount})`,
          );
        } catch (error) {
          this.logger.error(`Self-healing failed for ${park.name}: ${error}`);
        }
      }

      // 2. Set countryCode from country name (if already available but code missing)
      if (park.country && !park.countryCode && !updates.countryCode) {
        const iso = getCountryISO(park.country);
        if (iso) {
          updates.countryCode = iso;
          updates.metadataRetryCount = 0; // Reset on success
          needsUpdate = true;
          updatedCountryCodes++;
        }
      }

      // 3. Set influencingCountries
      const currentCountryCode = updates.countryCode || park.countryCode;
      if (
        currentCountryCode &&
        (!park.influencingCountries || park.influencingCountries.length === 0)
      ) {
        const influences = COUNTRY_INFLUENCES[currentCountryCode];
        if (influences && influences.length > 0) {
          updates.influencingCountries = influences.slice(0, 3);
          needsUpdate = true;
          updatedInfluences++;
        }
      }

      // Save updates
      if (needsUpdate) {
        await this.parkRepository.update(park.id, updates);
        this.logger.debug(
          `✓ ${park.name} updated: countryCode=${updates.countryCode || park.countryCode}, retryCount=${updates.metadataRetryCount ?? park.metadataRetryCount}`,
        );
      }
    }

    this.logger.log(`✅ Park enrichment complete!`);
    this.logger.log(`   Country codes: ${updatedCountryCodes} updated`);
    this.logger.log(`   Influencing countries: ${updatedInfluences} updated`);
    this.logger.log(`   Self-healing retries: ${retryAttempts} triggered`);
  }
}
