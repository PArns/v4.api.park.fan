import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { getCountryISOCode } from "../../common/constants/country-codes.constant";
import { COUNTRY_INFLUENCES } from "../../common/country-influences";
import { getInfluencingRegions } from "../../common/region-influences";

/**
 * ParkEnrichmentProcessor
 *
 * Enriches park data after sync:
 * - Converts country names to ISO codes
 * - Populates influencingRegions for cross-border tourism
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
        "influencingRegions",
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
        const iso = getCountryISOCode(park.country);
        if (iso) {
          updates.countryCode = iso;
          updates.metadataRetryCount = 0; // Reset on success
          needsUpdate = true;
          updatedCountryCodes++;
        }
      }

      // 3. Set influencingRegions
      const currentCountryCode = updates.countryCode || park.countryCode;
      const currentRegionCode = updates.regionCode || park.regionCode;

      if (
        currentCountryCode &&
        (!park.influencingRegions || park.influencingRegions.length === 0)
      ) {
        let newInfluences: {
          countryCode: string;
          regionCode: string | null;
        }[] = [];

        // A. Try Regional Specific Configuration (First Priority)
        // e.g. DE-BW -> [DE-RP, DE-BY, CH, FR...]
        if (currentRegionCode) {
          const regionInfluences = getInfluencingRegions(
            currentCountryCode,
            currentRegionCode,
          );
          if (regionInfluences.length > 0) {
            newInfluences = regionInfluences;
          }
        }

        // B. Fallback to Country Neighbors (Second Priority)
        // If no specific regional config exists, use the country's neighbors
        if (newInfluences.length === 0) {
          const influencingCountryCodes =
            COUNTRY_INFLUENCES[currentCountryCode];
          if (influencingCountryCodes && influencingCountryCodes.length > 0) {
            newInfluences = influencingCountryCodes.slice(0, 3).map((code) => ({
              countryCode: code,
              regionCode: null,
            }));
          }
        }

        if (newInfluences.length > 0) {
          updates.influencingRegions = newInfluences;
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
