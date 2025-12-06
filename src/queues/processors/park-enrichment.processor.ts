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
      select: ["id", "name", "country", "countryCode", "influencingCountries"],
    });

    let updatedCountryCodes = 0;
    let updatedInfluences = 0;

    for (const park of parks) {
      let needsUpdate = false;
      const updates: Partial<Park> = {};

      // 1. Set countryCode from country name
      if (park.country && !park.countryCode) {
        const iso = getCountryISO(park.country);
        if (iso) {
          updates.countryCode = iso;
          needsUpdate = true;
          updatedCountryCodes++;
        } else {
          this.logger.warn(
            `No ISO code mapping for country: ${park.country} (${park.name})`,
          );
        }
      }

      // 2. Set influencingCountries
      const currentCountryCode = updates.countryCode || park.countryCode;
      if (
        currentCountryCode &&
        (!park.influencingCountries || park.influencingCountries.length === 0)
      ) {
        const influences = COUNTRY_INFLUENCES[currentCountryCode];
        if (influences && influences.length > 0) {
          // Take top 3 most important neighbors
          updates.influencingCountries = influences.slice(0, 3);
          needsUpdate = true;
          updatedInfluences++;
        }
      }

      // Save updates
      if (needsUpdate) {
        await this.parkRepository.update(park.id, updates);
        this.logger.debug(
          `✓ ${park.name}: ${updates.countryCode || park.countryCode} → [${updates.influencingCountries?.join(", ") || park.influencingCountries?.join(", ") || "none"}]`,
        );
      }
    }

    this.logger.log(`✅ Park enrichment complete!`);
    this.logger.log(`   Country codes: ${updatedCountryCodes} updated`);
    this.logger.log(`   Influencing countries: ${updatedInfluences} updated`);
  }
}
