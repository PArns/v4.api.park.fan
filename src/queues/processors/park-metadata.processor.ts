import { Processor, Process, InjectQueue } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { ParksService } from "../../parks/parks.service";
import { DestinationsService } from "../../destinations/destinations.service";
import { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import { GoogleGeocodingClient } from "../../external-apis/geocoding/google-geocoding.client";
import { MultiSourceOrchestrator } from "../../external-apis/data-sources/multi-source-orchestrator.service";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { Park } from "../../parks/entities/park.entity";
import { ParkMetadata } from "../../external-apis/data-sources/interfaces/data-source.interface";
import { generateSlug } from "../../common/utils/slug.util";

/**
 * Park Metadata Processor (Multi-Source)
 *
 * Processes jobs in the 'park-metadata' queue.
 * Fetches parks from BOTH ThemeParks.wiki AND Queue-Times.com
 * Automatically matches parks across sources
 * Enriches all parks with geocoding data
 */
@Processor("park-metadata")
export class ParkMetadataProcessor {
  private readonly logger = new Logger(ParkMetadataProcessor.name);

  constructor(
    private parksService: ParksService,
    private destinationsService: DestinationsService,
    private themeParksClient: ThemeParksClient,
    private geocodingClient: GoogleGeocodingClient,
    private orchestrator: MultiSourceOrchestrator,
    @InjectRepository(ExternalEntityMapping)
    private mappingRepository: Repository<ExternalEntityMapping>,
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectQueue("weather") private weatherQueue: Queue,
    @InjectQueue("children-metadata") private childrenQueue: Queue,
    @InjectQueue("park-enrichment") private enrichmentQueue: Queue,
  ) {}

  @Process("sync-all-parks")
  async handleFetchParks(_job: Job): Promise<void> {
    this.logger.log("🚀 Starting MULTI-SOURCE park metadata sync...");

    try {
      // Step 1: Sync destinations (Wiki only)
      const destinationCount =
        await this.destinationsService.syncDestinations();
      this.logger.log(`✅ Synced ${destinationCount} destinations`);

      // Step 2: Discover parks from ALL sources
      this.logger.log("🔍 Discovering parks from all sources...");
      const { matched, wikiOnly, qtOnly } =
        await this.orchestrator.discoverAllParks();

      this.logger.log(
        `📊 Discovery complete: ${matched.length} matched, ${wikiOnly.length} wiki-only, ${qtOnly.length} qt-only`,
      );

      // Step 3: Process matched parks (exists in BOTH sources)
      for (const match of matched) {
        await this.processMatchedPark(match.wiki, match.qt, match.confidence);
      }

      // Step 4: Process wiki-only parks
      for (const parkMeta of wikiOnly) {
        await this.processWikiOnlyPark(parkMeta);
      }

      // Step 5: Process Queue-Times-only parks
      for (const parkMeta of qtOnly) {
        await this.processQueueTimesOnlyPark(parkMeta);
      }

      // Step 6: Sync schedules for all parks
      await this.syncSchedulesForAllParks();

      // Step 7: Geocode parks that need it
      await this.geocodeParks();

      this.logger.log("🎉 Multi-source park metadata sync complete!");

      // Step 8: Enrich parks with ISO codes and influencing countries
      this.logger.log("🌍 Triggering park enrichment...");
      await this.enrichmentQueue.add("enrich-all", {}, { priority: 3 });

      // Step 9: Trigger children metadata sync
      this.logger.log("🎢 Triggering children metadata sync...");
      await this.childrenQueue.add("fetch-all-children", {}, { priority: 2 });

      // Step 10: Trigger weather sync
      await this.weatherQueue.add("fetch-weather", {}, { priority: 2 });
    } catch (error) {
      this.logger.error("❌ Park metadata sync failed", error);
      throw error;
    }
  }

  /**
   * Process a park that exists in BOTH sources
   */
  private async processMatchedPark(
    wiki: ParkMetadata,
    qt: ParkMetadata,
    confidence: number,
  ): Promise<void> {
    // Find or create destination (from wiki data)
    let destination = null;
    if (wiki.destinationId) {
      destination = await this.destinationsService.findByExternalId(
        wiki.destinationId,
      );
    }

    // Check if park already exists
    let park = await this.parkRepository.findOne({
      where: { externalId: wiki.externalId },
    });

    if (!park) {
      // Create park using Wiki data (richer)
      const bestName = wiki.name.length > qt.name.length ? wiki.name : qt.name;
      park = await this.parkRepository.save({
        externalId: wiki.externalId,
        name: bestName,
        slug: generateSlug(bestName),
        destinationId: destination?.id || undefined,
        timezone: wiki.timezone,
        latitude: wiki.latitude,
        longitude: wiki.longitude,
        primaryDataSource: "multi-source",
        dataSources: ["themeparks-wiki", "queue-times"],
        wikiEntityId: wiki.externalId,
        queueTimesEntityId: qt.externalId,
      });
      this.logger.debug(`✓ Created matched park: ${park.name} (${park.id})`);
    } else {
      // Update existing park with multi-source info
      park.dataSources = ["themeparks-wiki", "queue-times"];
      park.primaryDataSource = "multi-source";
      park.wikiEntityId = wiki.externalId;
      park.queueTimesEntityId = qt.externalId;

      // Prefer longer name if available (e.g. "Universals Epic Universe" > "Epic Universe")
      const bestName = wiki.name.length > qt.name.length ? wiki.name : qt.name;
      if (bestName.length > park.name.length) {
        this.logger.log(`Updating park name: "${park.name}" -> "${bestName}"`);
        park.name = bestName;
        park.slug = generateSlug(bestName);
      }

      await this.parkRepository.save(park);
      this.logger.debug(`✓ Updated matched park: ${park.name} (${park.id})`);
    }

    // Create external mappings for BOTH sources (with conflict handling)
    await this.createMapping(
      park.id,
      "park",
      "themeparks-wiki",
      wiki.externalId,
      1.0,
      "exact",
    );
    await this.createMapping(
      park.id,
      "park",
      "queue-times",
      qt.externalId,
      confidence,
      "fuzzy",
    );
  }

  /**
   * Process a park that exists ONLY in ThemeParks.wiki
   */
  private async processWikiOnlyPark(wiki: ParkMetadata): Promise<void> {
    // Check if park already exists (Try ID first, then fallback to Slug for self-healing)
    let park = await this.findParkWithFallback(
      wiki.externalId,
      wiki.name,
      "themeparks-wiki",
    );

    if (park) {
      if (!park.wikiEntityId) {
        park.wikiEntityId = wiki.externalId;
        park.queueTimesEntityId = null;
        await this.parkRepository.save(park);
        this.logger.debug(
          `✓ Backfilled columns for wiki-only park: ${park.name}`,
        );
      }
      this.logger.debug(`✓ Park already exists: ${park.name}`);
      return;
    }

    let destination = null;
    if (wiki.destinationId) {
      destination = await this.destinationsService.findByExternalId(
        wiki.destinationId,
      );
    }

    park = await this.parkRepository.save({
      externalId: wiki.externalId,
      name: wiki.name,
      slug: generateSlug(wiki.name),
      destinationId: destination?.id || undefined,
      timezone: wiki.timezone,
      latitude: wiki.latitude,
      longitude: wiki.longitude,
      primaryDataSource: "themeparks-wiki",
      dataSources: ["themeparks-wiki"],
      wikiEntityId: wiki.externalId,
      queueTimesEntityId: null,
    });

    await this.createMapping(
      park.id,
      "park",
      "themeparks-wiki",
      wiki.externalId,
      1.0,
      "exact",
    );

    this.logger.debug(`✓ Created wiki-only park: ${park.name}`);
  }

  /**
   * Process a park that exists ONLY in Queue-Times
   */
  private async processQueueTimesOnlyPark(qt: ParkMetadata): Promise<void> {
    const qtExternalId = `qt-${qt.externalId}`; // Prefix to avoid collision

    // Check if park already exists
    let park = await this.parkRepository.findOne({
      where: { externalId: qtExternalId },
    });

    if (park) {
      if (!park.queueTimesEntityId) {
        park.queueTimesEntityId = qt.externalId;
        park.wikiEntityId = null;
        try {
          await this.parkRepository.save(park);
        } catch (error: any) {
          // Handle race condition where another sync created the park
          if (error.message && error.message.includes("duplicate key")) {
            this.logger.warn(
              `Park ${park.name} already exists (race condition), updating instead`,
            );
            return;
          }
          throw error;
        }
        this.logger.debug(
          `✓ Backfilled columns for qt-only park: ${park.name}`,
        );
      }
      this.logger.debug(`✓ Park already exists: ${park.name}`);
      return;
    }

    park = await this.parkRepository.save({
      externalId: qtExternalId,
      name: qt.name,
      slug: generateSlug(qt.name),
      // destinationId will be null by default (nullable column)
      timezone: qt.timezone,
      latitude: qt.latitude,
      longitude: qt.longitude,
      continent: qt.continent,
      continentSlug: qt.continent ? generateSlug(qt.continent) : undefined,
      country: qt.country,
      countrySlug: qt.country ? generateSlug(qt.country) : undefined,
      primaryDataSource: "queue-times",
      dataSources: ["queue-times"],
      wikiEntityId: null,
      queueTimesEntityId: qt.externalId,
    });

    await this.createMapping(
      park.id,
      "park",
      "queue-times",
      qt.externalId,
      1.0,
      "exact",
    );

    this.logger.debug(`✓ Created qt-only park: ${park.name}`);
  }

  /**
   * Create an external entity mapping
   * Automatically resolves conflicts by updating stale mappings
   */
  private async createMapping(
    internalEntityId: string,
    internalEntityType: "park" | "attraction" | "show" | "restaurant",
    externalSource: string,
    externalEntityId: string,
    matchConfidence: number,
    matchMethod: "exact" | "fuzzy" | "manual" | "geographic",
  ): Promise<void> {
    // Check if mapping already exists for this external entity
    const existing = await this.mappingRepository.findOne({
      where: {
        externalSource,
        externalEntityId,
      },
    });

    if (existing) {
      // Check if it points to the correct internal entity
      if (existing.internalEntityId === internalEntityId) {
        this.logger.debug(
          `Mapping already exists: ${externalSource}:${externalEntityId}`,
        );
        return;
      }

      // CONFLICT: External ID is mapped to a different internal entity
      // This happens when park IDs rotate or parks merge/split
      this.logger.warn(
        `⚠️ Mapping conflict detected: ${externalSource}:${externalEntityId} ` +
          `currently maps to ${existing.internalEntityId} but should map to ${internalEntityId}. ` +
          `Updating mapping...`,
      );

      // Update the existing mapping to point to the correct entity
      existing.internalEntityId = internalEntityId;
      existing.internalEntityType = internalEntityType;
      existing.matchConfidence = matchConfidence;
      existing.matchMethod = matchMethod;
      existing.verified = matchMethod === "exact" || matchMethod === "manual";

      await this.mappingRepository.save(existing);
      this.logger.log(
        `✅ Resolved mapping conflict: ${externalSource}:${externalEntityId} now correctly maps to ${internalEntityId}`,
      );
      return;
    }

    // No conflict, create new mapping
    await this.mappingRepository.save({
      internalEntityId,
      internalEntityType,
      externalSource,
      externalEntityId,
      matchConfidence,
      matchMethod,
      verified: matchMethod === "exact" || matchMethod === "manual",
    });
  }

  /**
   * Sync schedules for all parks (Wiki only)
   */
  private async syncSchedulesForAllParks(): Promise<void> {
    this.logger.log("📅 Syncing schedules...");
    const parks = await this.parkRepository.find();
    let totalScheduleEntries = 0;

    for (const park of parks) {
      // Only sync schedules for parks with Wiki data
      if (!park.dataSources || !park.dataSources.includes("themeparks-wiki")) {
        continue;
      }

      try {
        // Get Wiki external ID from mapping
        const mapping = await this.mappingRepository.findOne({
          where: {
            internalEntityId: park.id,
            internalEntityType: "park",
            externalSource: "themeparks-wiki",
          },
        });

        if (!mapping) continue;

        const scheduleResponse = await this.themeParksClient.getSchedule(
          mapping.externalEntityId,
        );
        const savedEntries = await this.parksService.saveScheduleData(
          park.id,
          scheduleResponse.schedule,
        );
        totalScheduleEntries += savedEntries;
      } catch (error) {
        this.logger.error(`Failed to sync schedule for ${park.name}: ${error}`);
      }
    }

    this.logger.log(`✅ Synced ${totalScheduleEntries} schedule entries`);
  }

  /**
   * Geocode parks that need geographic data
   */
  private async geocodeParks(): Promise<void> {
    const parksWithoutGeodata =
      await this.parksService.findParksWithoutGeodata();

    if (parksWithoutGeodata.length === 0) {
      this.logger.log("✅ All parks already have geocoding data");
      return;
    }

    this.logger.log(`🌍 Geocoding ${parksWithoutGeodata.length} parks...`);
    let geocodedCount = 0;

    for (const park of parksWithoutGeodata) {
      // Initialize an object to hold updates for the park
      const updates: Partial<Park> = {};
      let needsUpdate = false;

      // 3. Reverse Geocoding (if coordinates exist)
      if (park.latitude && park.longitude) {
        // Smart Check: If we have coordinates but no region data, we should try to geocode again
        // regardless of geocodingAttemptedAt, because we might have cached data without regions.
        // The GoogleGeocodingClient handles the smart caching (fetch only if missing in cache).
        const missingRegionData = !park.region || !park.regionCode;

        const shouldGeocode =
          !park.city ||
          !park.country ||
          !park.geocodingAttemptedAt ||
          missingRegionData; // Retry if we are missing regional data

        if (shouldGeocode) {
          try {
            const geoData = await this.geocodingClient.reverseGeocode(
              Number(park.latitude),
              Number(park.longitude),
            );

            if (geoData) {
              updates.city = geoData.city;
              updates.country = geoData.country;
              updates.countryCode = geoData.countryCode; // Now available directly from geocoding
              updates.continent = geoData.continent;

              // Save region data
              if (geoData.region) updates.region = geoData.region;
              if (geoData.regionCode) updates.regionCode = geoData.regionCode;

              updates.geocodingAttemptedAt = new Date();
              needsUpdate = true;
              this.logger.debug(
                `Geocoded ${park.name}: ${geoData.city}, ${geoData.regionCode || geoData.region || ""}, ${geoData.country} (${geoData.continent})`,
              );
            } else {
              // Mark as attempted even if failed to avoid constant retries (unless smart retry logic overrides)
              if (!missingRegionData) {
                updates.geocodingAttemptedAt = new Date();
                needsUpdate = true;
              }
            }
          } catch (error: any) {
            this.logger.warn(
              `Geocoding failed for ${park.name}: ${error.message}`,
            );
            // Mark as attempted even if failed to avoid constant retries (unless smart retry logic overrides)
            if (!missingRegionData) {
              updates.geocodingAttemptedAt = new Date();
              needsUpdate = true;
            }
          }
        }
      }

      if (needsUpdate) {
        await this.parksService.updateGeodata(park.id, updates);
        geocodedCount++;
      } else if (!park.geocodingAttemptedAt) {
        // If no geocoding was performed and it was never attempted, mark it as attempted
        await this.parksService.markGeocodingAttempted(park.id);
      }

      // Rate limiting (100ms delay)
      await this.sleep(100);
    }

    this.logger.log(
      `✅ Geocoded ${geocodedCount}/${parksWithoutGeodata.length} parks`,
    );
  }

  /**
   * Find a park by External ID, with fallback to Slug for self-healing
   * If found by slug but ID differs, it updates the ID (Self-Healing)
   */
  private async findParkWithFallback(
    externalId: string,
    name: string,
    source: string,
  ): Promise<Park | null> {
    // 1. Try finding by External ID (Fastest, Standard)
    let park = await this.parkRepository.findOne({
      where: { externalId },
    });

    if (park) {
      return park;
    }

    // 2. Fallback: Try finding by Slug (Self-Healing for ID rotation)
    // Only do this if the name is reasonably long to avoid false positives on overly generic names
    if (name.length < 5) return null;

    const slug = generateSlug(name);
    park = await this.parkRepository.findOne({
      where: { slug },
    });

    if (park) {
      // 3. We found the park, but the ID has changed!
      this.logger.warn(
        `⚠️ Park ID rotation detected for "${park.name}" (${source}). Updating: ${park.externalId} -> ${externalId}`,
      );

      // Verify this isn't a collision with another valid park (sanity check)
      // (The unique constraint on externalId would catch this on save, but good to check)

      park.externalId = externalId;
      if (source === "themeparks-wiki") {
        park.wikiEntityId = externalId;
      } else if (source === "queue-times") {
        park.queueTimesEntityId = externalId;
      }

      await this.parkRepository.save(park);

      // We must also update the mapping immediately to prevent unique constraint errors later
      await this.updateMapping(park.id, source, externalId);
    }

    return park;
  }

  /**
   * Update existing mapping for ID rotation
   */
  private async updateMapping(
    internalEntityId: string,
    externalSource: string,
    newExternalId: string,
  ): Promise<void> {
    // Find any existing mapping for this internal entity + source
    const existing = await this.mappingRepository.findOne({
      where: {
        internalEntityId,
        externalSource,
        internalEntityType: "park",
      },
    });

    if (existing) {
      this.logger.log(
        `🔄 Updating mapping for ${internalEntityId} [${externalSource}]: ${existing.externalEntityId} -> ${newExternalId}`,
      );
      // Update the external ID
      await this.mappingRepository.update(existing.id, {
        externalEntityId: newExternalId,
      });
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
