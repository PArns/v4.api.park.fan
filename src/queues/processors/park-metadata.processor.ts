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
    // Check if park already exists
    let park = await this.parkRepository.findOne({
      where: { externalId: wiki.externalId },
    });

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
        await this.parkRepository.save(park);
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
   */
  private async createMapping(
    internalEntityId: string,
    internalEntityType: "park" | "attraction" | "show" | "restaurant",
    externalSource: string,
    externalEntityId: string,
    matchConfidence: number,
    matchMethod: "exact" | "fuzzy" | "manual" | "geographic",
  ): Promise<void> {
    // Check if mapping already exists
    const existing = await this.mappingRepository.findOne({
      where: {
        externalSource,
        externalEntityId,
      },
    });

    if (existing) {
      this.logger.debug(
        `Mapping already exists: ${externalSource}:${externalEntityId}`,
      );
      return;
    }

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
      try {
        const geodata = await this.geocodingClient.reverseGeocode(
          Number(park.latitude),
          Number(park.longitude),
        );

        if (geodata) {
          await this.parksService.updateGeodata(park.id, geodata);
          geocodedCount++;
        } else {
          await this.parksService.markGeocodingAttempted(park.id);
        }
      } catch (error) {
        await this.parksService.markGeocodingAttempted(park.id);
        this.logger.error(`Failed to geocode ${park.name}: ${error}`);
      }

      // Rate limiting (100ms delay)
      await this.sleep(100);
    }

    this.logger.log(
      `✅ Geocoded ${geocodedCount}/${parksWithoutGeodata.length} parks`,
    );
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
