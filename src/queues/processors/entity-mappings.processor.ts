import { Processor, Process } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Job } from "bull";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { AttractionsService } from "../../attractions/attractions.service";
import { ShowsService } from "../../shows/shows.service";
import { RestaurantsService } from "../../restaurants/restaurants.service";
import { ParksService } from "../../parks/parks.service";
import { MultiSourceOrchestrator } from "../../external-apis/data-sources/multi-source-orchestrator.service";
import { EntityMatcherService } from "../../external-apis/data-sources/entity-matcher.service";
import { EntityMetadata } from "../../external-apis/data-sources/interfaces/data-source.interface";

/**
 * Entity Mappings Processor
 *
 * Creates ExternalEntityMapping entries for attractions, shows, and restaurants
 * by matching entities from ThemeParks.wiki and Queue-Times.com
 *
 * This processor solves the fundamental multi-source ID problem:
 * - Attractions are keyed by ThemeParks.wiki externalId in our DB
 * - Queue-Times has different IDs for the same attractions
 * - We need BOTH mappings to support land assignments and multi-source data
 *
 * Strategy:
 * 1. For each park with multi-source support
 * 2. Fetch entity metadata from both sources
 * 3. Use EntityMatcher to match entities by name + location
 * 4. Create TWO mappings for each matched entity:
 *    - themeparks-wiki ID → internal ID
 *    - queue-times ID → internal ID
 *
 * This runs AFTER children-metadata sync and BEFORE wait-times sync
 */
@Processor("entity-mappings")
export class EntityMappingsProcessor {
  private readonly logger = new Logger(EntityMappingsProcessor.name);
  private processedParksCount = 0;

  constructor(
    @InjectRepository(ExternalEntityMapping)
    private mappingRepository: Repository<ExternalEntityMapping>,
    private attractionsService: AttractionsService,
    private showsService: ShowsService,
    private restaurantsService: RestaurantsService,
    private parksService: ParksService,
    private orchestrator: MultiSourceOrchestrator,
    private entityMatcher: EntityMatcherService,
  ) {}

  @Process("sync-park-mappings")
  async handleSyncMappings(job: Job<{ parkId: string }>): Promise<void> {
    const { parkId } = job.data;

    // Validate inputs
    if (!parkId) {
      this.logger.error("Missing parkId in job data");
      return;
    }

    try {
      await this.syncParkEntityMappings(parkId);
    } catch (error) {
      this.logger.error(`Failed to sync mappings for park ${parkId}: ${error}`);
      throw error;
    }
  }

  /**
   * Sync entity mappings for a single park
   */
  private async syncParkEntityMappings(parkId: string): Promise<number> {
    // Get park mappings to find external IDs
    const parkMappings = await this.mappingRepository.find({
      where: {
        internalEntityId: parkId,
        internalEntityType: "park",
      },
    });

    const wikiMapping = parkMappings.find(
      (m) => m.externalSource === "themeparks-wiki",
    );
    const qtMapping = parkMappings.find(
      (m) => m.externalSource === "queue-times",
    );

    if (!wikiMapping || !qtMapping) {
      this.logger.debug(
        `Skipping park ${parkId}: Missing multi-source mappings`,
      );
      return 0; // Park not in both sources
    }

    const park = await this.parksService.findById(parkId);
    this.logger.verbose(`🔗 Syncing entity mappings for park ${park?.name}...`);

    // Fetch entities from both sources
    const wikiSource = this.orchestrator.getSource("themeparks-wiki");
    const qtSource = this.orchestrator.getSource("queue-times");

    if (!wikiSource || !qtSource) {
      this.logger.error(
        `Skipping park ${parkId}: One or more data sources unavailable`,
      );
      return 0;
    }

    const wikiEntities = await wikiSource.fetchParkEntities(
      wikiMapping.externalEntityId,
    );
    const qtEntities = await qtSource.fetchParkEntities(
      qtMapping.externalEntityId,
    );

    if (wikiEntities.length === 0 || qtEntities.length === 0) {
      this.logger.warn(
        `Skipping park ${parkId}: No entities found from one or both sources`,
      );
      return 0;
    }

    let mappingsCreated = 0;

    // Filter by type and match
    mappingsCreated += await this.matchAndMapEntities(
      parkId,
      wikiEntities.filter((e) => e.entityType === "ATTRACTION"),
      qtEntities.filter((e) => e.entityType === "ATTRACTION"),
      "attraction",
      "themeparks-wiki",
      "queue-times",
    );

    // Shows and Restaurants likely only on wiki, but good to check
    mappingsCreated += await this.matchAndMapEntities(
      parkId,
      wikiEntities.filter((e) => e.entityType === "SHOW"),
      qtEntities.filter((e) => e.entityType === "SHOW"),
      "show",
      "themeparks-wiki",
      "queue-times",
    );

    mappingsCreated += await this.matchAndMapEntities(
      parkId,
      wikiEntities.filter((e) => e.entityType === "RESTAURANT"),
      qtEntities.filter((e) => e.entityType === "RESTAURANT"),
      "restaurant",
      "themeparks-wiki",
      "queue-times",
    );

    this.logger.verbose(
      `✅ Created ${mappingsCreated} mappings for park ${park?.name}`,
    );

    this.processedParksCount++;
    if (this.processedParksCount % 10 === 0) {
      this.logger.log(
        `Progress: Processed ${this.processedParksCount} parks for entity mappings`,
      );
    }
    return mappingsCreated;
  }

  /**
   * Match entities from two sources and create mappings
   */
  private async matchAndMapEntities(
    parkId: string,
    source1Entities: EntityMetadata[],
    source2Entities: EntityMetadata[],
    entityType: "attraction" | "show" | "restaurant",
    source1Name: string,
    source2Name: string,
  ): Promise<number> {
    if (source1Entities.length === 0 || source2Entities.length === 0) {
      return 0;
    }

    // Use EntityMatcher to find matches
    const matchResult = this.entityMatcher.matchEntities(
      source1Entities,
      source2Entities,
    );

    let mappingsCreated = 0;

    // For each matched pair, find internal ID and create both mappings
    for (const match of matchResult.matched) {
      // Find internal entity by source1 (wiki) externalId
      // NOTE: We assume seeding created the entity with the wiki ID as its externalId
      // or we have a mapping already.
      // Actually, seeding sets Attraction.externalId = Wiki ID.
      const repository = this.getRepository(entityType);

      const entity = await repository.findOne({
        where: {
          parkId,
          externalId: match.entity1.externalId,
        },
        relations: [], // Ensure we don't fetch unnecessary relations
      });
      // Note: We need landName in the entity to check if it's missing.
      // Ensure entity returned by repository has landName.
      // TypeORM findOne returns all columns by default, so 'landName' should be present.

      if (!entity) {
        // Try finding via mapping if direct externalId match fails
        // This handles cases where seeding might have used a diff ID (unlikely)
        continue;
      }

      // Create mapping for source1 (Update confidence/cache)
      await this.createMapping(
        entity.id,
        entityType,
        source1Name,
        match.entity1.externalId,
        1.0, // Source of Truth
        "exact", // Was 'seed-source'
      );
      mappingsCreated++;

      // Create mapping for source2 (Queue-Times)
      // This is the NEW link we need!
      await this.createMapping(
        entity.id,
        entityType,
        source2Name,
        match.entity2.externalId,
        match.confidence,
        "geographic", // Was 'name+location'
      );
      mappingsCreated++;

      // ENRICHMENT: If this is an attraction and source2 (QT) has land data, update our internal entity
      if (
        entityType === "attraction" &&
        match.entity2.landName &&
        !entity.landName // Only update if missing (don't overwrite Wiki if present)
      ) {
        this.logger.debug(
          `🌱 Enriching attraction "${entity.name}" with land "${match.entity2.landName}" from ${source2Name}`,
        );
        await this.attractionsService.updateLandInfo(
          entity.id,
          match.entity2.landName,
          match.entity2.landId || null,
        );
      }
    }

    return mappingsCreated;
  }

  /**
   * Get repository for entity type
   */
  private getRepository(entityType: "attraction" | "show" | "restaurant"): any {
    switch (entityType) {
      case "attraction":
        return this.attractionsService.getRepository();
      case "show":
        return this.showsService.getRepository();
      case "restaurant":
        return this.restaurantsService.getRepository();
    }
  }

  /**
   * Create entity mapping with duplicate check
   */
  private async createMapping(
    internalEntityId: string,
    internalEntityType: "park" | "attraction" | "show" | "restaurant",
    externalSource: string,
    externalEntityId: string,
    matchConfidence: number,
    matchMethod: "exact" | "fuzzy" | "manual" | "geographic", // Fixed type
  ): Promise<void> {
    // Check if mapping exists for this external source/ID pair
    // The Unique Constraint is likely on (externalSource, externalEntityId)
    const existing = await this.mappingRepository.findOne({
      where: {
        externalSource,
        externalEntityId,
      },
    });

    if (existing) {
      // Update if internal ID differs (should not happen often) or to update details
      if (existing.internalEntityId !== internalEntityId) {
        this.logger.warn(
          `Updating mapping for ${externalSource}:${externalEntityId} from ${existing.internalEntityId} to ${internalEntityId}`,
        );
        await this.mappingRepository.update(existing.id, {
          internalEntityId,
          internalEntityType,
          matchConfidence,
          matchMethod,
        });
      }
      // else: identical mapping exists, do nothing
    } else {
      try {
        await this.mappingRepository.save({
          internalEntityId,
          internalEntityType,
          externalSource,
          externalEntityId,
          matchConfidence,
          matchMethod,
        });
      } catch (error: any) {
        // Catch race conditions
        if (error.code === "23505") {
          // Postgres duplicate key
          this.logger.warn(
            `Duplicate key ignored for ${externalSource}:${externalEntityId}`,
          );
        } else {
          throw error;
        }
      }
    }
  }
}
