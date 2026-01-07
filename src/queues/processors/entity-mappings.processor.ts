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
  private skippedParksCount = 0;
  private mappedCountBySource: Record<string, number> = {};

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
    const wzMapping = parkMappings.find(
      (m) => m.externalSource === "wartezeiten-app",
    );

    // Track total mappings
    let totalMappings = 0;
    const parkStats: Record<string, number> = {};

    // Need at least one source to do matching
    // Previously required Wiki, but now support Wartezeiten-only parks
    if (!wikiMapping && !qtMapping && !wzMapping) {
      this.skippedParksCount++;
      return 0;
    }

    // Special case: Wartezeiten-only parks (no Wiki, no QT)
    // These parks have entities created with Wartezeiten UUIDs, so we can't match them
    // They're already correctly set up during initial sync
    if (!wikiMapping && !qtMapping && wzMapping) {
      // Wartezeiten-only park - entities already have correct externalIds
      // No matching needed, but we could create mappings for consistency
      this.skippedParksCount++;
      return 0;
    }

    const wikiSource = this.orchestrator.getSource("themeparks-wiki");
    const qtSource = this.orchestrator.getSource("queue-times");
    const wzSource = this.orchestrator.getSource("wartezeiten-app");

    // Fetch Wiki entities (Primary Source)
    let wikiEntities: EntityMetadata[] = [];
    if (wikiSource && wikiMapping) {
      wikiEntities = await wikiSource.fetchParkEntities(
        wikiMapping.externalEntityId,
      );
    }

    // Process Queue-Times (if available)
    if (qtSource && qtMapping) {
      const qtEntities = await qtSource.fetchParkEntities(
        qtMapping.externalEntityId,
      );

      if (wikiEntities.length > 0 && qtEntities.length > 0) {
        await this.processSourcePair(
          parkId,
          wikiEntities,
          qtEntities,
          "themeparks-wiki",
          "queue-times",
          parkStats,
        );
      }
    }

    // Process Wartezeiten.app (if available)
    // NOTE: Wartezeiten only has attractions
    if (wzSource && wzMapping) {
      const wzEntities = await wzSource.fetchParkEntities(
        wzMapping.externalEntityId,
      );

      if (wikiEntities.length > 0 && wzEntities.length > 0) {
        // Only process attractions for Wartezeiten
        await this.matchAndMapEntities(
          parkId,
          wikiEntities.filter((e) => e.entityType === "ATTRACTION"),
          wzEntities.filter((e) => e.entityType === "ATTRACTION"),
          "attraction",
          "themeparks-wiki",
          "wartezeiten-app",
          parkStats,
        );
      }
    }

    // Calculate totals from stats
    totalMappings = Object.values(parkStats).reduce(
      (sum, current) => sum + current,
      0,
    );

    // Log progress periodically
    this.processedParksCount++;
    // Aggregate stats only when reporting to avoid locking
    Object.entries(parkStats).forEach(([source, count]) => {
      this.mappedCountBySource[source] =
        (this.mappedCountBySource[source] || 0) + count;
    });

    if (this.processedParksCount % 10 === 0) {
      const globalStatsStr = Object.entries(this.mappedCountBySource)
        .map(([k, v]) => `${k}: ${v}`)
        .join(", ");
      this.logger.log(
        `Progress: Processed ${this.processedParksCount} parks (Skipped: ${this.skippedParksCount}) - Mapped: ${globalStatsStr}`,
      );
    }
    return totalMappings;
  }

  /**
   * Helper to process Wiki <-> Other Source matching for all types
   */
  private async processSourcePair(
    parkId: string,
    source1Entities: EntityMetadata[],
    source2Entities: EntityMetadata[],
    source1Name: string,
    source2Name: string,
    stats: Record<string, number>,
  ) {
    const processType = async (type: "attraction" | "show" | "restaurant") => {
      await this.matchAndMapEntities(
        parkId,
        source1Entities.filter((e) => e.entityType === type.toUpperCase()),
        source2Entities.filter((e) => e.entityType === type.toUpperCase()),
        type,
        source1Name,
        source2Name,
        stats,
      );
    };

    await processType("attraction");
    await processType("show");
    await processType("restaurant");
  }

  /**
   * Match entities from two sources and create mappings
   */
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
    stats: Record<string, number>,
  ): Promise<void> {
    if (source1Entities.length === 0 || source2Entities.length === 0) {
      return;
    }

    // Use EntityMatcher to find matches
    const matchResult = this.entityMatcher.matchEntities(
      source1Entities,
      source2Entities,
    );

    // For each matched pair, find internal ID and create both mappings
    for (const match of matchResult.matched) {
      const repository = this.getRepository(entityType);

      // Find internal entity by source1 externalId (usually Wiki, but could be Wartezeiten for WZ-only parks)
      // NOTE: For Wiki-based parks, seeding sets Attraction.externalId = Wiki ID
      // For Wartezeiten-only parks, externalId = Wartezeiten UUID
      let entity = await repository.findOne({
        where: {
          parkId,
          externalId: match.entity1.externalId,
        },
        relations: [], // Ensure we don't fetch unnecessary relations
      });

      // Fallback: If not found by source1 externalId, try finding via existing mapping
      // This handles cases where:
      // 1. Entity was created with a different source's ID
      // 2. Entity already has a mapping from a previous sync
      if (!entity) {
        const existingMapping = await this.mappingRepository.findOne({
          where: {
            externalSource: source1Name,
            externalEntityId: match.entity1.externalId,
            internalEntityType: entityType,
          },
        });

        if (existingMapping) {
          entity = await repository.findOne({
            where: { id: existingMapping.internalEntityId },
            relations: [],
          });
        }

        // If still not found, skip this match
        if (!entity) {
          this.logger.debug(
            `Entity not found for ${source1Name}:${match.entity1.externalId} in park ${parkId}`,
          );
          continue;
        }
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
      stats[source1Name] = (stats[source1Name] || 0) + 1;

      // Create mapping for source2 (Queue-Times OR Wartezeiten)
      // This is the NEW link we need!
      await this.createMapping(
        entity.id,
        entityType,
        source2Name,
        match.entity2.externalId,
        match.confidence,
        "geographic", // Was 'name+location'
      );
      stats[source2Name] = (stats[source2Name] || 0) + 1;

      // ENRICHMENT: Update additional fields based on entity type
      if (entityType === "attraction") {
        const updates: Partial<any> = {};

        // Update queueTimesEntityId if source2 is Queue-Times
        // Extract numeric ID from "qt-ride-8" -> "8"
        if (source2Name === "queue-times") {
          const qtNumericId = this.extractQueueTimesNumericId(
            match.entity2.externalId,
          );
          if (qtNumericId && !entity.queueTimesEntityId) {
            updates.queueTimesEntityId = qtNumericId;
          }
        }

        // Update land information if available and missing
        if (match.entity2.landName && !entity.landName) {
          // Only update if missing (don't overwrite Wiki if present)
          await this.attractionsService.updateLandInfo(
            entity.id,
            match.entity2.landName,
            match.entity2.landId || null,
          );
        }

        // Apply queueTimesEntityId update if needed
        if (Object.keys(updates).length > 0) {
          await this.attractionsService
            .getRepository()
            .update(entity.id, updates);
        }
      } else if (entityType === "show" || entityType === "restaurant") {
        // Update land information for Shows and Restaurants if available and missing
        // Note: Queue-Times doesn't typically have Shows/Restaurants, but Wartezeiten might
        if (match.entity2.landName && !entity.landName) {
          const repository = this.getRepository(entityType);
          await repository.update(entity.id, {
            landName: match.entity2.landName,
            landExternalId: match.entity2.landId || null,
          });
        }
      }
    }
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
   * Extract numeric Queue-Times ID from external ID
   * Examples:
   * - "qt-ride-8" -> "8"
   * - "qt-park-56" -> "56"
   * - "8" -> "8" (already numeric)
   */
  private extractQueueTimesNumericId(externalId: string): string | null {
    if (!externalId) return null;

    // Handle prefixed IDs like "qt-ride-8" or "qt-park-56"
    if (externalId.startsWith("qt-ride-")) {
      return externalId.replace("qt-ride-", "");
    }
    if (externalId.startsWith("qt-park-")) {
      return externalId.replace("qt-park-", "");
    }

    // If already numeric, return as-is
    if (/^\d+$/.test(externalId)) {
      return externalId;
    }

    return null;
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
