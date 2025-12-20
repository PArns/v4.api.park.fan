import { Injectable, Logger } from "@nestjs/common";
import { LiveDataResponse } from "./interfaces/data-source.interface";
import { normalizeForMatching } from "../../common/utils/slug.util";

/**
 * Conflict Resolver Service
 *
 * Aggregates complementary data from multiple sources
 * and resolves conflicts when the same entity exists in multiple sources.
 */
@Injectable()
export class ConflictResolverService {
  private readonly logger = new Logger(ConflictResolverService.name);

  /**
   * Aggregate park data from multiple sources
   *
   * Strategy: Use complementary data from each source
   * - ThemeParks.wiki: Wait times, schedules, shows, restaurants, forecasts
   * - Queue-Times: Lands (unique)
   */
  aggregateParkData(sources: Map<string, LiveDataResponse>): LiveDataResponse {
    const wiki = sources.get("themeparks-wiki");
    const qt = sources.get("queue-times");

    if (wiki && qt) {
      // Both sources available: MERGE complementary data

      // Compare wait times for validation
      this.compareWaitTimes(wiki, qt);

      // MERGE entities from both sources
      this.logger.verbose(
        `Merging entities: Wiki=${wiki.entities.length}, QT=${qt.entities.length}`,
      );
      const mergedEntities = this.mergeEntities(wiki.entities, qt.entities);
      this.logger.verbose(`Merged result: ${mergedEntities.length} total entities`);

      // Return aggregated data
      return {
        source: "multi-source",
        parkExternalId: wiki.parkExternalId,
        entities: mergedEntities, // ✅ MERGED from both sources
        lands: qt.lands, // Use Queue-Times (exclusive feature)
        fetchedAt: new Date(),
      };
    } else if (wiki) {
      // Wiki only
      return { ...wiki, lands: [] };
    } else if (qt) {
      // Queue-Times only
      return qt;
    }

    throw new Error("No data sources available");
  }

  /**
   * Merge entities from multiple sources
   *
   * Strategy:
   * 1. Create lookup map by normalized name
   * 2. For duplicates: Prefer Wiki (richer data) but mark source
   * 3. Add QT-only entities not in Wiki
   */
  private mergeEntities(wikiEntities: any[], qtEntities: any[]): any[] {
    const merged = new Map<string, any>();

    // Add all Wiki entities first (source of truth for rich data)
    for (const entity of wikiEntities) {
      const key = normalizeForMatching(entity.name);
      merged.set(key, { ...entity, source: "themeparks-wiki" });
    }

    // Add QT entities that don't exist in Wiki
    for (const qtEntity of qtEntities) {
      const key = normalizeForMatching(qtEntity.name);

      if (!merged.has(key)) {
        // QT-only entity: add it
        merged.set(key, { ...qtEntity, source: "queue-times" });
      }
      // If entity exists in Wiki: Wiki takes precedence (already added above)
      // We could merge/average wait times here, but Wiki is more reliable
    }

    return Array.from(merged.values());
  }

  /**
   * Compare wait times from different sources for validation
   */
  private compareWaitTimes(wiki: LiveDataResponse, qt: LiveDataResponse): void {
    for (const wikiEntity of wiki.entities) {
      // Find matching Qt entity
      const qtEntity = qt.entities.find(
        (e) =>
          normalizeForMatching(e.name) ===
          normalizeForMatching(wikiEntity.name),
      );

      if (qtEntity && wikiEntity.waitTime && qtEntity.waitTime) {
        // Check timestamp difference
        const wikiTime = new Date(wikiEntity.lastUpdated || wiki.fetchedAt);
        const qtTime = new Date(qtEntity.lastUpdated || qt.fetchedAt);
        const timeDiff = Math.abs(wikiTime.getTime() - qtTime.getTime());

        // Only compare if timestamps within 10 minutes
        if (timeDiff <= 10 * 60 * 1000) {
          const waitDiff = Math.abs(wikiEntity.waitTime - qtEntity.waitTime);

          if (waitDiff > 15) {
            this.logger.warn(
              `⚠️ Wait time discrepancy for "${wikiEntity.name}": ` +
              `wiki=${wikiEntity.waitTime}min, qt=${qtEntity.waitTime}min (diff: ${waitDiff}min)`,
            );
          }
        }
      }
    }
  }
}
