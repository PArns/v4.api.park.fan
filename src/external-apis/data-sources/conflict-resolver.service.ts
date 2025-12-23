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
   * - ThemeParks.wiki: Wait times, schedules, shows, restaurants, forecasts (completeness: 10)
   * - Wartezeiten: Crowd level (unique!), wait times validation (completeness: 6)
   * - Queue-Times: Lands (unique!), wait times validation (completeness: 5)
   *
   * Wait time merging: Median of all sources + round to 5-minute intervals
   */
  aggregateParkData(sources: Map<string, LiveDataResponse>): LiveDataResponse {
    const wiki = sources.get("themeparks-wiki");
    const qt = sources.get("queue-times");
    const wz = sources.get("wartezeiten-app");

    // If no data from any source, throw error
    if (!wiki && !qt && !wz) {
      throw new Error("No data sources available");
    }

    // Determine return values
    const parkExternalId =
      wiki?.parkExternalId || qt?.parkExternalId || wz!.parkExternalId;
    const lands = qt?.lands || [];
    const crowdLevel = wz?.crowdLevel; // Unique to Wartezeiten!

    // Operating Hours Strategy:
    // 1. If both sources have data: Compare and log discrepancies
    // 2. Prefer Wiki (source of truth for schedules)
    // 3. Fallback to Wartezeiten (Enrichment)
    let operatingHours = undefined;

    const wikiHours =
      wiki?.operatingHours && wiki.operatingHours.length > 0
        ? wiki.operatingHours
        : undefined;
    const wzHours =
      wz?.operatingHours && wz.operatingHours.length > 0
        ? wz.operatingHours
        : undefined;

    if (wikiHours && wzHours) {
      // SCENARIO: Both sources have data -> Compare
      this.compareOperatingHours(
        parkExternalId || "unknown",
        wikiHours,
        wzHours,
      );
      operatingHours = wikiHours; // Prefer Wiki
    } else if (wikiHours) {
      // SCENARIO: Only Wiki has data -> Use Wiki
      operatingHours = wikiHours;
    } else if (wzHours) {
      // SCENARIO: Only Wartezeiten has data -> Enrich
      operatingHours = wzHours;
    }
    // SCENARIO: No data -> undefined

    // Merge entities from all sources
    const mergedEntities = this.mergeEntities(
      wiki?.entities || [],
      qt?.entities || [],
      wz?.entities || [],
    );

    // Cross-validate wait times if multiple sources available
    if (wiki && (qt || wz)) {
      this.compareWaitTimes(wiki, qt, wz);
    }

    return {
      source: "multi-source",
      parkExternalId,
      entities: mergedEntities,
      lands,
      crowdLevel,
      operatingHours, // Enriched data
      fetchedAt: new Date(),
    };
  }

  /**
   * Merge entities from multiple sources (2 or 3 sources)
   *
   * Strategy:
   * 1. Create lookup map by normalized name
   * 2. For entities in multiple sources: Calculate median wait time and round to 5 minutes
   * 3. Prefer Wiki for metadata (richer data)
   * 4. Add source-only entities
   */
  private mergeEntities(
    wikiEntities: any[],
    qtEntities: any[],
    wzEntities: any[] = [],
  ): any[] {
    const merged = new Map<string, any>();

    // Add all Wiki entities first (source of truth for rich data)
    for (const entity of wikiEntities) {
      const key = normalizeForMatching(entity.name);
      merged.set(key, {
        ...entity,
        source: "themeparks-wiki",
        sources: ["themeparks-wiki"],
      });
    }

    // Add/merge QT entities
    for (const qtEntity of qtEntities) {
      const key = normalizeForMatching(qtEntity.name);

      if (merged.has(key)) {
        // Entity exists in Wiki: merge wait times
        const existing = merged.get(key);
        existing.sources.push("queue-times");

        // If both have wait times, we'll merge them later
        if (qtEntity.waitTime) {
          existing.qtWaitTime = qtEntity.waitTime;
        }
      } else {
        // QT-only entity: add it
        merged.set(key, {
          ...qtEntity,
          source: "queue-times",
          sources: ["queue-times"],
        });
      }
    }

    // Add/merge Wartezeiten entities
    for (const wzEntity of wzEntities) {
      const key = normalizeForMatching(wzEntity.name);

      if (merged.has(key)) {
        // Entity exists: merge wait times
        const existing = merged.get(key);
        existing.sources.push("wartezeiten-app");

        if (wzEntity.waitTime) {
          existing.wzWaitTime = wzEntity.waitTime;
        }
      } else {
        // Wartezeiten-only entity: add it
        merged.set(key, {
          ...wzEntity,
          source: "wartezeiten-app",
          sources: ["wartezeiten-app"],
        });
      }
    }

    for (const [_key, entity] of merged.entries()) {
      const waitTimes: number[] = [];

      if (entity.waitTime) waitTimes.push(entity.waitTime);
      if (entity.qtWaitTime) waitTimes.push(entity.qtWaitTime);
      if (entity.wzWaitTime) waitTimes.push(entity.wzWaitTime);

      if (waitTimes.length > 1) {
        let finalWaitTime: number;

        if (waitTimes.length === 3) {
          // Sort for easier comparison
          waitTimes.sort((a, b) => a - b);

          // Check if 2 of 3 sources agree (consensus)
          if (waitTimes[0] === waitTimes[1]) {
            // First two agree: use consensus value
            finalWaitTime = waitTimes[0];
          } else if (waitTimes[1] === waitTimes[2]) {
            // Last two agree: use consensus value
            finalWaitTime = waitTimes[1];
          } else {
            // All 3 different: use median (robust against outliers)
            finalWaitTime = waitTimes[1]; // Middle value after sorting
          }
        } else if (waitTimes.length === 2) {
          // Use average (= median for 2 values)
          finalWaitTime = (waitTimes[0] + waitTimes[1]) / 2;
        } else {
          // Single source (shouldn't happen in this branch, but for safety)
          finalWaitTime = waitTimes[0];
        }

        // Round to nearest 5 minutes
        entity.waitTime = Math.round(finalWaitTime / 5) * 5;
        // Log removed to reduce noise
        // this.logger.verbose(
        //   `Multi-source wait time for "${entity.name}": [${waitTimes.join(", ")}] → final: ${finalWaitTime.toFixed(1)} → rounded: ${entity.waitTime} min`,
        // );
      }

      // Cleanup temporary fields
      delete entity.qtWaitTime;
      delete entity.wzWaitTime;
    }

    return Array.from(merged.values());
  }

  /**
   * Compare wait times from different sources for validation (2 or 3 sources)
   */
  private compareWaitTimes(
    wiki: LiveDataResponse,
    qt?: LiveDataResponse,
    wz?: LiveDataResponse,
  ): void {
    for (const wikiEntity of wiki.entities) {
      const matches: { source: string; waitTime: number; lastUpdated: Date }[] =
        [];

      // Find matching entities in other sources
      const qtEntity = qt?.entities.find(
        (e) =>
          normalizeForMatching(e.name) ===
          normalizeForMatching(wikiEntity.name),
      );
      const wzEntity = wz?.entities.find(
        (e) =>
          normalizeForMatching(e.name) ===
          normalizeForMatching(wikiEntity.name),
      );

      if (wikiEntity.waitTime) {
        matches.push({
          source: "wiki",
          waitTime: wikiEntity.waitTime,
          lastUpdated: new Date(wikiEntity.lastUpdated || wiki.fetchedAt),
        });
      }

      if (qtEntity?.waitTime) {
        matches.push({
          source: "qt",
          waitTime: qtEntity.waitTime,
          lastUpdated: new Date(qtEntity.lastUpdated || qt!.fetchedAt),
        });
      }

      if (wzEntity?.waitTime) {
        matches.push({
          source: "wz",
          waitTime: wzEntity.waitTime,
          lastUpdated: new Date(wzEntity.lastUpdated || wz!.fetchedAt),
        });
      }

      // Only compare if we have 2+ sources and timestamps are within 10 minutes
      if (matches.length >= 2) {
        const timestamps = matches.map((m) => m.lastUpdated.getTime());
        const maxTimeDiff = Math.max(...timestamps) - Math.min(...timestamps);

        if (maxTimeDiff <= 10 * 60 * 1000) {
          const waitTimes = matches.map((m) => m.waitTime);
          const min = Math.min(...waitTimes);
          const max = Math.max(...waitTimes);
          const diff = max - min;

          if (diff > 15) {
            const summary = matches
              .map((m) => `${m.source}=${m.waitTime}min`)
              .join(", ");
            this.logger.warn(
              `⚠️ Wait time discrepancy for "${wikiEntity.name}": ${summary} (diff: ${diff}min)`,
            );
          }
        }
      }
    }
  }

  /**
   * Compare operating hours from different sources
   */
  private compareOperatingHours(
    parkId: string,
    wiki: import("./interfaces/data-source.interface").OperatingWindow[],
    wz: import("./interfaces/data-source.interface").OperatingWindow[],
  ): void {
    const wOne = wiki[0];
    const zOne = wz[0];

    if (wOne && zOne) {
      // Compare open/close times (simple string comparison for ISO timestamps)
      const openDiff = wOne.open !== zOne.open;
      const closeDiff = wOne.close !== zOne.close;

      if (openDiff || closeDiff) {
        this.logger.warn(
          `⚠️ Operating Hours Discrepancy for ${parkId}: Wiki(${wOne.open}-${wOne.close}) vs Wartezeiten(${zOne.open}-${zOne.close})`,
        );
      }
    }
  }
}
