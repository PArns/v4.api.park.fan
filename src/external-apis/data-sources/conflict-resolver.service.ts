import { Injectable, Logger } from "@nestjs/common";
import {
  LiveDataResponse,
  OperatingWindow,
} from "./interfaces/data-source.interface";
import { normalizeForMatching } from "../../common/utils/slug.util";

/**
 * Merged Entity Structure
 *
 * Represents an entity (attraction, show, restaurant) that has been
 * aggregated from multiple data sources with conflict resolution applied.
 */
interface MergedEntity {
  name: string;
  source: string; // Primary source
  sources: string[]; // All sources that contributed
  waitTime?: number; // Final calculated wait time
  qtWaitTime?: number; // Temporary: Queue-Times wait time
  wzWaitTime?: number; // Temporary: Wartezeiten wait time
  [key: string]: any; // Additional properties from sources
}

/**
 * Conflict Resolver Service
 *
 * Aggregates and reconciles data from multiple theme park data sources:
 * - **ThemeParks.wiki** (primary source, completeness: 10/10)
 * - **Queue-Times** (land data, completeness: 5/10)
 * - **Wartezeiten.app** (crowd levels, completeness: 6/10)
 *
 * ## Conflict Resolution Strategies
 *
 * ### Wait Times
 * Uses consensus-based approach with outlier protection:
 * - **3 sources, 2 agree**: Use consensus value (majority vote)
 * - **3 sources, all different**: Use median (robust against outliers)
 * - **2 sources**: Use average
 * - **Always**: Round to nearest 5 minutes
 *
 * ### Operating Hours
 * Priority-based fallback:
 * 1. Wiki (preferred, most reliable)
 * 2. Wartezeiten (enrichment when Wiki unavailable)
 * 3. undefined (no data)
 *
 * ### Entities
 * Name-based matching with multi-source merging:
 * - Wiki provides rich metadata (base entity)
 * - Queue-Times adds land assignments
 * - Wartezeiten validates wait times
 *
 * ### Unique Data
 * - **Crowd Level**: Exclusive to Wartezeiten
 * - **Lands**: Exclusive to Queue-Times
 *
 * @see MultiSourceOrchestrator - Coordinates data fetching from sources
 * @see WaitTimesProcessor - Consumes aggregated data
 */
@Injectable()
export class ConflictResolverService {
  private readonly logger = new Logger(ConflictResolverService.name);

  // Constants for wait time processing
  private readonly WAIT_TIME_ROUNDING_INTERVAL = 5; // minutes
  private readonly WAIT_TIME_DISCREPANCY_THRESHOLD = 15; // minutes
  private readonly TIMESTAMP_COMPARISON_WINDOW = 10 * 60 * 1000; // 10 minutes in ms

  /**
   * Aggregate park data from multiple sources
   *
   * Combines data from up to 3 sources (Wiki, Queue-Times, Wartezeiten)
   * and resolves conflicts using predefined strategies.
   *
   * @param sources - Map of source name to LiveDataResponse
   * @returns Aggregated LiveDataResponse with resolved conflicts
   * @throws Error if no data sources are available
   *
   * @example
   * ```ts
   * const sources = new Map([
   *   ['themeparks-wiki', wikiResponse],
   *   ['queue-times', qtResponse],
   * ]);
   * const aggregated = service.aggregateParkData(sources);
   * console.log(aggregated.entities.length); // Merged entities
   * ```
   */
  aggregateParkData(sources: Map<string, LiveDataResponse>): LiveDataResponse {
    const wiki = sources.get("themeparks-wiki");
    const qt = sources.get("queue-times");
    const wz = sources.get("wartezeiten-app");

    // Validate at least one source is available
    if (!wiki && !qt && !wz) {
      throw new Error("No data sources available");
    }

    // Extract unique data from each source
    const parkExternalId =
      wiki?.parkExternalId || qt?.parkExternalId || wz!.parkExternalId;
    const lands = qt?.lands || []; // Exclusive to Queue-Times
    const crowdLevel = wz?.crowdLevel; // Exclusive to Wartezeiten

    // Resolve operating hours with fallback strategy
    const operatingHours = this.resolveOperatingHours(
      parkExternalId || "unknown",
      wiki?.operatingHours,
      wz?.operatingHours,
    );

    // Merge entities from all sources with conflict resolution
    const mergedEntities = this.mergeEntities(
      wiki?.entities || [],
      qt?.entities || [],
      wz?.entities || [],
    );

    // Cross-validate wait times for quality assurance
    if (wiki && (qt || wz)) {
      this.compareWaitTimes(wiki, qt, wz);
    }

    return {
      source: "multi-source",
      parkExternalId,
      entities: mergedEntities,
      lands,
      crowdLevel,
      operatingHours,
      fetchedAt: new Date(),
    };
  }

  /**
   * Resolve operating hours from multiple sources
   *
   * Uses a priority-based fallback strategy:
   * 1. Prefer Wiki (most reliable)
   * 2. Fallback to Wartezeiten (enrichment)
   * 3. Return undefined if no data
   *
   * Logs discrepancies when both sources have data.
   *
   * @param parkId - Park identifier for logging
   * @param wikiHours - Operating hours from Wiki
   * @param wzHours - Operating hours from Wartezeiten
   * @returns Resolved operating hours or undefined
   */
  private resolveOperatingHours(
    parkId: string,
    wikiHours?: OperatingWindow[],
    wzHours?: OperatingWindow[],
  ): OperatingWindow[] | undefined {
    const hasWiki = wikiHours && wikiHours.length > 0;
    const hasWz = wzHours && wzHours.length > 0;

    if (hasWiki && hasWz) {
      // Both sources available: compare and prefer Wiki
      this.compareOperatingHours(parkId, wikiHours, wzHours);
      return wikiHours;
    }

    // Fallback: use whichever is available
    return hasWiki ? wikiHours : hasWz ? wzHours : undefined;
  }

  /**
   * Merge entities from multiple sources (2 or 3 sources)
   *
   * Strategy:
   * 1. Use Wiki entities as base (richest metadata)
   * 2. Merge Queue-Times and Wartezeiten data by name matching
   * 3. Apply consensus-based wait time resolution
   * 4. Clean up temporary merge fields
   *
   * @param wikiEntities - Entities from ThemeParks.wiki
   * @param qtEntities - Entities from Queue-Times
   * @param wzEntities - Entities from Wartezeiten
   * @returns Array of merged entities with resolved conflicts
   */
  private mergeEntities(
    wikiEntities: any[],
    qtEntities: any[],
    wzEntities: any[] = [],
  ): any[] {
    const merged = new Map<string, MergedEntity>();

    // Step 1: Add all Wiki entities (base/anchor)
    for (const entity of wikiEntities) {
      const key = normalizeForMatching(entity.name);
      merged.set(key, {
        ...entity,
        source: "themeparks-wiki",
        sources: ["themeparks-wiki"],
      });
    }

    // Step 2: Merge Queue-Times entities
    for (const qtEntity of qtEntities) {
      this.addOrMergeEntity(merged, qtEntity, "queue-times");
    }

    // Step 3: Merge Wartezeiten entities
    for (const wzEntity of wzEntities) {
      this.addOrMergeEntity(merged, wzEntity, "wartezeiten-app");
    }

    // Step 4: Resolve wait time conflicts
    for (const [_key, entity] of merged.entries()) {
      const waitTimes: number[] = [];

      if (entity.waitTime) waitTimes.push(entity.waitTime);
      if (entity.qtWaitTime) waitTimes.push(entity.qtWaitTime);
      if (entity.wzWaitTime) waitTimes.push(entity.wzWaitTime);

      if (waitTimes.length > 1) {
        entity.waitTime = this.calculateConsensusWaitTime(waitTimes);
      }

      // Cleanup temporary merge fields
      delete entity.qtWaitTime;
      delete entity.wzWaitTime;
    }

    return Array.from(merged.values());
  }

  /**
   * Add or merge entity into the merged entities map
   *
   * If entity already exists (matched by name), merges data.
   * Otherwise, adds as new entity.
   *
   * @param merged - Map of normalized name to merged entity
   * @param entity - Entity to add or merge
   * @param sourceName - Source identifier (e.g., "queue-times")
   */
  private addOrMergeEntity(
    merged: Map<string, MergedEntity>,
    entity: any,
    sourceName: string,
  ): void {
    const key = normalizeForMatching(entity.name);

    if (merged.has(key)) {
      // Entity exists: merge data
      const existing = merged.get(key)!;
      existing.sources.push(sourceName);

      // Store source-specific wait time for later conflict resolution
      if (entity.waitTime) {
        const sourceKey = this.getSourceKey(sourceName);
        existing[`${sourceKey}WaitTime`] = entity.waitTime;
      }
    } else {
      // New entity: add to map
      merged.set(key, {
        ...entity,
        source: sourceName,
        sources: [sourceName],
      });
    }
  }

  /**
   * Get short key for source name
   *
   * Maps full source names to abbreviated keys for temporary field names.
   *
   * @param sourceName - Full source name (e.g., "queue-times")
   * @returns Abbreviated key (e.g., "qt")
   */
  private getSourceKey(sourceName: string): string {
    const keyMap: Record<string, string> = {
      "queue-times": "qt",
      "wartezeiten-app": "wz",
    };
    return keyMap[sourceName] || sourceName;
  }

  /**
   * Calculate wait time from multiple sources using consensus-based approach
   *
   * Strategy:
   * - **3 sources, 2 agree**: Use consensus value (majority vote)
   * - **3 sources, all different**: Use median (robust against outliers)
   * - **2 sources**: Use average
   * - **1 source**: Use that value
   * - **Always**: Round to nearest 5 minutes
   *
   * @param waitTimes - Array of wait times from different sources (in minutes)
   * @returns Calculated wait time rounded to nearest 5 minutes
   *
   * @example
   * ```ts
   * // Consensus: 2 of 3 agree
   * calculateConsensusWaitTime([25, 25, 30]); // Returns 25
   *
   * // Outlier protection: all different
   * calculateConsensusWaitTime([5, 10, 90]); // Returns 10 (median)
   *
   * // Average: 2 sources
   * calculateConsensusWaitTime([20, 30]); // Returns 25
   * ```
   */
  private calculateConsensusWaitTime(waitTimes: number[]): number {
    if (waitTimes.length === 0) return 0;
    if (waitTimes.length === 1) {
      return (
        Math.round(waitTimes[0] / this.WAIT_TIME_ROUNDING_INTERVAL) *
        this.WAIT_TIME_ROUNDING_INTERVAL
      );
    }

    let finalWaitTime: number;

    if (waitTimes.length === 3) {
      // Sort for consensus detection and median calculation
      const sorted = [...waitTimes].sort((a, b) => a - b);

      // Check for consensus (2 of 3 agree)
      if (sorted[0] === sorted[1]) {
        finalWaitTime = sorted[0]; // First two agree
      } else if (sorted[1] === sorted[2]) {
        finalWaitTime = sorted[1]; // Last two agree
      } else {
        // All different: use median (robust against outliers)
        finalWaitTime = sorted[1];
      }
    } else {
      // 2 sources: use average (= median for 2 values)
      finalWaitTime = waitTimes.reduce((a, b) => a + b, 0) / waitTimes.length;
    }

    // Round to nearest interval (e.g., 5 minutes)
    return (
      Math.round(finalWaitTime / this.WAIT_TIME_ROUNDING_INTERVAL) *
      this.WAIT_TIME_ROUNDING_INTERVAL
    );
  }

  /**
   * Compare wait times from different sources for validation
   *
   * Logs warnings when wait times from multiple sources differ significantly.
   * Only compares if timestamps are within 10 minutes (ensures fresh data).
   *
   * @param wiki - Wiki live data response
   * @param qt - Queue-Times live data response (optional)
   * @param wz - Wartezeiten live data response (optional)
   */
  private compareWaitTimes(
    wiki: LiveDataResponse,
    qt?: LiveDataResponse,
    wz?: LiveDataResponse,
  ): void {
    for (const wikiEntity of wiki.entities) {
      const matches: { source: string; waitTime: number; lastUpdated: Date }[] =
        [];

      // Find matching entities in other sources by name
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

      // Collect wait times from all available sources
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

      // Only compare if we have 2+ sources with recent data
      if (matches.length >= 2) {
        const timestamps = matches.map((m) => m.lastUpdated.getTime());
        const maxTimeDiff = Math.max(...timestamps) - Math.min(...timestamps);

        // Ensure timestamps are within comparison window (10 minutes)
        if (maxTimeDiff <= this.TIMESTAMP_COMPARISON_WINDOW) {
          const waitTimes = matches.map((m) => m.waitTime);
          const min = Math.min(...waitTimes);
          const max = Math.max(...waitTimes);
          const diff = max - min;

          // Log warning if discrepancy exceeds threshold
          if (diff > this.WAIT_TIME_DISCREPANCY_THRESHOLD) {
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
   *
   * Logs warnings when operating hours differ between Wiki and Wartezeiten.
   * Used for quality assurance and data validation.
   *
   * @param parkId - Park identifier for logging
   * @param wiki - Operating hours from Wiki
   * @param wz - Operating hours from Wartezeiten
   */
  private compareOperatingHours(
    parkId: string,
    wiki: OperatingWindow[],
    wz: OperatingWindow[],
  ): void {
    const wikiWindow = wiki[0];
    const wzWindow = wz[0];

    if (wikiWindow && wzWindow) {
      // Compare open/close times (ISO timestamp comparison)
      const openDiff = wikiWindow.open !== wzWindow.open;
      const closeDiff = wikiWindow.close !== wzWindow.close;

      if (openDiff || closeDiff) {
        this.logger.warn(
          `⚠️ Operating Hours Discrepancy for ${parkId}: ` +
            `Wiki(${wikiWindow.open}-${wikiWindow.close}) vs ` +
            `Wartezeiten(${wzWindow.open}-${wzWindow.close})`,
        );
      }
    }
  }
}
