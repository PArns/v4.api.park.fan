import { Injectable, Logger } from "@nestjs/common";
import { compareTwoStrings } from "string-similarity";
import {
  ParkMetadata,
  EntityMetadata,
} from "./interfaces/data-source.interface";
import { normalizeForMatching } from "../../common/utils/slug.util";

/**
 * Entity Matcher Service
 *
 * Automatically matches parks and attractions across different data sources
 * using fuzzy name matching and geographic proximity.
 */
@Injectable()
export class EntityMatcherService {
  private readonly logger = new Logger(EntityMatcherService.name);

  /**
   * Match parks from different sources
   *
   * @param wikiParks - Parks from ThemeParks.wiki
   * @param qtParks - Parks from Queue-Times
   * @returns Matched parks, wiki-only, and qt-only parks
   */
  matchParks(
    wikiParks: ParkMetadata[],
    qtParks: ParkMetadata[],
  ): {
    matched: Array<{
      wiki: ParkMetadata;
      qt: ParkMetadata;
      confidence: number;
    }>;
    wikiOnly: ParkMetadata[];
    qtOnly: ParkMetadata[];
  } {
    const matched: Array<{
      wiki: ParkMetadata;
      qt: ParkMetadata;
      confidence: number;
    }> = [];
    const wikiOnly: ParkMetadata[] = [];
    const qtOnly = [...qtParks]; // Clone array

    for (const wiki of wikiParks) {
      let bestMatch: ParkMetadata | null = null;
      let bestScore = 0;

      for (const qt of qtOnly) {
        const score = this.calculateParkSimilarity(wiki, qt);
        if (score > bestScore && score > 0.75) {
          // 75% threshold
          bestScore = score;
          bestMatch = qt;
        }
      }

      if (bestMatch) {
        matched.push({ wiki, qt: bestMatch, confidence: bestScore });
        qtOnly.splice(qtOnly.indexOf(bestMatch), 1);

        this.logger.log(
          `✓ Matched: "${wiki.name}" ↔ "${bestMatch.name}" (${(bestScore * 100).toFixed(1)}%)`,
        );
      } else {
        wikiOnly.push(wiki);
        this.logger.log(`✗ No match for Wiki park: "${wiki.name}"`);
      }
    }

    this.logger.log(
      `Park matching complete: ${matched.length} matched, ${wikiOnly.length} wiki-only, ${qtOnly.length} qt-only`,
    );

    return { matched, wikiOnly, qtOnly };
  }

  /**
   * Calculate similarity between two parks
   *
   * @param wiki - Wiki park
   * @param qt - Queue-Times park
   * @returns Similarity score (0-1)
   */
  private calculateParkSimilarity(
    wiki: ParkMetadata,
    qt: ParkMetadata,
  ): number {
    const n1 = normalizeForMatching(wiki.name);
    const n2 = normalizeForMatching(qt.name);

    // 1. Name similarity (60% weight)
    let rawNameSim = compareTwoStrings(n1, n2);

    // Boost if one includes the other (e.g. "Animal Kingdom" in "Disney's Animal Kingdom")
    // Check length to avoid matching "The" in "The Park"
    if (
      (n1.length > 5 && n2.includes(n1)) ||
      (n2.length > 5 && n1.includes(n2))
    ) {
      rawNameSim = Math.max(rawNameSim, 0.9);
    }

    const nameSim = rawNameSim * 0.6;

    // 2. Geographic proximity (40% weight)
    let geoSim = 0;

    // Check for valid coordinates (ignore 0,0)
    const hasValidWikiGeo =
      wiki.latitude &&
      wiki.longitude &&
      (Math.abs(wiki.latitude) > 0.1 || Math.abs(wiki.longitude) > 0.1);
    const hasValidQtGeo =
      qt.latitude &&
      qt.longitude &&
      (Math.abs(qt.latitude) > 0.1 || Math.abs(qt.longitude) > 0.1);

    if (hasValidWikiGeo && hasValidQtGeo) {
      const distance = this.haversineDistance(
        { latitude: wiki.latitude!, longitude: wiki.longitude! },
        { latitude: qt.latitude!, longitude: qt.longitude! },
      );

      // Smart Sign Correction: Check if flipping longitude fixes the match
      // This handles cases where one source has East positive vs West negative error
      if (distance > 1000) {
        const flippedDist = this.haversineDistance(
          { latitude: wiki.latitude!, longitude: wiki.longitude! },
          { latitude: qt.latitude!, longitude: -qt.longitude! }, // Try flipping sign
        );

        if (flippedDist < 100) {
          this.logger.debug(
            `Found sign error match for ${wiki.name}: ${distance}km -> ${flippedDist}km`,
          );
          // Use the flipped distance
          geoSim = (1 - Math.min(flippedDist / 10, 1)) * 0.4;
        } else {
          geoSim = (1 - Math.min(distance / 10, 1)) * 0.4;
        }
      } else {
        geoSim = (1 - Math.min(distance / 10, 1)) * 0.4;
      }
    } else {
      // Fallback if one or both missing valid geo: Name is 100% of score
      return nameSim / 0.6;
    }

    return nameSim + geoSim;
  }

  /**
   * Match entities (attractions, shows, restaurants) within a park
   *
   * @param source1Entities - Entities from source 1 (e.g. Wiki)
   * @param source2Entities - Entities from source 2 (e.g. Queue-Times)
   */
  matchEntities(
    source1Entities: EntityMetadata[],
    source2Entities: EntityMetadata[],
  ): {
    matched: Array<{
      entity1: EntityMetadata;
      entity2: EntityMetadata;
      confidence: number;
    }>;
    unmatched1: EntityMetadata[];
    unmatched2: EntityMetadata[];
  } {
    const matched: Array<{
      entity1: EntityMetadata;
      entity2: EntityMetadata;
      confidence: number;
    }> = [];

    const unmatched1: EntityMetadata[] = [];
    const unmatched2 = [...source2Entities]; // Clone for manipulation

    for (const entity1 of source1Entities) {
      let bestMatch: EntityMetadata | null = null;
      let bestScore = 0;

      for (const entity2 of unmatched2) {
        // Skip if types don't match (e.g. don't match Show with Attraction)
        if (entity1.entityType !== entity2.entityType) {
          continue;
        }

        const score = this.calculateEntitySimilarity(entity1, entity2);
        if (score > bestScore && score > 0.8) {
          // 80% threshold
          bestScore = score;
          bestMatch = entity2;
        }
      }

      if (bestMatch) {
        matched.push({
          entity1,
          entity2: bestMatch,
          confidence: bestScore,
        });
        // Remove from pool to avoid double matching
        const index = unmatched2.indexOf(bestMatch);
        if (index > -1) {
          unmatched2.splice(index, 1);
        }
      } else {
        unmatched1.push(entity1);
      }
    }

    return { matched, unmatched1, unmatched2 };
  }

  /**
   * Calculate similarity between two entities
   */
  private calculateEntitySimilarity(
    e1: EntityMetadata,
    e2: EntityMetadata,
  ): number {
    // Name similarity (80% weight)
    const n1 = normalizeForMatching(e1.name);
    const n2 = normalizeForMatching(e2.name);

    // Name similarity (80% weight)
    let rawNameSim = compareTwoStrings(n1, n2);

    // Boost if substring
    if (
      (n1.length > 5 && n2.includes(n1)) ||
      (n2.length > 5 && n1.includes(n2))
    ) {
      rawNameSim = Math.max(rawNameSim, 0.9);
    }

    const nameSim = rawNameSim * 0.8;

    // Location similarity (20% weight) - if both have location
    let geoSim = 0;

    if (e1.latitude && e1.longitude && e2.latitude && e2.longitude) {
      const distance = this.haversineDistance(
        { latitude: e1.latitude, longitude: e1.longitude },
        { latitude: e2.latitude, longitude: e2.longitude },
      );
      // Close = within 100m = 1.0 score
      // Far = > 1km = 0.0 score
      // Linear decay
      const maxDist = 1.0; // km
      geoSim = Math.max(0, 1 - distance / maxDist) * 0.2;
    } else {
      // If missing geo, normalize name score to 100%
      // e.g. 0.8 score becomes 1.0 equivalent
      return nameSim / 0.8;
    }

    return nameSim + geoSim;
  }

  /**
   * Calculate distance between two coordinates using Haversine formula
   *
   * @returns Distance in kilometers
   */
  private haversineDistance(
    p1: { latitude: number; longitude: number },
    p2: { latitude: number; longitude: number },
  ): number {
    const R = 6371; // Earth radius in km
    const dLat = this.toRad(p2.latitude - p1.latitude);
    const dLon = this.toRad(p2.longitude - p1.longitude);

    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(this.toRad(p1.latitude)) *
        Math.cos(this.toRad(p2.latitude)) *
        Math.sin(dLon / 2) *
        Math.sin(dLon / 2);

    return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  private toRad(deg: number): number {
    return (deg * Math.PI) / 180;
  }
}
