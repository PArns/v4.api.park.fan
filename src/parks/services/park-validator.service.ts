import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Not, IsNull } from "typeorm";
import { Park } from "../entities/park.entity";
import { QueueTimesClient } from "../../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";
import { normalizeForMatching } from "../../common/utils/slug.util";
import { calculateHaversineDistance } from "../../common/utils/distance.util";
import { calculateNameSimilarity } from "../utils/park-merge.util";

export interface MismatchedQtId {
  parkId: string;
  parkName: string;
  city: string | null;
  currentQtId: string;
  expectedQtId: string | null;
  apiParkName: string;
  distanceKm: number | null;
  similarity: number;
  reason: string;
}

export interface MismatchedWzId {
  parkId: string;
  parkName: string;
  city: string | null;
  currentWzId: string;
  expectedWzId: string | null;
  apiParkName: string;
  similarity: number;
  reason: string;
}

export interface MissingQtId {
  parkId: string;
  parkName: string;
  city: string | null;
  suggestedQtId: string;
  apiParkName: string;
  distanceKm: number | null;
  similarity: number;
}

export interface MissingWzId {
  parkId: string;
  parkName: string;
  city: string | null;
  suggestedWzId: string;
  apiParkName: string;
  similarity: number;
}

export interface DuplicatePair {
  park1: { id: string; name: string; city: string | null };
  park2: { id: string; name: string; city: string | null };
  score: number;
  reason: string;
  sharedEntityIds: {
    wiki?: boolean;
    queueTimes?: boolean;
    wartezeiten?: boolean;
  };
}

export interface ValidationReport {
  mismatchedQtIds: MismatchedQtId[];
  mismatchedWzIds: MismatchedWzId[];
  missingQtIds: MissingQtId[];
  missingWzIds: MissingWzId[];
  duplicates: DuplicatePair[];
  summary: {
    totalParks: number;
    parksWithQtId: number;
    parksWithWzId: number;
    issuesFound: number;
  };
}

@Injectable()
export class ParkValidatorService {
  private readonly logger = new Logger(ParkValidatorService.name);

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    private readonly queueTimesClient: QueueTimesClient,
    private readonly wartezeitenClient: WartezeitenClient,
  ) {}

  /**
   * Validates all Queue-Times IDs against the Queue-Times API
   */
  async validateQueueTimesIds(): Promise<MismatchedQtId[]> {
    this.logger.log("🔍 Validating Queue-Times IDs...");

    const parksWithQtId = await this.parkRepository.find({
      where: { queueTimesEntityId: Not(IsNull()) },
      select: [
        "id",
        "name",
        "city",
        "queueTimesEntityId",
        "latitude",
        "longitude",
      ],
    });

    if (parksWithQtId.length === 0) {
      this.logger.log("No parks with Queue-Times IDs found");
      return [];
    }

    // Fetch all parks from Queue-Times API
    const apiParksResponse = await this.queueTimesClient.getParks();
    const apiParks = apiParksResponse.flatMap((group) => group.parks);

    // Build lookup map by ID
    const apiParksById = new Map<number, (typeof apiParks)[0]>();
    for (const park of apiParks) {
      apiParksById.set(park.id, park);
    }

    const mismatches: MismatchedQtId[] = [];

    for (const park of parksWithQtId) {
      const qtId = this.extractQueueTimesNumericId(park.queueTimesEntityId!);
      if (!qtId) {
        mismatches.push({
          parkId: park.id,
          parkName: park.name,
          city: park.city,
          currentQtId: park.queueTimesEntityId!,
          expectedQtId: null,
          apiParkName: "",
          distanceKm: null,
          similarity: 0,
          reason: "Invalid QT-ID format",
        });
        continue;
      }

      const apiPark = apiParksById.get(qtId);
      if (!apiPark) {
        mismatches.push({
          parkId: park.id,
          parkName: park.name,
          city: park.city,
          currentQtId: park.queueTimesEntityId!,
          expectedQtId: null,
          apiParkName: "",
          distanceKm: null,
          similarity: 0,
          reason: "QT-ID not found in API",
        });
        continue;
      }

      // Check name similarity
      const similarity = calculateNameSimilarity(park.name, apiPark.name);
      const normalizedDbName = normalizeForMatching(park.name);
      const normalizedApiName = normalizeForMatching(apiPark.name);

      // Check geo distance if both have coordinates
      let distanceKm: number | null = null;
      if (
        park.latitude &&
        park.longitude &&
        apiPark.latitude &&
        apiPark.longitude
      ) {
        const apiLat = parseFloat(apiPark.latitude);
        const apiLng = parseFloat(apiPark.longitude);

        // Calculate initial distance
        distanceKm = calculateHaversineDistance(
          { latitude: park.latitude, longitude: park.longitude },
          { latitude: apiLat, longitude: apiLng },
          "km",
        );

        // Smart Sign Correction: Check if flipping longitude fixes the match
        // This handles cases where one source has East positive vs West negative error
        // (e.g., -81.5 vs 81.5 for Orlando, FL)
        if (distanceKm > 1000) {
          const flippedDistance = calculateHaversineDistance(
            { latitude: park.latitude, longitude: park.longitude },
            { latitude: apiLat, longitude: -apiLng }, // Try flipping sign
            "km",
          );

          // If flipped distance is much better (<100km), use it and note the sign error
          if (flippedDistance < 100) {
            this.logger.debug(
              `Found sign error for ${park.name}: ${distanceKm.toFixed(2)}km -> ${flippedDistance.toFixed(2)}km (flipped longitude)`,
            );
            distanceKm = flippedDistance;
            // Note: We don't auto-fix the coordinate here, just use corrected distance for validation
            // The actual coordinate fix should be done manually or via repair service
          }
        }
      }

      // Determine if it's a mismatch
      // If similarity is 100%, consider it correct even if normalized names differ slightly
      // (e.g., due to special characters like ® that might be handled differently)
      const isNameMismatch =
        similarity < 1.0 &&
        (similarity < 0.8 || normalizedDbName !== normalizedApiName);
      const isGeoMismatch =
        distanceKm !== null && distanceKm !== undefined && distanceKm > 1.0;

      if (isNameMismatch || isGeoMismatch) {
        const reasons: string[] = [];
        if (isNameMismatch) {
          reasons.push(
            `Name mismatch (similarity: ${(similarity * 100).toFixed(1)}%)`,
          );
        }
        if (isGeoMismatch) {
          const distanceStr =
            distanceKm !== null && distanceKm !== undefined
              ? `${distanceKm.toFixed(2)}km`
              : "unknown";
          reasons.push(`Geo mismatch (distance: ${distanceStr})`);
        }

        mismatches.push({
          parkId: park.id,
          parkName: park.name,
          city: park.city,
          currentQtId: park.queueTimesEntityId!,
          expectedQtId: null, // Will be determined by findMissingQueueTimesIds
          apiParkName: apiPark.name,
          distanceKm,
          similarity,
          reason: reasons.join(", "),
        });
      }
    }

    this.logger.log(
      `✅ Validated ${parksWithQtId.length} parks, found ${mismatches.length} mismatches`,
    );
    return mismatches;
  }

  /**
   * Validates all Wartezeiten.app IDs against the Wartezeiten.app API
   */
  async validateWartezeitenIds(): Promise<MismatchedWzId[]> {
    this.logger.log("🔍 Validating Wartezeiten.app IDs...");

    const parksWithWzId = await this.parkRepository.find({
      where: { wartezeitenEntityId: Not(IsNull()) },
      select: ["id", "name", "city", "wartezeitenEntityId"],
    });

    if (parksWithWzId.length === 0) {
      this.logger.log("No parks with Wartezeiten IDs found");
      return [];
    }

    // Fetch all parks from Wartezeiten.app API
    const apiParks = await this.wartezeitenClient.getParks("en");

    // Build lookup map by UUID
    const apiParksByUuid = new Map<string, (typeof apiParks)[0]>();
    for (const park of apiParks) {
      apiParksByUuid.set(park.uuid, park);
    }

    // Check for duplicate WZ IDs in our database
    const wzIdUsage = new Map<string, string[]>();
    for (const park of parksWithWzId) {
      const wzId = park.wartezeitenEntityId!;
      if (!wzIdUsage.has(wzId)) {
        wzIdUsage.set(wzId, []);
      }
      wzIdUsage.get(wzId)!.push(park.id);
    }

    const mismatches: MismatchedWzId[] = [];

    for (const park of parksWithWzId) {
      const wzId = park.wartezeitenEntityId!;
      const apiPark = apiParksByUuid.get(wzId);

      if (!apiPark) {
        mismatches.push({
          parkId: park.id,
          parkName: park.name,
          city: park.city,
          currentWzId: wzId,
          expectedWzId: null,
          apiParkName: "",
          similarity: 0,
          reason: "WZ-ID not found in API",
        });
        continue;
      }

      // Check for duplicate usage
      const parksWithSameWzId = wzIdUsage.get(wzId)!;
      if (parksWithSameWzId.length > 1) {
        mismatches.push({
          parkId: park.id,
          parkName: park.name,
          city: park.city,
          currentWzId: wzId,
          expectedWzId: null,
          apiParkName: apiPark.name,
          similarity: 0,
          reason: `WZ-ID used by ${parksWithSameWzId.length} parks (duplicate)`,
        });
        continue;
      }

      // Check name similarity
      const similarity = calculateNameSimilarity(park.name, apiPark.name);
      const normalizedDbName = normalizeForMatching(park.name);
      const normalizedApiName = normalizeForMatching(apiPark.name);

      // If similarity is 100%, consider it correct even if normalized names differ slightly
      const isNameMismatch =
        similarity < 1.0 &&
        (similarity < 0.8 || normalizedDbName !== normalizedApiName);

      if (isNameMismatch) {
        mismatches.push({
          parkId: park.id,
          parkName: park.name,
          city: park.city,
          currentWzId: wzId,
          expectedWzId: null,
          apiParkName: apiPark.name,
          similarity,
          reason: `Name mismatch (similarity: ${(similarity * 100).toFixed(1)}%)`,
        });
      }
    }

    this.logger.log(
      `✅ Validated ${parksWithWzId.length} parks, found ${mismatches.length} mismatches`,
    );
    return mismatches;
  }

  /**
   * Finds parks without Queue-Times IDs that should have one
   */
  async findMissingQueueTimesIds(): Promise<MissingQtId[]> {
    this.logger.log("🔍 Finding parks missing Queue-Times IDs...");

    const parksWithoutQtId = await this.parkRepository.find({
      where: { queueTimesEntityId: IsNull() },
      select: ["id", "name", "city", "latitude", "longitude"],
    });

    if (parksWithoutQtId.length === 0) {
      this.logger.log("No parks without Queue-Times IDs found");
      return [];
    }

    // Fetch all parks from Queue-Times API
    const apiParksResponse = await this.queueTimesClient.getParks();
    const apiParks = apiParksResponse.flatMap((group) => group.parks);

    // Get all currently used QT IDs
    const usedQtIds = await this.parkRepository
      .createQueryBuilder("park")
      .select("park.queueTimesEntityId", "qtId")
      .where("park.queueTimesEntityId IS NOT NULL")
      .getRawMany();
    const usedQtIdSet = new Set(
      usedQtIds
        .map((r) => this.extractQueueTimesNumericId(r.qtId))
        .filter(Boolean),
    );

    const suggestions: MissingQtId[] = [];

    for (const park of parksWithoutQtId) {
      let bestMatch: (typeof apiParks)[0] | null = null;
      let bestDistance: number | null = null;
      let bestSimilarity = 0;

      for (const apiPark of apiParks) {
        // Skip if already used
        if (usedQtIdSet.has(apiPark.id)) {
          continue;
        }

        // Try geo matching first
        let distance: number | null = null;
        if (
          park.latitude &&
          park.longitude &&
          apiPark.latitude &&
          apiPark.longitude
        ) {
          const apiLat = parseFloat(apiPark.latitude);
          const apiLng = parseFloat(apiPark.longitude);

          // Calculate initial distance
          distance = calculateHaversineDistance(
            { latitude: park.latitude, longitude: park.longitude },
            { latitude: apiLat, longitude: apiLng },
            "km",
          );

          // Smart Sign Correction: Check if flipping longitude fixes the match
          if (distance > 1000) {
            const flippedDistance = calculateHaversineDistance(
              { latitude: park.latitude, longitude: park.longitude },
              { latitude: apiLat, longitude: -apiLng }, // Try flipping sign
              "km",
            );

            // If flipped distance is much better (<100km), use it
            if (flippedDistance < 100) {
              this.logger.debug(
                `Found sign error for ${park.name}: ${distance.toFixed(2)}km -> ${flippedDistance.toFixed(2)}km (flipped longitude)`,
              );
              distance = flippedDistance;
            }
          }

          // If within 1km, this is a strong candidate
          if (distance <= 1.0) {
            const similarity = calculateNameSimilarity(park.name, apiPark.name);
            if (!bestMatch || distance < (bestDistance || Infinity)) {
              bestMatch = apiPark;
              bestDistance = distance;
              bestSimilarity = similarity;
            }
          }
        }

        // Fallback to name matching if no geo match found
        if (!bestMatch) {
          const similarity = calculateNameSimilarity(park.name, apiPark.name);
          if (similarity >= 0.85 && similarity > bestSimilarity) {
            bestMatch = apiPark;
            bestDistance = distance;
            bestSimilarity = similarity;
          }
        }
      }

      if (bestMatch) {
        suggestions.push({
          parkId: park.id,
          parkName: park.name,
          city: park.city,
          suggestedQtId: `qt-park-${bestMatch.id}`,
          apiParkName: bestMatch.name,
          distanceKm: bestDistance,
          similarity: bestSimilarity,
        });
      }
    }

    this.logger.log(
      `✅ Found ${suggestions.length} parks that might need Queue-Times IDs`,
    );
    return suggestions;
  }

  /**
   * Finds parks without Wartezeiten.app IDs that should have one
   */
  async findMissingWartezeitenIds(): Promise<MissingWzId[]> {
    this.logger.log("🔍 Finding parks missing Wartezeiten.app IDs...");

    const parksWithoutWzId = await this.parkRepository.find({
      where: { wartezeitenEntityId: IsNull() },
      select: ["id", "name", "city"],
    });

    if (parksWithoutWzId.length === 0) {
      this.logger.log("No parks without Wartezeiten IDs found");
      return [];
    }

    // Fetch all parks from Wartezeiten.app API
    const apiParks = await this.wartezeitenClient.getParks("en");

    // Get all currently used WZ IDs
    const usedWzIds = await this.parkRepository
      .createQueryBuilder("park")
      .select("park.wartezeitenEntityId", "wzId")
      .where("park.wartezeitenEntityId IS NOT NULL")
      .getRawMany();
    const usedWzIdSet = new Set(usedWzIds.map((r) => r.wzId).filter(Boolean));

    // Build name lookup map (normalized)
    const apiParksByName = new Map<string, (typeof apiParks)[0]>();
    for (const park of apiParks) {
      const normalized = normalizeForMatching(park.name);
      if (!apiParksByName.has(normalized)) {
        apiParksByName.set(normalized, park);
      }
    }

    const suggestions: MissingWzId[] = [];

    for (const park of parksWithoutWzId) {
      const normalizedDbName = normalizeForMatching(park.name);
      const apiPark = apiParksByName.get(normalizedDbName);

      if (apiPark && !usedWzIdSet.has(apiPark.uuid)) {
        const similarity = calculateNameSimilarity(park.name, apiPark.name);
        if (similarity >= 0.85) {
          suggestions.push({
            parkId: park.id,
            parkName: park.name,
            city: park.city,
            suggestedWzId: apiPark.uuid,
            apiParkName: apiPark.name,
            similarity,
          });
        }
      }
    }

    this.logger.log(
      `✅ Found ${suggestions.length} parks that might need Wartezeiten IDs`,
    );
    return suggestions;
  }

  /**
   * Finds duplicate parks based on city, geo proximity, and name similarity
   */
  async findDuplicates(): Promise<DuplicatePair[]> {
    this.logger.log("🔍 Finding duplicate parks...");

    const allParks = await this.parkRepository.find({
      select: [
        "id",
        "name",
        "city",
        "wikiEntityId",
        "queueTimesEntityId",
        "wartezeitenEntityId",
        "latitude",
        "longitude",
      ],
    });

    const duplicates: DuplicatePair[] = [];

    for (let i = 0; i < allParks.length; i++) {
      for (let j = i + 1; j < allParks.length; j++) {
        const park1 = allParks[i];
        const park2 = allParks[j];

        // Skip if they already share an entity ID (they're already linked)
        if (
          (park1.wikiEntityId &&
            park2.wikiEntityId &&
            park1.wikiEntityId === park2.wikiEntityId) ||
          (park1.queueTimesEntityId &&
            park2.queueTimesEntityId &&
            park1.queueTimesEntityId === park2.queueTimesEntityId) ||
          (park1.wartezeitenEntityId &&
            park2.wartezeitenEntityId &&
            park1.wartezeitenEntityId === park2.wartezeitenEntityId)
        ) {
          continue;
        }

        // Check city match
        const sameCity = park1.city && park2.city && park1.city === park2.city;

        // Check geo proximity
        let geoProximity = false;
        if (
          park1.latitude &&
          park1.longitude &&
          park2.latitude &&
          park2.longitude
        ) {
          const distance = calculateHaversineDistance(
            { latitude: park1.latitude, longitude: park1.longitude },
            { latitude: park2.latitude, longitude: park2.longitude },
            "km",
          );
          geoProximity = distance < 1.0;
        }

        // Check name similarity
        const nameSimilarity = calculateNameSimilarity(park1.name, park2.name);

        // Determine if duplicate
        // IMPORTANT: Require either same city OR geo proximity for high name similarity
        // This prevents false positives like "Disneyland Park" (Anaheim) vs "Disneyland Park" (Paris)
        const isDuplicate =
          (sameCity && nameSimilarity >= 0.85) ||
          (geoProximity && nameSimilarity >= 0.85) ||
          (nameSimilarity >= 0.98 && (sameCity || geoProximity)); // Very high similarity still needs location match

        if (isDuplicate) {
          const reasons: string[] = [];
          if (sameCity) reasons.push("same city");
          if (geoProximity) reasons.push("geo proximity < 1km");
          if (nameSimilarity >= 0.98) reasons.push("very high name similarity");

          duplicates.push({
            park1: { id: park1.id, name: park1.name, city: park1.city },
            park2: { id: park2.id, name: park2.name, city: park2.city },
            score: nameSimilarity,
            reason: reasons.join(", "),
            sharedEntityIds: {
              wiki: park1.wikiEntityId !== null && park2.wikiEntityId !== null,
              queueTimes:
                park1.queueTimesEntityId !== null &&
                park2.queueTimesEntityId !== null,
              wartezeiten:
                park1.wartezeitenEntityId !== null &&
                park2.wartezeitenEntityId !== null,
            },
          });
        }
      }
    }

    this.logger.log(`✅ Found ${duplicates.length} duplicate pairs`);
    return duplicates;
  }

  /**
   * Runs complete validation and returns comprehensive report
   */
  async validateAll(): Promise<ValidationReport> {
    this.logger.log("🚀 Starting complete park validation...");

    const [
      mismatchedQtIds,
      mismatchedWzIds,
      missingQtIds,
      missingWzIds,
      duplicates,
    ] = await Promise.all([
      this.validateQueueTimesIds(),
      this.validateWartezeitenIds(),
      this.findMissingQueueTimesIds(),
      this.findMissingWartezeitenIds(),
      this.findDuplicates(),
    ]);

    const totalParks = await this.parkRepository.count();
    const parksWithQtId = await this.parkRepository.count({
      where: { queueTimesEntityId: Not(IsNull()) },
    });
    const parksWithWzId = await this.parkRepository.count({
      where: { wartezeitenEntityId: Not(IsNull()) },
    });

    const issuesFound =
      mismatchedQtIds.length +
      mismatchedWzIds.length +
      missingQtIds.length +
      missingWzIds.length +
      duplicates.length;

    this.logger.log(
      `✅ Validation complete: ${issuesFound} issues found (${mismatchedQtIds.length} QT mismatches, ${mismatchedWzIds.length} WZ mismatches, ${missingQtIds.length} missing QT IDs, ${missingWzIds.length} missing WZ IDs, ${duplicates.length} duplicates)`,
    );

    return {
      mismatchedQtIds,
      mismatchedWzIds,
      missingQtIds,
      missingWzIds,
      duplicates,
      summary: {
        totalParks,
        parksWithQtId,
        parksWithWzId,
        issuesFound,
      },
    };
  }

  /**
   * Extract numeric park ID from prefixed external ID
   * @param externalId - Prefixed ID like "qt-park-56" or legacy "56"
   * @returns Numeric park ID (56) or null if invalid
   */
  private extractQueueTimesNumericId(externalId: string): number | null {
    if (externalId.startsWith("qt-park-")) {
      const id = parseInt(externalId.replace("qt-park-", ""), 10);
      return isNaN(id) ? null : id;
    }
    // Legacy format: just the number
    const id = parseInt(externalId, 10);
    return isNaN(id) ? null : id;
  }

  /**
   * Get park repository (for external access)
   * Exposed for admin controller to determine winners
   */
  getParkRepository(): Repository<Park> {
    return this.parkRepository;
  }
}
