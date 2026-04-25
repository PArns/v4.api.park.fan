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

    if (parksWithQtId.length === 0) return [];

    const apiParksResponse = await this.queueTimesClient.getParks();
    const apiParks = apiParksResponse.flatMap((group) => group.parks);
    const apiParksById = new Map<number, (typeof apiParks)[0]>();
    for (const p of apiParks) apiParksById.set(p.id, p);

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

      const similarity = calculateNameSimilarity(park.name, apiPark.name);
      const distanceKm = this.getValidDistance(park, apiPark);

      const isNameMismatch =
        similarity < 1.0 &&
        (similarity < 0.8 ||
          normalizeForMatching(park.name) !==
            normalizeForMatching(apiPark.name));
      const isGeoMismatch = distanceKm !== null && distanceKm > 5.0; // Increased threshold for geo mismatch

      if (isNameMismatch || isGeoMismatch) {
        mismatches.push({
          parkId: park.id,
          parkName: park.name,
          city: park.city,
          currentQtId: park.queueTimesEntityId!,
          expectedQtId: null,
          apiParkName: apiPark.name,
          distanceKm,
          similarity,
          reason:
            `${isNameMismatch ? "Name mismatch" : ""} ${isGeoMismatch ? "Geo mismatch" : ""}`.trim(),
        });
      }
    }
    return mismatches;
  }

  private getValidDistance(park: any, apiPark: any): number | null {
    if (
      !park.latitude ||
      !park.longitude ||
      !apiPark.latitude ||
      !apiPark.longitude
    )
      return null;

    const apiLat = parseFloat(apiPark.latitude);
    const apiLng = parseFloat(apiPark.longitude);

    let dist = calculateHaversineDistance(
      { latitude: park.latitude, longitude: park.longitude },
      { latitude: apiLat, longitude: apiLng },
      "km",
    );

    if (dist > 1000) {
      const flipped = calculateHaversineDistance(
        { latitude: park.latitude, longitude: park.longitude },
        { latitude: apiLat, longitude: -apiLng },
        "km",
      );
      if (flipped < 100) return flipped;
    }
    return dist;
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

    if (parksWithWzId.length === 0) return [];

    const apiParks = await this.wartezeitenClient.getParks("en");
    const apiParksByUuid = new Map<string, (typeof apiParks)[0]>();
    for (const p of apiParks) apiParksByUuid.set(p.uuid, p);

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

      // Check for duplicate WZ ID usage across our DB
      const parksWithSameWzId = parksWithWzId.filter(
        (p) => p.wartezeitenEntityId === wzId,
      );
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

      const similarity = calculateNameSimilarity(park.name, apiPark.name);
      const normalizedDbName = normalizeForMatching(park.name);
      const normalizedApiName = normalizeForMatching(apiPark.name);
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
    return mismatches;
  }

  /**
   * Finds duplicate parks based on city, geo proximity, and name similarity
   */
  async findDuplicates(): Promise<DuplicatePair[]> {
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
        const p1 = allParks[i];
        const p2 = allParks[j];

        if (
          (p1.wikiEntityId && p1.wikiEntityId === p2.wikiEntityId) ||
          (p1.queueTimesEntityId &&
            p1.queueTimesEntityId === p2.queueTimesEntityId) ||
          (p1.wartezeitenEntityId &&
            p1.wartezeitenEntityId === p2.wartezeitenEntityId)
        ) {
          continue;
        }

        const sameCity = p1.city && p2.city && p1.city === p2.city;
        let geoProximity = false;
        if (p1.latitude && p1.longitude && p2.latitude && p2.longitude) {
          const dist = calculateHaversineDistance(
            { latitude: p1.latitude, longitude: p1.longitude },
            { latitude: p2.latitude, longitude: p2.longitude },
            "km",
          );
          geoProximity = dist < 1.0;
        }

        const nameSimilarity = calculateNameSimilarity(p1.name, p2.name);
        const isDuplicate =
          (sameCity && nameSimilarity >= 0.85) ||
          (geoProximity && nameSimilarity >= 0.85) ||
          (nameSimilarity >= 0.98 && (sameCity || geoProximity));

        if (isDuplicate) {
          const reasons: string[] = [];
          if (sameCity) reasons.push("same city");
          if (geoProximity) reasons.push("geo proximity < 1km");
          if (nameSimilarity >= 0.98) reasons.push("very high name similarity");

          duplicates.push({
            park1: { id: p1.id, name: p1.name, city: p1.city },
            park2: { id: p2.id, name: p2.name, city: p2.city },
            score: nameSimilarity,
            reason: reasons.join(", "),
            sharedEntityIds: {
              wiki: p1.wikiEntityId !== null && p2.wikiEntityId !== null,
              queueTimes:
                p1.queueTimesEntityId !== null &&
                p2.queueTimesEntityId !== null,
              wartezeiten:
                p1.wartezeitenEntityId !== null &&
                p2.wartezeitenEntityId !== null,
            },
          });
        }
      }
    }
    return duplicates;
  }

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

  async findMissingQueueTimesIds(): Promise<MissingQtId[]> {
    this.logger.log("🔍 Finding parks missing Queue-Times IDs...");

    const parksWithoutQtId = await this.parkRepository.find({
      where: { queueTimesEntityId: IsNull() },
      select: ["id", "name", "city", "latitude", "longitude"],
    });

    if (parksWithoutQtId.length === 0) return [];

    const apiParksResponse = await this.queueTimesClient.getParks();
    const apiParks = apiParksResponse.flatMap((group) => group.parks);

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
        if (usedQtIdSet.has(apiPark.id)) continue;

        let distance: number | null = null;
        if (
          park.latitude &&
          park.longitude &&
          apiPark.latitude &&
          apiPark.longitude
        ) {
          const apiLat = parseFloat(apiPark.latitude);
          const apiLng = parseFloat(apiPark.longitude);
          distance = calculateHaversineDistance(
            { latitude: park.latitude, longitude: park.longitude },
            { latitude: apiLat, longitude: apiLng },
            "km",
          );
          if (distance > 1000) {
            const flipped = calculateHaversineDistance(
              { latitude: park.latitude, longitude: park.longitude },
              { latitude: apiLat, longitude: -apiLng },
              "km",
            );
            if (flipped < 100) distance = flipped;
          }
          if (distance <= 1.0) {
            const similarity = calculateNameSimilarity(park.name, apiPark.name);
            if (!bestMatch || distance < (bestDistance || Infinity)) {
              bestMatch = apiPark;
              bestDistance = distance;
              bestSimilarity = similarity;
            }
          }
        }

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

  async findMissingWartezeitenIds(): Promise<MissingWzId[]> {
    this.logger.log("🔍 Finding parks missing Wartezeiten.app IDs...");

    const parksWithoutWzId = await this.parkRepository.find({
      where: { wartezeitenEntityId: IsNull() },
      select: ["id", "name", "city"],
    });

    if (parksWithoutWzId.length === 0) return [];

    const apiParks = await this.wartezeitenClient.getParks("en");

    const usedWzIds = await this.parkRepository
      .createQueryBuilder("park")
      .select("park.wartezeitenEntityId", "wzId")
      .where("park.wartezeitenEntityId IS NOT NULL")
      .getRawMany();
    const usedWzIdSet = new Set(usedWzIds.map((r) => r.wzId).filter(Boolean));

    const apiParksByName = new Map<string, (typeof apiParks)[0]>();
    for (const park of apiParks) {
      const normalized = normalizeForMatching(park.name);
      if (!apiParksByName.has(normalized)) {
        apiParksByName.set(normalized, park);
      }
    }

    const suggestions: MissingWzId[] = [];

    for (const park of parksWithoutWzId) {
      const apiPark = apiParksByName.get(normalizeForMatching(park.name));
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
   * Extract numeric park ID from prefixed external ID
   */
  private extractQueueTimesNumericId(externalId: string): number | null {
    const clean = externalId.replace("qt-park-", "");
    const id = parseInt(clean, 10);
    return isNaN(id) ? null : id;
  }

  getParkRepository(): Repository<Park> {
    return this.parkRepository;
  }
}
