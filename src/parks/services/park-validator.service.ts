import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, Not, IsNull } from "typeorm";
import { Park } from "../entities/park.entity";
import { QueueTimesClient } from "../../external-apis/queue-times/queue-times.client";
import { WartezeitenClient } from "../../external-apis/wartezeiten/wartezeiten.client";
import { normalizeForMatching } from "../../common/utils/slug.util";
import { calculateHaversineDistance } from "../../common/utils/distance.util";
import { extractQueueTimesNumericId } from "../../common/utils/external-id.util";
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

/**
 * Two rows may describe one place while their names barely agree, so long as
 * every physical fact does. `Wet'n'Wild` (ThemeParks.wiki) and
 * `Wet 'n' Wild Gold Coast` (Queue-Times) are one water park in Oxenford,
 * Queensland, down to the same thirteen slides — and they score 0.6923 on
 * names, which is under every threshold `findDuplicates` had.
 *
 * The three constants below are the branch that catches that pair. Each was
 * placed against the whole catalogue (213 parks, all carrying coordinates,
 * 22 578 pairs) rather than chosen, because `POST merge-duplicate-parks`
 * with `autoDetect: true` merges whatever this function returns — with no dry
 * run and no review gate, so a false positive deletes a real park.
 */

/**
 * Closer than this and the two rows are not near each other, they are on the
 * same point.
 *
 * The catalogue has exactly five pairs under the 0.05 km this was first
 * proposed at: PortAventura World's three parks on one resort geocode
 * (0.0000 km), the Wet'n'Wild pair (0.0000 km), and
 * `Hurricane Harbor Chicago` against `Six Flags Hurricane Harbor, Rockford`
 * at 0.0424 km — two real parks 110 km apart, of which the Rockford row
 * carries a Gurnee geocode. That last pair also has disjoint sources, so at
 * 0.05 km the name floor below would be the only thing standing between it
 * and an automatic merge. At 0.01 km it is out on geometry instead — and
 * since the floor is 0.65 it is out on the name as well, which is why that
 * figure is what it is.
 *
 * With both conditions in place the nearest row pair this radius has to
 * separate is **0.1174 km** away (`Boonie Bears Adventure Park Linhai`
 * against `Boonie Bears Water Park Linhai`, 0.6923), i.e. 11.7× the radius,
 * with nothing at all in between. At a floor of 0.6 that margin was the
 * Rockford pair's 0.0424 km, 4.2×.
 */
const SHARED_POINT_KM = 0.01;

/**
 * A name floor, because identical coordinates alone describe a resort as
 * readily as a duplicate.
 *
 * It sits under the pair it must catch (0.6923) and far over the only other
 * pairs sharing a point, PortAventura World's own three (0.1600–0.2000).
 *
 * **0.65 rather than 0.60, so that this condition and `SHARED_POINT_KM` hold
 * independently.** At 0.60 exactly one catalogue pair cleared both the floor
 * and `sourcesDisjoint` and was held out by the radius alone: `Hurricane
 * Harbor Chicago` against `Six Flags Hurricane Harbor, Rockford`, 0.6122 at
 * 0.0424 km. That radius is not ours to rely on there — the Rockford row's
 * coordinates are the Gurnee point Queue-Times itself publishes, so one
 * upstream correction moves the pair to 0.0000 km and an automatic merge
 * deletes a real park. Measured over all 213 catalogue parks (22 578 pairs):
 * **63 pairs score in [0.60, 0.65), every one of them two genuinely different
 * parks**, and the closest of the 63 is 0.1901 km apart (`Fantawild FT Wild
 * Land Xiaogan` against `Fantawild Water Park Xiaogan`), i.e. 19× this
 * radius. Raising the floor therefore excludes nothing the radius does not
 * already exclude, and leaves the target pair 0.0423 of margin.
 *
 * It still cannot do more than that, and the measured figures say where its
 * limit is. The dangerous shape is a second venue at one address, and those
 * score AT or ABOVE the target — Legoland Windsor against its water park
 * 0.7429, Alton Towers against its waterpark 0.6923, and `Boonie Bears
 * Adventure Park Linhai` against `Boonie Bears Water Park Linhai` 0.6923 at
 * 0.1174 km. Keeping that class out is `SHARED_POINT_KM`'s job, not this
 * constant's; no two such rows in the catalogue are closer than 0.1174 km.
 */
const SHARED_POINT_NAME_SIMILARITY = 0.65;

/** Whether any of the three upstream sources has given this row an ID. */
function namesASource(park: {
  wikiEntityId: string | null;
  queueTimesEntityId: string | null;
  wartezeitenEntityId: string | null;
}): boolean {
  return !!(
    park.wikiEntityId ||
    park.queueTimesEntityId ||
    park.wartezeitenEntityId
  );
}

/**
 * A position, or null when the row does not have one.
 *
 * Three things the truthiness check this replaces got wrong, and the first is
 * the one that matters here. **`0, 0` is Null Island** — a row whose geocoding
 * failed, not a park in the Gulf of Guinea; `source-id-inheritance.util.ts`
 * refuses it for the same reason, and `queue-times-data-source.ts` writes the
 * API's coordinates through `parseFloat` without filtering, so the value does
 * reach the table. Two such rows are 0.0000 km apart and would satisfy
 * `SHARED_POINT_KM` on no location information at all: before this branch
 * existed they still needed 0.85 on names to be called duplicates, and now
 * they would need 0.65.
 *
 * The other two: `latitude` and `longitude` are `decimal`, which Postgres
 * hands back as strings, so they are coerced here once rather than left to
 * coerce themselves inside the haversine; and a park exactly on the prime
 * meridian read as "no coordinates" under `p.latitude && p.longitude`, which
 * silently excluded it from `geoProximity`. No park in the catalogue sits on
 * the meridian or the equator today (nearest: 0.319° and 1.254°), so fixing
 * that changes nothing now and stops being a trap later.
 */
function usableCoordinate(park: {
  latitude: number | null;
  longitude: number | null;
}): { latitude: number; longitude: number } | null {
  if (park.latitude === null || park.latitude === undefined) return null;
  if (park.longitude === null || park.longitude === undefined) return null;
  const latitude = Number(park.latitude);
  const longitude = Number(park.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  if (latitude === 0 && longitude === 0) return null;
  return { latitude, longitude };
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
      const qtId = this.extractQueueTimesNumericParkId(
        park.queueTimesEntityId!,
      );
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
   * Finds duplicate parks based on city, geo proximity, and name similarity.
   *
   * Every branch but one asks the name first and lets geography confirm it.
   * The exception is `sharedPoint`, where the physical facts lead: two rows on
   * one point that no upstream source lists twice are the same place even when
   * one of them carries a regional suffix the other does not. See
   * `SHARED_POINT_KM`.
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

        // Two rows pointing at the SAME upstream entity are the strongest
        // duplicate signal there is — one external park cannot be two parks.
        // This used to `continue` (skip the pair), which is what hid both
        // real production duplicates: they shared a Queue-Times resp.
        // Wartezeiten ID. Only `parks.externalId` is unique; these three
        // columns are not, so shared values are both possible and damning.
        const sharedWiki = !!(
          p1.wikiEntityId && p1.wikiEntityId === p2.wikiEntityId
        );
        const sharedQueueTimes = !!(
          p1.queueTimesEntityId &&
          p1.queueTimesEntityId === p2.queueTimesEntityId
        );
        const sharedWartezeiten = !!(
          p1.wartezeitenEntityId &&
          p1.wartezeitenEntityId === p2.wartezeitenEntityId
        );
        const sharedEntityId =
          sharedWiki || sharedQueueTimes || sharedWartezeiten;

        const sameCity = p1.city && p2.city && p1.city === p2.city;
        const where1 = usableCoordinate(p1);
        const where2 = usableCoordinate(p2);
        const distanceKm =
          where1 && where2
            ? calculateHaversineDistance(where1, where2, "km")
            : null;
        const geoProximity = distanceKm !== null && distanceKm < 1.0;

        // One upstream source holding an ID for BOTH rows is that source
        // saying it knows two parks here — evidence against a duplicate, not
        // for one, and it is what keeps PortAventura Park and Ferrari Land
        // apart on their shared resort geocode (Queue-Times 19 and 277).
        // A shared *value* is the opposite signal and already has its own
        // branch below; it cannot reach this one, because an equal ID means
        // both rows carry that source and the sources are then not disjoint.
        // The test is structural — it reads what each row IS, not when it was
        // last heard from — and it needs a source on each side, or a row with
        // no IDs at all would be "from a different source" than everything.
        const sourcesDisjoint =
          namesASource(p1) &&
          namesASource(p2) &&
          !(p1.wikiEntityId && p2.wikiEntityId) &&
          !(p1.queueTimesEntityId && p2.queueTimesEntityId) &&
          !(p1.wartezeitenEntityId && p2.wartezeitenEntityId);

        const nameSimilarity = calculateNameSimilarity(p1.name, p2.name);

        // Two sources, one point, and names that still agree on something.
        // The physical facts carry this one; the name only has to rule out a
        // resort whose parks share a geocode.
        const sharedPoint =
          distanceKm !== null &&
          distanceKm < SHARED_POINT_KM &&
          sourcesDisjoint &&
          nameSimilarity >= SHARED_POINT_NAME_SIMILARITY;

        const isDuplicate =
          sharedPoint ||
          (sameCity && nameSimilarity >= 0.85) ||
          (geoProximity && nameSimilarity >= 0.85) ||
          (nameSimilarity >= 0.98 && (sameCity || geoProximity)) ||
          // Near-identical name + a shared upstream ID. Deliberately does NOT
          // require geo or city: a broken geocode is what makes a ghost row a
          // ghost, and it must not also shield it from detection. The shared
          // ID is the verifier that keeps genuinely distinct same-name parks
          // (Disneyland Paris vs Anaheim) out.
          (nameSimilarity >= 0.95 && sharedEntityId);

        if (isDuplicate) {
          const reasons: string[] = [];
          if (sharedPoint)
            reasons.push("same coordinates, one park per source");
          if (sameCity) reasons.push("same city");
          if (geoProximity) reasons.push("geo proximity < 1km");
          if (nameSimilarity >= 0.98) reasons.push("very high name similarity");
          if (sharedWiki) reasons.push("shared wiki ID");
          if (sharedQueueTimes) reasons.push("shared queue-times ID");
          if (sharedWartezeiten) reasons.push("shared wartezeiten ID");

          duplicates.push({
            park1: { id: p1.id, name: p1.name, city: p1.city },
            park2: { id: p2.id, name: p2.name, city: p2.city },
            score: nameSimilarity,
            reason: reasons.join(", "),
            // "shared" means the same value on both rows — not merely that
            // both rows happen to carry some ID of that kind.
            sharedEntityIds: {
              wiki: sharedWiki,
              queueTimes: sharedQueueTimes,
              wartezeiten: sharedWartezeiten,
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
        .map((r) => this.extractQueueTimesNumericParkId(r.qtId))
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
   * Extract numeric park ID from a prefixed external ID.
   * Wraps the shared string parser and coerces to a number for ID comparisons.
   */
  private extractQueueTimesNumericParkId(externalId: string): number | null {
    const numeric = extractQueueTimesNumericId(externalId);
    return numeric === null ? null : Number(numeric);
  }

  getParkRepository(): Repository<Park> {
    return this.parkRepository;
  }
}
