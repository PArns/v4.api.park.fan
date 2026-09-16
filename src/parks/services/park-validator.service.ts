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
 * The branch that catches it asks three conditions: the two constants below
 * and the source-disjointness test in `findDuplicates` itself. Both constants
 * were placed against the whole catalogue (213 parks, all carrying
 * coordinates, 22 578 pairs) rather than chosen, because this function's
 * result is what `POST merge-duplicate-parks` acts on, and a false positive
 * there deletes a real park.
 *
 * Since PAR-247 it is no longer the only thing between a false positive and
 * that deletion: every pair carries `safe`, `autoDetect` merges the safe ones
 * alone, and it writes nothing at all without `dryRun: false`. A `sharedPoint`
 * pair is never safe. The constants still decide what an operator is shown,
 * which is why they stay measured rather than estimated.
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
 * parks.** The closest of the 63 is the Rockford pair itself at 0.0424 km —
 * it sits in this band, which is the whole reason the band matters — and the
 * closest of the other 62 is 0.1901 km away (`Fantawild FT Wild Land Xiaogan`
 * against `Fantawild Water Park Xiaogan`), i.e. 19× this radius. So all 63
 * are outside the radius already: raising the floor excludes nothing the
 * radius was not excluding, and leaves the target pair 0.0423 of margin.
 *
 * It still cannot do more than that, and the measured figures say where its
 * limit is. The dangerous shape is a second venue at one address, and those
 * score AT or ABOVE the target — Legoland Windsor against its water park
 * 0.7429, Alton Towers against its waterpark 0.6923, and `Boonie Bears
 * Adventure Park Linhai` against `Boonie Bears Water Park Linhai` 0.6923 at
 * 0.1174 km. Where such a venue carries its OWN geocode, keeping it out is
 * `SHARED_POINT_KM`'s job, not this constant's: no two such rows in the
 * catalogue are closer than 0.1174 km.
 *
 * **The residual risk is the venue that inherits the resort's geocode**, and
 * it is worth stating because the radius has no vote there at all. Three rows
 * do it today — PortAventura Park, Ferrari Land and Caribe Aquatic Park, all
 * on 41.0986786/1.1517730, 0.0000 km apart — and the NAME is what keeps all
 * three pairs apart, at 0.1600–0.2000 against this floor. `sourcesDisjoint`
 * additionally fails for one of the three, PortAventura Park against Ferrari
 * Land, because Queue-Times lists both; Caribe Aquatic Park carries a
 * wartezeiten id and no other, so its two pairs are disjoint and rest on the
 * floor alone. The margin there is wide. But a water park
 * that synced in on its resort's point, from a source the theme-park row does
 * not carry, with a name like `Legoland Windsor` against its water park
 * (0.7429), would satisfy all three conditions. No such row exists in the
 * catalogue today (measured: nothing above this floor sits closer than
 * 0.1174 km except the pair this branch is for), and since PAR-247 such a row
 * would be reported rather than merged: `sharedPoint` cannot be `safe`, so
 * `autoDetect` leaves it for a human whatever the geometry says.
 *
 * **Any shared placeholder geocode is the same hazard**, not only a resort's.
 * `usableCoordinate` refuses `0, 0` because that is the placeholder this repo
 * writes, but a source falling back to a city or state centroid puts two rows
 * on one point just as exactly, and a value-specific refusal cannot see it.
 * Another threshold does not help — the radius cannot tell a shared address
 * from a shared fallback — which is why the review gate and not this constant
 * is what stands between such a pair and a deletion.
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
 * Three things the truthiness check got wrong, and the first is the one that
 * matters here. It is replaced in `findDuplicates` only: `getValidDistance`
 * and `findMissingQueueTimesIds` further down still test `park.latitude &&
 * park.longitude` and hand the raw `decimal` strings to the haversine. Both
 * belong to the Queue-Times id matching rather than to duplicate detection,
 * so they are PAR-258's along with the third copy of these rules in
 * `source-id-inheritance.util.ts`, not this branch's.
 *
 * **`0, 0` is Null Island** — a row whose geocoding
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
 * silently excluded it from `geoProximity`. That last one only ever bit on
 * NUMERIC input — a unit test, or a future column transformer — because
 * Postgres hands back `"0.0000000"`, and that string is truthy. No park in
 * the catalogue sits on the meridian or the equator today (nearest: 0.319°
 * and 1.254°), so it changes nothing now and stops being a trap later.
 *
 * **It narrows the four name-led branches, deliberately.** Because
 * `"0.0000000"` is truthy, two rows whose geocoding failed did pass the old
 * check, and `geoProximity` then read them as 0.0000 km apart — so
 * `geoProximity && nameSimilarity >= 0.85` could fire on two rows with no
 * location information at all. It no longer can. That is the intended
 * direction: 0 km between two failed geocodes is not evidence of proximity,
 * and a genuine ghost pair is still reachable through `sameCity` and through
 * `nameSimilarity >= 0.95 && sharedEntityId`, neither of which asks about
 * geometry. No catalogue row sits at `0, 0` today, so nothing changes yet.
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

/**
 * The name floor a pair needs before an automatic merge may delete one of its
 * two rows.
 *
 * It is the floor of the `nameSimilarity >= AUTO_MERGE_NAME_SIMILARITY &&
 * sharedEntityId` branch below, and it is the same constant in both places
 * rather than the same number: `safe` IS that one branch, so the safe set is a
 * subset of the detected set by construction, and two literals would let
 * somebody raise the branch above the verdict — which would mark a pair safe
 * that only a weaker branch ever detected.
 */
const AUTO_MERGE_NAME_SIMILARITY = 0.95;

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
  /**
   * Whether this pair carries enough evidence to be merged without a human
   * looking at it. `POST merge-duplicate-parks` with `autoDetect: true` merges
   * the safe ones and nothing else.
   *
   * Two conditions, and the attraction side uses both in its own form
   * (`isSafeToAutoMerge`): positive evidence from an upstream source, and
   * names that agree. Here the evidence is a shared entity **value** — one
   * external park cannot be two parks, which is why this branch exists at all
   * and why both real production duplicates carried one. Names still have to
   * agree, because a shared id can also be a mis-assignment, and a mis-assigned
   * id plus a name nobody checked is how a real park gets deleted.
   *
   * Everything else is `false`: a name against a city, a name against a
   * geocode, and `sharedPoint`. Those are heuristics measured against the
   * catalogue as it stands today, and the catalogue is upstream's to change.
   *
   * `sharedPoint` is out **structurally rather than by a special case**: it
   * requires `sourcesDisjoint`, which is false as soon as both rows carry an
   * id from the same source — and an equal value means exactly that. So
   * `sharedPoint` implies `!sharedEntityId` implies `!safe`, and there is no
   * second rule here to drift out of step with that one.
   */
  safe: boolean;
  /** Why a human has to look, or null when nobody has to. */
  reviewReason: string | null;
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
   *
   * **A returned pair is a report, not an instruction.** Which of them may be
   * merged with nobody watching is `safe`, and a caller that deletes a row
   * reads that field — see `DuplicatePair.safe`.
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
        // for one. PortAventura Park and Ferrari Land are the shape it is for,
        // both carried by Queue-Times (19 and 277) on one resort geocode,
        // though at today's floor it is not what refuses them: their names
        // score 0.2000 against 0.65, so the floor already does. Disabling this
        // whole condition leaves the PortAventura case green and five others
        // red. It is the guard that matters if the floor is ever lowered, and
        // the one doing the work wherever a name clears it.
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
          (nameSimilarity >= AUTO_MERGE_NAME_SIMILARITY && sharedEntityId);

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

          const safe =
            sharedEntityId && nameSimilarity >= AUTO_MERGE_NAME_SIMILARITY;
          const reviewReason = safe
            ? null
            : !sharedEntityId
              ? "no upstream source holds one id for both rows — this pair rests on names and geometry alone"
              : `names score ${nameSimilarity.toFixed(4)} against ${AUTO_MERGE_NAME_SIMILARITY} — "${p1.name}" vs "${p2.name}", so the shared id alone decides it`;

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
            safe,
            reviewReason,
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
