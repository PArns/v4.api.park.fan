import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, Repository } from "typeorm";
import axios from "axios";
import {
  AttractionRideProfile,
  type RideMeasurements,
} from "../../attractions/entities/attraction-ride-profile.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { getGlossaryTermIdsUrl } from "../../config/glossary.config";

export interface RideProfileInput {
  elements?: string[];
  types?: string[];
  manufacturerName?: string | null;
  manufacturerTermId?: string | null;
  model?: string | null;
  openedYear?: number | null;
  inversions?: number | null;
  curatedStats?: Partial<RideMeasurements> | null;
}

/** How long a fetched glossary id list is trusted. */
const TERM_ID_TTL_MS = 10 * 60 * 1000;

const MEASUREMENT_BOUNDS: Record<keyof RideMeasurements, [number, number]> = {
  topSpeedKmh: [0, 400],
  heightM: [0, 250],
  lengthM: [0, 10_000],
  durationSeconds: [0, 3600],
};

/**
 * The write side of `attraction_ride_profiles`, which had no write side.
 *
 * Until now these rows were edited with hand-written SQL against production,
 * matched on `parks.slug` AND `attractions.slug` together (park slugs are not
 * globally unique), with `seeded_at = now()` remembered by the person typing
 * it. Forgetting that one column meant the correction was written, was correct,
 * and stayed invisible: `findCuratedSince` uses `seeded_at` as the sole marker
 * for which rows to evict caches for, so an unstamped edit surfaced only as
 * TTLs expired — up to 6 h in Redis, 900 s at the edge, a day in the frontend's
 * data cache.
 *
 * Two things move into code here. The stamp, so it cannot be forgotten. And the
 * term-id check, which used to happen the next morning in a nightly audit: the
 * ids are frontend glossary ids that nothing in this database validates, and a
 * wrong one does not error — `GlossaryTermLink` drops an id it cannot resolve,
 * so the ride's layout walkthrough just comes out shorter. Checking at write
 * time turns that into a message while the editor still remembers what they
 * meant.
 */
@Injectable()
export class AdminRideProfileService {
  private readonly logger = new Logger(AdminRideProfileService.name);

  private termIds: Set<string> | null = null;
  private termIdsFetchedAt = 0;

  constructor(
    @InjectRepository(AttractionRideProfile)
    private readonly profiles: Repository<AttractionRideProfile>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
  ) {}

  async find(attractionId: string): Promise<AttractionRideProfile | null> {
    return this.profiles.findOne({ where: { attractionId } });
  }

  /**
   * Which of these attractions have a profile at all.
   *
   * One query rather than one per ride: the park list asks this for every
   * attraction it shows, and Europa-Park has about a hundred — a hundred
   * concurrent single-row lookups was comfortably the slowest thing the admin
   * did, and it got slower with the park.
   */
  async findIdsWithProfile(attractionIds: string[]): Promise<Set<string>> {
    if (attractionIds.length === 0) return new Set();
    const rows = await this.profiles.find({
      where: { attractionId: In(attractionIds) },
      select: { attractionId: true },
    });
    return new Set(rows.map((row) => row.attractionId));
  }

  async upsert(
    attractionId: string,
    input: RideProfileInput,
  ): Promise<{ profile: AttractionRideProfile; unknownTermIds: string[] }> {
    const attraction = await this.attractions.findOne({
      where: { id: attractionId },
    });
    if (!attraction)
      throw new NotFoundException(`No attraction ${attractionId}`);

    const elements = this.cleanTermList(input.elements, "elements");
    const types = this.cleanTermList(input.types, "types");
    const manufacturerTermId = input.manufacturerTermId?.trim() || null;

    const unknownTermIds = await this.checkTermIds([
      ...elements,
      ...types,
      ...(manufacturerTermId ? [manufacturerTermId] : []),
    ]);
    if (unknownTermIds.length > 0) {
      throw new BadRequestException(
        `The glossary does not define these term ids, so they would render as ` +
          `nothing: ${unknownTermIds.join(", ")}`,
      );
    }

    const existing = await this.profiles.findOne({ where: { attractionId } });
    const profile =
      existing ??
      this.profiles.create({ attractionId, parkId: attraction.parkId });

    profile.elements = elements;
    profile.types = types;
    profile.manufacturerName = input.manufacturerName?.trim() || null;
    profile.manufacturerTermId = manufacturerTermId;
    profile.model = input.model?.trim() || null;
    profile.openedYear = input.openedYear ?? null;
    profile.inversions = input.inversions ?? null;
    profile.curatedStats = this.cleanMeasurements(input.curatedStats);
    // The stamp `findCuratedSince` reads. Set on every write, without
    // exception — an unstamped correction is an invisible one.
    profile.seededAt = new Date();

    const saved = await this.profiles.save(profile);
    return { profile: saved, unknownTermIds: [] };
  }

  async remove(attractionId: string): Promise<boolean> {
    const result = await this.profiles.delete({ attractionId });
    return (result.affected ?? 0) > 0;
  }

  /**
   * Term ids the glossary defines, fetched from the frontend and briefly cached.
   *
   * Returns an empty list of complaints when the frontend cannot be reached.
   * That is deliberate: an unreachable frontend is not evidence that the ids
   * are wrong, and refusing every curation because a deploy is mid-flight would
   * make this check the thing that breaks curation rather than the thing that
   * protects it. The nightly audit still catches whatever slips through.
   */
  private async checkTermIds(ids: string[]): Promise<string[]> {
    if (ids.length === 0) return [];

    const known = await this.loadTermIds();
    if (!known) return [];

    return [...new Set(ids)].filter((id) => !known.has(id));
  }

  private async loadTermIds(): Promise<Set<string> | null> {
    if (this.termIds && Date.now() - this.termIdsFetchedAt < TERM_ID_TTL_MS) {
      return this.termIds;
    }
    try {
      const { data } = await axios.get<{ count: number; ids: string[] }>(
        getGlossaryTermIdsUrl(),
        { timeout: 8000 },
      );
      // An empty list is a broken source, never "every id is wrong" — the same
      // rule the nightly audit follows.
      if (!Array.isArray(data?.ids) || data.ids.length === 0) return null;
      this.termIds = new Set(data.ids);
      this.termIdsFetchedAt = Date.now();
      return this.termIds;
    } catch (error) {
      this.logger.warn(
        `Glossary term ids unavailable, skipping the write-time check: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      return null;
    }
  }

  /**
   * Clean a term list without changing what it says.
   *
   * Blank entries go, whitespace goes — and that is all. `elements` is the
   * ride's layout in ride order, so it is NOT sorted and NOT deduplicated: a
   * layout that takes the same figure twice says so twice, and sorting it would
   * describe a ride nobody has ever ridden.
   */
  private cleanTermList(value: string[] | undefined, field: string): string[] {
    if (value === undefined) return [];
    if (!Array.isArray(value)) {
      throw new BadRequestException(
        `${field} must be a list of glossary term ids`,
      );
    }
    return value
      .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
      .filter((entry) => entry.length > 0);
  }

  private cleanMeasurements(
    input: Partial<RideMeasurements> | null | undefined,
  ): RideMeasurements | null {
    if (!input) return null;

    const output: RideMeasurements = {
      topSpeedKmh: null,
      heightM: null,
      lengthM: null,
      durationSeconds: null,
    };
    let any = false;

    for (const key of Object.keys(output) as Array<keyof RideMeasurements>) {
      const raw = input[key];
      if (raw === null || raw === undefined || raw === ("" as unknown))
        continue;
      const value = Number(raw);
      const [min, max] = MEASUREMENT_BOUNDS[key];
      if (!Number.isFinite(value) || value < min || value > max) {
        throw new BadRequestException(
          `${key} must be a number between ${min} and ${max}`,
        );
      }
      output[key] = value;
      any = true;
    }

    // All-null is the same as no curated measurements at all, and storing an
    // object of nulls would make `mergeRideStats` report source "mixed" for a
    // ride nobody has measured by hand.
    return any ? output : null;
  }
}
