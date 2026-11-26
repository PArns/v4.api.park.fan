import {
  BadGatewayException,
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, In, IsNull, Repository } from "typeorm";
import { Park } from "../parks/entities/park.entity";
import {
  ParkFacilityImport,
  type FacilityReviewStatus,
  type FacilityRunOutcome,
} from "./entities/park-facility-import.entity";
import { ParkFacilityOsm } from "./entities/park-facility-osm.entity";
import { planFacilityMerge } from "./facility-merge";
import {
  FACILITY_TYPES,
  facilitiesEnabled,
  OSM_ATTRIBUTION,
  type FacilityType,
  type OsmElementType,
} from "./facility.types";
import { insideRings, outlineRings } from "./outline.geometry";
import { OverpassClient } from "./overpass.client";
import {
  buildBoundaryQuery,
  buildFacilitiesQuery,
  parseFacilities,
  pickBoundary,
} from "./overpass.parser";

export interface OsmRef {
  type: OsmElementType;
  id: string;
}

export interface FacilityImportResult {
  parkId: string;
  boundary: OsmRef & { name: string | null };
  status: FacilityReviewStatus;
  inserted: number;
  updated: number;
  removed: number;
  unchanged: number;
  byType: Partial<Record<FacilityType, number>>;
}

/** How one park's import attempt ended, as the bulk run tallies it. */
export interface FacilityRunResult {
  parkId: string;
  outcome: FacilityRunOutcome;
  points: number;
  message: string | null;
  /** The failure was Overpass not answering, not something about this park. */
  overpassDown: boolean;
}

export interface PublicFacility {
  id: string;
  type: FacilityType;
  latitude: number;
  longitude: number;
  name: string | null;
  wheelchair: ParkFacilityOsm["wheelchair"];
  changingTable: boolean | null;
  fee: boolean | null;
  /** The OSM element, so anyone can look the point up or correct it there. */
  osm: OsmRef & { version: number; timestamp: string };
}

export interface PublicFacilities {
  parkId: string;
  /** False until an admin approved this park's import: show no layer at all. */
  available: boolean;
  importedAt: string | null;
  facilities: PublicFacility[];
  attribution: typeof OSM_ATTRIBUTION;
}

/** One park in the admin's list. */
export interface FacilityParkRow {
  parkId: string;
  name: string;
  country: string | null;
  /** `none`: no import was ever tried. */
  status: FacilityReviewStatus | "none";
  lastRun: FacilityRunOutcome | null;
  lastRunAt: string | null;
  lastRunMessage: string | null;
  boundary: (OsmRef & { name: string | null }) | null;
  importedAt: string | null;
  approvedAt: string | null;
  counts: {
    toilets: number;
    drinkingWater: number;
    hidden: number;
    /** Visible points whose position lies outside the outline. */
    outside: number;
  };
  /** Whether "approve all" would approve this park (`isBulkApprovable`). */
  bulkApprovable: boolean;
}

/** The fewest visible points a park needs for the bulk approval. */
export const BULK_APPROVE_MIN_POINTS = 3;

/**
 * The parks "approve all" takes without a person looking at each: imported
 * and waiting, the last run went through, at least three visible points, and
 * every one of them inside the outline. Everything else is reviewed by hand.
 */
export function isBulkApprovable(
  row: Pick<FacilityParkRow, "status" | "lastRun" | "counts">,
): boolean {
  const visible = row.counts.toilets + row.counts.drinkingWater;
  return (
    row.status === "imported" &&
    row.lastRun === "ok" &&
    visible >= BULK_APPROVE_MIN_POINTS &&
    row.counts.outside === 0
  );
}

/** `way/123` or `relation/123`, as openstreetmap.org writes it. */
export function parseBoundaryRef(value: string): OsmRef {
  const match = /^(way|relation)\/(\d{1,19})$/.exec(value.trim());
  if (!match) {
    throw new BadRequestException(
      "boundary must look like way/31052062 or relation/123",
    );
  }
  return { type: match[1] as OsmElementType, id: match[2] };
}

/** No outline near the park carries its name. Recorded, never guessed around. */
export class NoOutlineException extends ConflictException {}

/**
 * Facilities of a park from OpenStreetMap: imported when an admin asks (the
 * `facilities` queue runs it), served once an admin approved what came in.
 * Nothing here runs on a schedule or at boot. docs/frontend/facilities.md.
 */
@Injectable()
export class FacilitiesService {
  constructor(
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @InjectRepository(ParkFacilityOsm)
    private readonly facilities: Repository<ParkFacilityOsm>,
    @InjectRepository(ParkFacilityImport)
    private readonly imports: Repository<ParkFacilityImport>,
    private readonly overpass: OverpassClient,
    private readonly dataSource: DataSource,
  ) {}

  /**
   * Reads one park's facilities from Overpass and brings the table in line:
   * new elements in, changed ones updated, vanished ones out. The outline is
   * the one named, else the one the last import used, else the one whose name
   * matches the park. Throws without writing when anything is off.
   */
  async importPark(
    parkId: string,
    boundaryOverride?: OsmRef,
  ): Promise<FacilityImportResult> {
    const park = await this.parks.findOne({
      where: { id: parkId },
      select: {
        id: true,
        name: true,
        curatedName: true,
        latitude: true,
        longitude: true,
      },
    });
    if (!park) throw new NotFoundException("Park not found");
    const previous = await this.imports.findOne({ where: { parkId } });

    const boundary: OsmRef & { name: string | null } = boundaryOverride
      ? { ...boundaryOverride, name: null }
      : previous?.boundaryOsmType && previous.boundaryOsmId
        ? {
            type: previous.boundaryOsmType,
            id: previous.boundaryOsmId,
            name: previous.boundaryName,
          }
        : await this.findBoundary(park);

    const answer = await this.overpass.query(buildFacilitiesQuery(boundary));
    const elements = answer.elements ?? [];
    const outline = elements.find(
      (e) => e.type === boundary.type && String(e.id) === boundary.id,
    );
    const rings = outline ? outlineRings(outline) : [];
    // A wrong id, a deleted outline or one whose ways do not close.
    if (rings.length === 0) {
      throw new ConflictException(
        `${boundary.type}/${boundary.id} is not a closed outline in OpenStreetMap`,
      );
    }
    const incoming = parseFacilities(elements).map((facility) => ({
      ...facility,
      insideOutline: insideRings(facility.longitude, facility.latitude, rings),
    }));
    const existing = await this.facilities.find({ where: { parkId } });
    // An outline Overpass has no area for answers with nothing, and so does a
    // retagged park. Neither is a reason to delete what a person approved.
    if (incoming.length === 0 && existing.length > 0) {
      throw new ConflictException(
        `Overpass returned no facilities for ${boundary.type}/${boundary.id}, ` +
          `and this park has ${existing.length}. Nothing was changed; check the outline.`,
      );
    }

    const plan = planFacilityMerge(existing, incoming);
    const fetchedAt = new Date();
    // Only what a visitor would newly see asks for another look. A removal or
    // a bare version bump shows nothing nobody approved.
    const needsReview =
      plan.insert.length > 0 || plan.update.some((u) => u.visible);
    const status =
      previous?.status === "approved" && !needsReview ? "approved" : "imported";

    await this.dataSource.transaction(async (manager) => {
      if (plan.removeIds.length > 0) {
        await manager.delete(ParkFacilityOsm, plan.removeIds);
      }
      for (const { id, facility } of plan.update) {
        await manager.update(ParkFacilityOsm, id, { ...facility, fetchedAt });
      }
      if (plan.insert.length > 0) {
        await manager.insert(
          ParkFacilityOsm,
          plan.insert.map((facility) => ({ ...facility, parkId, fetchedAt })),
        );
      }
      const base = new Date(answer.osm3s?.timestamp_osm_base ?? NaN);
      await manager.save(ParkFacilityImport, {
        parkId,
        status,
        boundaryOsmType: boundary.type,
        boundaryOsmId: boundary.id,
        boundaryName: boundary.name ?? outline?.tags?.name ?? null,
        boundaryRings: rings,
        importedAt: fetchedAt,
        osmBase: Number.isNaN(base.getTime()) ? null : base,
        lastRun: "ok",
        lastRunAt: fetchedAt,
        lastRunMessage: null,
        approvedAt: status === "approved" ? previous!.approvedAt : null,
        approvedBy: status === "approved" ? previous!.approvedBy : null,
      });
    });

    const byType: FacilityImportResult["byType"] = {};
    for (const f of incoming) byType[f.type] = (byType[f.type] ?? 0) + 1;
    return {
      parkId,
      boundary,
      status,
      inserted: plan.insert.length,
      updated: plan.update.length,
      removed: plan.removeIds.length,
      unchanged: plan.unchanged,
      byType,
    };
  }

  /**
   * `importPark`, with the way it ended written to the park's row: what the
   * queue calls. A failure keeps the rows and the review state as they were
   * and leaves its reason; it never throws for one park's trouble.
   */
  async runImport(
    parkId: string,
    boundaryOverride?: OsmRef,
  ): Promise<FacilityRunResult> {
    try {
      const result = await this.importPark(parkId, boundaryOverride);
      const points = Object.values(result.byType).reduce((a, b) => a + b, 0);
      return {
        parkId,
        outcome: "ok",
        points,
        message: null,
        overpassDown: false,
      };
    } catch (error) {
      if (error instanceof NotFoundException) throw error;
      const outcome =
        error instanceof NoOutlineException ? "no_outline" : "failed";
      const message = error instanceof Error ? error.message : String(error);
      const row =
        (await this.imports.findOne({ where: { parkId } })) ??
        this.imports.create({ parkId, status: "pending" });
      row.lastRun = outcome;
      row.lastRunAt = new Date();
      row.lastRunMessage = message;
      await this.imports.save(row);
      return {
        parkId,
        outcome,
        points: 0,
        message,
        overpassDown: error instanceof BadGatewayException,
      };
    }
  }

  /**
   * The parks a bulk run visits, in id order so a resumed run knows what is
   * behind it: every park that has not closed for good, and unless `all`,
   * only those whose last attempt did not go through.
   */
  async parkIdsForBulkRun(all: boolean): Promise<string[]> {
    const parks = await this.parks.find({
      where: { retiredAt: IsNull() },
      select: { id: true },
      order: { id: "ASC" },
    });
    if (all) return parks.map((p) => p.id);
    const done = new Set(
      (await this.imports.find({ where: { lastRun: "ok" } })).map(
        (row) => row.parkId,
      ),
    );
    return parks.map((p) => p.id).filter((id) => !done.has(id));
  }

  private async findBoundary(
    park: Pick<Park, "name" | "curatedName" | "latitude" | "longitude">,
  ): Promise<OsmRef & { name: string | null }> {
    const lat = Number(park.latitude);
    const lon = Number(park.longitude);
    if (
      park.latitude == null ||
      !Number.isFinite(lat) ||
      !Number.isFinite(lon)
    ) {
      throw new NoOutlineException(
        "The park has no coordinates; name the outline (way/<id>)",
      );
    }
    const { elements = [] } = await this.overpass.query(
      buildBoundaryQuery(lat, lon),
    );
    const picked = pickBoundary(elements, [park.name, park.curatedName]);
    if (!picked) {
      const seen = elements
        .map((e) => `${e.type}/${e.id} "${e.tags?.name ?? "?"}"`)
        .join(", ");
      throw new NoOutlineException(
        `No OpenStreetMap outline near the park carries its name. Found: ${seen || "none"}.`,
      );
    }
    return {
      type: picked.type,
      id: String(picked.id),
      name: picked.tags?.name ?? null,
    };
  }

  /** Every park that has not closed for good, with its import state and counts. */
  async listParks(): Promise<FacilityParkRow[]> {
    const [parks, imports, facilities] = await Promise.all([
      this.parks.find({
        where: { retiredAt: IsNull() },
        select: { id: true, name: true, curatedName: true, country: true },
        order: { name: "ASC" },
      }),
      this.imports.find(),
      // A few thousand narrow rows; counted here so no SQL has to be right.
      this.facilities.find({
        select: {
          parkId: true,
          type: true,
          hidden: true,
          insideOutline: true,
        },
      }),
    ]);
    const stateOf = new Map(imports.map((row) => [row.parkId, row]));
    const counts = new Map<string, FacilityParkRow["counts"]>();
    for (const f of facilities) {
      const c = counts.get(f.parkId) ?? {
        toilets: 0,
        drinkingWater: 0,
        hidden: 0,
        outside: 0,
      };
      if (f.hidden) c.hidden += 1;
      else {
        if (f.type === "toilets") c.toilets += 1;
        else c.drinkingWater += 1;
        if (!f.insideOutline) c.outside += 1;
      }
      counts.set(f.parkId, c);
    }
    return parks.map((park) => {
      const state = stateOf.get(park.id);
      const row = {
        parkId: park.id,
        name: park.curatedName ?? park.name,
        country: park.country ?? null,
        status: state?.status ?? ("none" as const),
        lastRun: state?.lastRun ?? null,
        lastRunAt: state?.lastRunAt.toISOString() ?? null,
        lastRunMessage: state?.lastRunMessage ?? null,
        boundary:
          state?.boundaryOsmType && state.boundaryOsmId
            ? {
                type: state.boundaryOsmType,
                id: state.boundaryOsmId,
                name: state.boundaryName,
              }
            : null,
        importedAt: state?.importedAt?.toISOString() ?? null,
        approvedAt: state?.approvedAt?.toISOString() ?? null,
        counts: counts.get(park.id) ?? {
          toilets: 0,
          drinkingWater: 0,
          hidden: 0,
          outside: 0,
        },
      };
      return { ...row, bulkApprovable: isBulkApprovable(row) };
    });
  }

  /**
   * Approves every park `isBulkApprovable` names. With `dryRun` it changes
   * nothing and answers with the parks it would take.
   */
  async approveAll(
    actor: string,
    dryRun: boolean,
  ): Promise<{ dryRun: boolean; count: number; parks: string[] }> {
    const take = (await this.listParks()).filter((row) => row.bulkApprovable);
    if (!dryRun && take.length > 0) {
      await this.imports.update(
        { parkId: In(take.map((row) => row.parkId)), status: "imported" },
        { status: "approved", approvedAt: new Date(), approvedBy: actor },
      );
    }
    return { dryRun, count: take.length, parks: take.map((row) => row.name) };
  }

  /** Everything the reviewer needs: the import's state, outline and every row. */
  async review(parkId: string): Promise<{
    import: ParkFacilityImport | null;
    facilities: ParkFacilityOsm[];
    attribution: typeof OSM_ATTRIBUTION;
  }> {
    return {
      import: await this.imports.findOne({ where: { parkId } }),
      facilities: await this.facilities.find({
        where: { parkId },
        order: { type: "ASC", osmId: "ASC" },
      }),
      attribution: OSM_ATTRIBUTION,
    };
  }

  /**
   * `approved` serves the park's rows; anything else takes them offline. A
   * park without a visible row cannot be approved.
   */
  async setApproved(
    parkId: string,
    approved: boolean,
    actor: string,
  ): Promise<ParkFacilityImport> {
    const row = await this.imports.findOne({ where: { parkId } });
    if (!row || row.status === "pending") {
      throw new NotFoundException("This park has no import yet");
    }
    // An approved park with no rows would tell a visitor it has no toilets.
    if (
      approved &&
      (await this.facilities.count({ where: { parkId, hidden: false } })) === 0
    ) {
      throw new ConflictException("This park has no visible facility to serve");
    }
    row.status = approved ? "approved" : "imported";
    row.approvedAt = approved ? new Date() : null;
    row.approvedBy = approved ? actor : null;
    return this.imports.save(row);
  }

  async setHidden(
    parkId: string,
    facilityId: string,
    hidden: boolean,
  ): Promise<ParkFacilityOsm> {
    const row = await this.facilities.findOne({
      where: { id: facilityId, parkId },
    });
    if (!row) throw new NotFoundException("Facility not found in this park");
    row.hidden = hidden;
    return this.facilities.save(row);
  }

  /** The park of a visible facility, for a caller that holds only the id (Parky's navigate card). */
  async parkIdOf(facilityId: string): Promise<string | null> {
    if (!facilitiesEnabled()) return null;
    const row = await this.facilities.findOne({
      where: { id: facilityId, hidden: false },
      select: { id: true, parkId: true },
    });
    return row?.parkId ?? null;
  }

  /**
   * What a visitor gets: null while the feature is off (the route is a 404),
   * an empty unavailable answer until the park is approved, else its rows of
   * `FACILITY_TYPES` that are not hidden.
   */
  async publicFor(parkId: string): Promise<PublicFacilities | null> {
    if (!facilitiesEnabled()) return null;
    const row = await this.imports.findOne({ where: { parkId } });
    const rows =
      row?.status === "approved"
        ? await this.facilities.find({
            where: { parkId, hidden: false, type: In([...FACILITY_TYPES]) },
            order: { type: "ASC", osmId: "ASC" },
          })
        : [];
    return {
      parkId,
      // Also false when the last visible point was hidden after approval.
      available: rows.length > 0,
      importedAt:
        rows.length > 0 ? (row!.importedAt?.toISOString() ?? null) : null,
      facilities: rows.map((f) => ({
        id: f.id,
        type: f.type,
        latitude: f.latitude,
        longitude: f.longitude,
        name: f.name,
        wheelchair: f.wheelchair,
        changingTable: f.changingTable,
        fee: f.fee,
        osm: {
          type: f.osmType,
          id: f.osmId,
          version: f.osmVersion,
          timestamp: new Date(f.osmTimestamp).toISOString(),
        },
      })),
      attribution: OSM_ATTRIBUTION,
    };
  }
}
