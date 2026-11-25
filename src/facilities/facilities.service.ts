import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { DataSource, Repository } from "typeorm";
import { Park } from "../parks/entities/park.entity";
import { ParkFacilityImport } from "./entities/park-facility-import.entity";
import { ParkFacilityOsm } from "./entities/park-facility-osm.entity";
import { planFacilityMerge } from "./facility-merge";
import {
  facilitiesEnabled,
  OSM_ATTRIBUTION,
  type FacilityType,
  type OsmElementType,
} from "./facility.types";
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
  status: ParkFacilityImport["status"];
  inserted: number;
  updated: number;
  removed: number;
  unchanged: number;
  byType: Partial<Record<FacilityType, number>>;
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

/**
 * Facilities of a park from OpenStreetMap: imported per park when an admin
 * asks, served once an admin approved what came in. Nothing here runs on a
 * schedule or at boot. docs/frontend/facilities.md.
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
   * matches the park.
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
      : previous
        ? {
            type: previous.boundaryOsmType,
            id: previous.boundaryOsmId,
            name: previous.boundaryName,
          }
        : await this.findBoundary(park);

    const answer = await this.overpass.query(buildFacilitiesQuery(boundary));
    const incoming = parseFacilities(answer.elements ?? []);
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
        boundaryName: boundary.name,
        importedAt: fetchedAt,
        osmBase: Number.isNaN(base.getTime()) ? null : base,
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
      throw new BadRequestException(
        "The park has no coordinates; name the outline with ?boundary=way/<id>",
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
      throw new ConflictException(
        `No OpenStreetMap outline near the park carries its name. Found: ${seen || "none"}. ` +
          "Name one with ?boundary=way/<id> or relation/<id>.",
      );
    }
    return {
      type: picked.type,
      id: String(picked.id),
      name: picked.tags?.name ?? null,
    };
  }

  /** Everything the reviewer needs: the import's state and every row. */
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
    if (!row) throw new NotFoundException("This park has no import yet");
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

  /**
   * What a visitor gets: null while the feature is off (the route is a 404),
   * an empty unavailable answer until the park is approved, else its rows
   * that are not hidden.
   */
  async publicFor(parkId: string): Promise<PublicFacilities | null> {
    if (!facilitiesEnabled()) return null;
    const row = await this.imports.findOne({ where: { parkId } });
    const approved = row?.status === "approved";
    const rows = approved
      ? await this.facilities.find({
          where: { parkId, hidden: false },
          order: { type: "ASC", osmId: "ASC" },
        })
      : [];
    return {
      parkId,
      // Also false when the last visible point was hidden after approval.
      available: rows.length > 0,
      importedAt: rows.length > 0 ? row!.importedAt.toISOString() : null,
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
