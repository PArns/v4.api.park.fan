import {
  BadRequestException,
  ConflictException,
  NotFoundException,
} from "@nestjs/common";
import type { DataSource, Repository } from "typeorm";
import type { Park } from "../parks/entities/park.entity";
import type { ParkFacilityImport } from "./entities/park-facility-import.entity";
import type { ParkFacilityOsm } from "./entities/park-facility-osm.entity";
import { FacilitiesService, parseBoundaryRef } from "./facilities.service";
import { OSM_ATTRIBUTION } from "./facility.types";
import type { OverpassClient } from "./overpass.client";
import type { OverpassElement, OverpassResponse } from "./overpass.parser";

const PARK = "11111111-1111-4111-8111-111111111111";

const toilet = (
  id: number,
  tags: Record<string, string> = {},
): OverpassElement => ({
  type: "node",
  id,
  lat: 50.8 + id / 1000,
  lon: 6.88,
  tags: { amenity: "toilets", ...tags },
  version: 1,
  timestamp: "2024-01-01T00:00:00Z",
});

const outline: OverpassElement = {
  type: "way",
  id: 31052062,
  tags: { tourism: "theme_park", name: "Phantasialand" },
  bounds: { minlat: 50.79, minlon: 6.87, maxlat: 50.81, maxlon: 6.89 },
};

/** The service over two arrays instead of two tables. */
function setup(
  park: Partial<Park> | null = { latitude: 50.8, longitude: 6.88 },
) {
  const rows: ParkFacilityOsm[] = [];
  let state: ParkFacilityImport | null = null;
  let nextId = 0;
  const queries: string[] = [];
  let facilitiesAnswer: OverpassElement[] = [];

  const overpass = {
    query: jest.fn(async (ql: string): Promise<OverpassResponse> => {
      queries.push(ql);
      return ql.includes("map_to_area")
        ? {
            osm3s: { timestamp_osm_base: "2026-11-25T13:41:51Z" },
            elements: facilitiesAnswer,
          }
        : { elements: [outline] };
    }),
  };
  const manager = {
    delete: async (_: unknown, ids: string[]) => {
      for (const id of ids) {
        rows.splice(
          rows.findIndex((r) => r.id === id),
          1,
        );
      }
    },
    update: async (_: unknown, id: string, patch: Partial<ParkFacilityOsm>) => {
      Object.assign(
        rows.find((r) => r.id === id)!,
        patch,
      );
    },
    insert: async (_: unknown, values: Partial<ParkFacilityOsm>[]) => {
      for (const value of values) {
        rows.push({
          hidden: false,
          ...value,
          id: `f${nextId++}`,
        } as ParkFacilityOsm);
      }
    },
    save: async (_: unknown, value: ParkFacilityImport) => {
      state = { ...value };
    },
  };
  const service = new FacilitiesService(
    {
      findOne: async () =>
        park && { id: PARK, name: "Phantasialand", curatedName: null, ...park },
    } as unknown as Repository<Park>,
    {
      find: async ({ where }: { where: Partial<ParkFacilityOsm> }) =>
        rows.filter(
          (r) => where.hidden === undefined || r.hidden === where.hidden,
        ),
      findOne: async ({ where }: { where: { id: string } }) =>
        rows.find((r) => r.id === where.id) ?? null,
      count: async () => rows.filter((r) => !r.hidden).length,
      save: async (row: ParkFacilityOsm) => row,
    } as unknown as Repository<ParkFacilityOsm>,
    {
      findOne: async () => state,
      save: async (row: ParkFacilityImport) => (state = row),
    } as unknown as Repository<ParkFacilityImport>,
    overpass as unknown as OverpassClient,
    {
      transaction: (run: (m: typeof manager) => Promise<void>) => run(manager),
    } as unknown as DataSource,
  );
  return {
    service,
    rows,
    queries,
    overpass,
    state: () => state,
    answer: (elements: OverpassElement[]) => (facilitiesAnswer = elements),
  };
}

describe("FacilitiesService", () => {
  const env = process.env.FACILITIES_ENABLED;
  beforeEach(() => {
    process.env.FACILITIES_ENABLED = "true";
  });
  afterAll(() => {
    if (env === undefined) delete process.env.FACILITIES_ENABLED;
    else process.env.FACILITIES_ENABLED = env;
  });

  describe("what is served", () => {
    async function approvedPark() {
      const ctx = setup();
      ctx.answer([toilet(1), toilet(2)]);
      await ctx.service.importPark(PARK);
      await ctx.service.setApproved(PARK, true, "admin@example.org");
      return ctx;
    }

    it("serves nothing while the flag is off, approved or not", async () => {
      const { service } = await approvedPark();
      for (const value of [undefined, "", "false", "1", "yes"]) {
        if (value === undefined) delete process.env.FACILITIES_ENABLED;
        else process.env.FACILITIES_ENABLED = value;
        expect(await service.publicFor(PARK)).toBeNull();
      }
    });

    it("serves no row of a park nobody approved", async () => {
      const { service, answer } = setup();
      expect(await service.publicFor(PARK)).toMatchObject({
        available: false,
        facilities: [],
      });
      answer([toilet(1)]);
      await service.importPark(PARK);
      expect(await service.publicFor(PARK)).toEqual({
        parkId: PARK,
        available: false,
        importedAt: null,
        facilities: [],
        attribution: OSM_ATTRIBUTION,
      });
    });

    it("serves an approved park with the notice and each point's OSM element", async () => {
      const { service } = await approvedPark();
      const answer = await service.publicFor(PARK);
      expect(answer).toMatchObject({
        available: true,
        attribution: {
          text: "© OpenStreetMap contributors",
          url: "https://www.openstreetmap.org/copyright",
          license: "ODbL-1.0",
        },
      });
      expect(answer!.facilities).toHaveLength(2);
      expect(answer!.facilities[0]).toMatchObject({
        type: "toilets",
        osm: { type: "node", id: "1", version: 1 },
      });
      expect(JSON.stringify(answer)).not.toContain("admin@example.org");
    });

    it("leaves a hidden point out", async () => {
      const { service, rows } = await approvedPark();
      await service.setHidden(PARK, rows[0].id, true);
      expect((await service.publicFor(PARK))!.facilities).toHaveLength(1);
    });

    it("refuses to approve a park with nothing to show", async () => {
      const { service, answer, rows } = setup();
      await service.importPark(PARK);
      await expect(
        service.setApproved(PARK, true, "admin@example.org"),
      ).rejects.toThrow(ConflictException);
      answer([toilet(1)]);
      await service.importPark(PARK);
      await service.setHidden(PARK, rows[0].id, true);
      await expect(
        service.setApproved(PARK, true, "admin@example.org"),
      ).rejects.toThrow(ConflictException);
    });

    it("stops serving once withdrawn", async () => {
      const { service } = await approvedPark();
      await service.setApproved(PARK, false, "admin@example.org");
      expect(await service.publicFor(PARK)).toMatchObject({ available: false });
    });

    it("stops serving when a re-import brings a point nobody saw", async () => {
      const { service, answer, state } = await approvedPark();
      answer([toilet(1), toilet(2), toilet(3)]);
      expect(await service.importPark(PARK)).toMatchObject({
        status: "imported",
        inserted: 1,
      });
      expect(state()).toMatchObject({ approvedAt: null, approvedBy: null });
      expect(await service.publicFor(PARK)).toMatchObject({
        available: false,
        facilities: [],
      });
    });

    it("stays approved through a removal and a bare version bump", async () => {
      const { service, answer } = await approvedPark();
      answer([{ ...toilet(1), version: 2 }]);
      expect(await service.importPark(PARK)).toMatchObject({
        status: "approved",
        updated: 1,
        removed: 1,
      });
      expect((await service.publicFor(PARK))!.facilities).toHaveLength(1);
    });
  });

  describe("importPark", () => {
    it("finds the outline by name, then cuts with it", async () => {
      const { service, answer, queries, state } = setup();
      answer([
        toilet(1, { wheelchair: "yes" }),
        toilet(2, { access: "private" }),
      ]);
      expect(await service.importPark(PARK)).toEqual({
        parkId: PARK,
        boundary: { type: "way", id: "31052062", name: "Phantasialand" },
        status: "imported",
        inserted: 1,
        updated: 0,
        removed: 0,
        unchanged: 0,
        byType: { toilets: 1 },
      });
      expect(queries).toHaveLength(2);
      expect(queries[1]).toContain("way(31052062);map_to_area");
      expect(state()).toMatchObject({
        status: "imported",
        boundaryOsmId: "31052062",
        osmBase: new Date("2026-11-25T13:41:51Z"),
      });
    });

    it("changes nothing the second time, and asks Overpass once", async () => {
      const { service, answer, rows, overpass } = setup();
      answer([toilet(1), toilet(2)]);
      await service.importPark(PARK);
      const before = JSON.stringify(rows);
      overpass.query.mockClear();
      expect(await service.importPark(PARK)).toMatchObject({
        inserted: 0,
        updated: 0,
        removed: 0,
        unchanged: 2,
      });
      expect(JSON.stringify(rows)).toBe(before);
      expect(overpass.query).toHaveBeenCalledTimes(1);
    });

    it("keeps a hidden flag across an update", async () => {
      const { service, answer, rows } = setup();
      answer([toilet(1)]);
      await service.importPark(PARK);
      await service.setHidden(PARK, rows[0].id, true);
      answer([{ ...toilet(1, { fee: "yes" }), version: 2 }]);
      await service.importPark(PARK);
      expect(rows[0]).toMatchObject({ hidden: true, fee: true, osmVersion: 2 });
    });

    it("refuses to empty a park on an empty answer", async () => {
      const { service, answer, rows } = setup();
      answer([toilet(1)]);
      await service.importPark(PARK);
      answer([]);
      await expect(service.importPark(PARK)).rejects.toThrow(ConflictException);
      expect(rows).toHaveLength(1);
    });

    it("uses the outline an admin names, without looking for one", async () => {
      const { service, answer, queries } = setup({ latitude: null as never });
      answer([toilet(1)]);
      await service.importPark(PARK, parseBoundaryRef("relation/42"));
      expect(queries).toHaveLength(1);
      expect(queries[0]).toContain("rel(42);map_to_area");
    });

    it("names what it found when no outline carries the park's name", async () => {
      const { service } = setup({
        name: "Toverland",
        // decimal columns arrive as strings
        latitude: "51.397" as never,
        longitude: "5.984" as never,
      });
      await expect(service.importPark(PARK)).rejects.toThrow(
        /way\/31052062 "Phantasialand"/,
      );
    });

    it("asks for an outline when the park has no coordinates", async () => {
      const { service } = setup({ latitude: null as never });
      await expect(service.importPark(PARK)).rejects.toThrow(
        BadRequestException,
      );
    });

    it("answers 404 for a park we do not have", async () => {
      await expect(setup(null).service.importPark(PARK)).rejects.toThrow(
        NotFoundException,
      );
    });
  });

  it("reads an outline reference strictly", () => {
    expect(parseBoundaryRef(" way/31052062 ")).toEqual({
      type: "way",
      id: "31052062",
    });
    for (const bad of ["node/1", "way/abc", "31052062", "way/1;rel(2)"]) {
      expect(() => parseBoundaryRef(bad)).toThrow(BadRequestException);
    }
  });
});
