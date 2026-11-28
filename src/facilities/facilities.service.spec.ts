import {
  BadGatewayException,
  BadRequestException,
  ConflictException,
  NotFoundException,
} from "@nestjs/common";
import type { DataSource, Repository } from "typeorm";
import type { Park } from "../parks/entities/park.entity";
import type { ParkFacilityImport } from "./entities/park-facility-import.entity";
import type { ParkFacilityOsm } from "./entities/park-facility-osm.entity";
import {
  FacilitiesService,
  isBulkApprovable,
  parseBoundaryRef,
} from "./facilities.service";
import { FACILITY_TYPES, OSM_ATTRIBUTION } from "./facility.types";
import type { OverpassClient } from "./overpass.client";
import type { OverpassElement, OverpassResponse } from "./overpass.parser";

const PARK = "11111111-1111-4111-8111-111111111111";
const OTHER = "22222222-2222-4222-8222-222222222222";

/** Inside the outline below unless `lat` says otherwise. */
const toilet = (
  id: number,
  tags: Record<string, string> = {},
  lat = 50.8 + id / 1000,
): OverpassElement => ({
  type: "node",
  id,
  lat,
  lon: 6.88,
  tags: { amenity: "toilets", ...tags },
  version: 1,
  timestamp: "2024-01-01T00:00:00Z",
});

const square = [
  { lat: 50.79, lon: 6.87 },
  { lat: 50.79, lon: 6.89 },
  { lat: 50.81, lon: 6.89 },
  { lat: 50.81, lon: 6.87 },
  { lat: 50.79, lon: 6.87 },
];
const outline: OverpassElement = {
  type: "way",
  id: 31052062,
  tags: { tourism: "theme_park", name: "Phantasialand" },
  bounds: { minlat: 50.79, minlon: 6.87, maxlat: 50.81, maxlon: 6.89 },
  geometry: square,
};

type ParkSeed = Partial<Park> & { id: string };

/** The service over arrays instead of tables. */
function setup(
  parks: ParkSeed[] = [
    { id: PARK, name: "Phantasialand", latitude: 50.8, longitude: 6.88 },
  ],
) {
  const rows: ParkFacilityOsm[] = [];
  const states = new Map<string, ParkFacilityImport>();
  let nextId = 0;
  const queries: string[] = [];
  let facilitiesAnswer: OverpassElement[] = [];
  let outlineAnswer: OverpassElement[] = [outline];

  const overpass = {
    query: jest.fn(async (ql: string): Promise<OverpassResponse> => {
      queries.push(ql);
      if (!ql.includes("map_to_area")) return { elements: [outline] };
      const id = /(?:way|rel)\((\d+)\)/.exec(ql)![1];
      return {
        osm3s: { timestamp_osm_base: "2026-11-25T13:41:51Z" },
        elements: [
          ...outlineAnswer
            .filter((e) => String(e.id) === id || id === "42")
            .map((e) =>
              id === "42"
                ? {
                    ...e,
                    type: "relation" as const,
                    id: 42,
                    geometry: undefined,
                    members: [
                      { type: "way" as const, role: "outer", geometry: square },
                    ],
                  }
                : e,
            ),
          ...facilitiesAnswer,
        ],
      };
    }),
  };
  // A plain value, or a FindOperator In([...]).
  const fits = (value: unknown, wanted: unknown) =>
    wanted === undefined ||
    (typeof wanted === "object" && wanted !== null && "value" in wanted
      ? (wanted as { value: unknown[] }).value.includes(value)
      : value === wanted);
  const matches = (row: ParkFacilityOsm, where: Record<string, unknown>) =>
    fits(row.parkId, where.parkId) &&
    fits(row.id, where.id) &&
    fits(row.hidden, where.hidden) &&
    fits(row.awaitingReview, where.awaitingReview) &&
    fits(row.type, where.type);
  const manager = {
    delete: async (_: unknown, ids: string[]) => {
      for (const id of ids) {
        rows.splice(
          rows.findIndex((r) => r.id === id),
          1,
        );
      }
    },
    update: async (
      entity: { name: string },
      where: string | Record<string, unknown>,
      patch: Record<string, unknown>,
    ) => {
      if (entity.name === "ParkFacilityImport") {
        for (const state of states.values()) {
          const w = where as Record<string, unknown>;
          if (fits(state.parkId, w.parkId) && fits(state.status, w.status)) {
            Object.assign(state, patch);
          }
        }
        return;
      }
      for (const row of rows) {
        if (
          typeof where === "string" ? row.id === where : matches(row, where)
        ) {
          Object.assign(row, patch);
        }
      }
    },
    insert: async (_: unknown, values: Partial<ParkFacilityOsm>[]) => {
      for (const value of values) {
        rows.push({
          hidden: false,
          awaitingReview: false,
          ...value,
          id: `f${nextId++}`,
        } as ParkFacilityOsm);
      }
    },
    save: async (_: unknown, value: ParkFacilityImport) => {
      states.set(value.parkId, { ...value });
      return value;
    },
  };
  const service = new FacilitiesService(
    {
      findOne: async ({ where }: { where: { id: string } }) =>
        parks.find((p) => p.id === where.id) ?? null,
      find: async () =>
        parks
          .filter((p) => !p.retiredAt)
          .sort((a, b) => a.id.localeCompare(b.id)),
    } as unknown as Repository<Park>,
    {
      find: async ({ where = {} }: { where?: Record<string, unknown> } = {}) =>
        rows.filter((r) => matches(r, where)),
      findOne: async ({ where }: { where: Record<string, unknown> }) =>
        rows.find((r) => matches(r, where)) ?? null,
      count: async ({ where }: { where: Record<string, unknown> }) =>
        rows.filter((r) => matches(r, where)).length,
      save: async (row: ParkFacilityOsm) => row,
    } as unknown as Repository<ParkFacilityOsm>,
    {
      findOne: async ({ where }: { where: { parkId: string } }) =>
        states.get(where.parkId) ?? null,
      find: async ({ where }: { where?: { lastRun?: string } } = {}) =>
        [...states.values()].filter(
          (s) => !where?.lastRun || s.lastRun === where.lastRun,
        ),
      create: (value: Partial<ParkFacilityImport>) =>
        ({
          boundaryOsmType: null,
          boundaryOsmId: null,
          boundaryName: null,
          boundaryRings: null,
          importedAt: null,
          osmBase: null,
          approvedAt: null,
          approvedBy: null,
          ...value,
        }) as ParkFacilityImport,
      save: async (row: ParkFacilityImport) => {
        states.set(row.parkId, row);
        return row;
      },
      update: async (
        where: { parkId: { value: string[] }; status: string },
        patch: Partial<ParkFacilityImport>,
      ) => {
        for (const id of where.parkId.value) {
          const row = states.get(id);
          if (row?.status === where.status) Object.assign(row, patch);
        }
      },
    } as unknown as Repository<ParkFacilityImport>,
    overpass as unknown as OverpassClient,
    {
      transaction: (run: (m: typeof manager) => Promise<unknown>) =>
        run(manager),
    } as unknown as DataSource,
  );
  return {
    service,
    rows,
    queries,
    overpass,
    state: (id = PARK) => states.get(id) ?? null,
    answer: (elements: OverpassElement[]) => (facilitiesAnswer = elements),
    outlines: (elements: OverpassElement[]) => (outlineAnswer = elements),
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
      expect(JSON.stringify(answer)).not.toContain("insideOutline");
    });

    it("serves toilets, drinking water and sights and no other type, whatever the table holds", async () => {
      expect(FACILITY_TYPES).toEqual(["toilets", "drinking_water", "sight"]);
      const { service, rows, answer } = setup();
      answer([
        toilet(1, { wheelchair: "yes", changing_table: "yes" }),
        toilet(2, { amenity: "drinking_water" }),
        toilet(3, { amenity: "atm" }),
        toilet(4, { amenity: "waste_basket", name: "Opa Gijs" }),
      ]);
      await service.importPark(PARK);
      // A row of a type the list no longer names, as an older import left it.
      rows.push({ ...rows[0], id: "old", osmId: "9", type: "atm" as never });
      await service.setApproved(PARK, true, "admin@example.org");
      const served = (await service.publicFor(PARK))!.facilities;
      expect(served.map((f) => f.type).sort()).toEqual([
        "drinking_water",
        "sight",
        "toilets",
      ]);
      expect(served.find((f) => f.type === "sight")).toMatchObject({
        name: "Opa Gijs",
      });
      expect(served.find((f) => f.type === "toilets")).toMatchObject({
        wheelchair: "yes",
        changingTable: true,
      });
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

    it("keeps serving what was approved while a new point waits for review", async () => {
      const { service, answer, state, rows } = await approvedPark();
      answer([
        toilet(1),
        toilet(2),
        toilet(3, { amenity: "waste_basket", name: "Opa Gijs" }),
      ]);
      expect(await service.importPark(PARK)).toMatchObject({
        status: "imported",
        inserted: 1,
      });
      expect(state()).toMatchObject({ approvedBy: "admin@example.org" });
      expect(rows.map((r) => r.awaitingReview)).toEqual([false, false, true]);
      const served = await service.publicFor(PARK);
      expect(served!.available).toBe(true);
      expect(served!.facilities.map((f) => f.osm.id)).toEqual(["1", "2"]);

      await service.setApproved(PARK, true, "admin@example.org");
      expect(rows.map((r) => r.awaitingReview)).toEqual([false, false, false]);
      expect((await service.publicFor(PARK))!.facilities).toHaveLength(3);
    });

    it("holds back a point whose shown fields changed until it is approved again", async () => {
      const { service, answer } = await approvedPark();
      answer([toilet(1, { wheelchair: "yes" }), toilet(2)]);
      expect(await service.importPark(PARK)).toMatchObject({
        status: "imported",
        updated: 1,
      });
      expect(
        (await service.publicFor(PARK))!.facilities.map((f) => f.osm.id),
      ).toEqual(["2"]);
    });

    it("serves nothing of a withdrawn park, even after a re-import", async () => {
      const { service, answer } = await approvedPark();
      await service.setApproved(PARK, false, "admin@example.org");
      answer([toilet(1), toilet(2), toilet(3)]);
      await service.importPark(PARK);
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
    it("finds the outline by name, cuts with it and keeps its rings", async () => {
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
      expect(queries[1]).toContain("way(31052062)->.b;");
      expect(state()).toMatchObject({
        status: "imported",
        lastRun: "ok",
        lastRunMessage: null,
        boundaryOsmId: "31052062",
        osmBase: new Date("2026-11-25T13:41:51Z"),
      });
      expect(state()!.boundaryRings![0]).toHaveLength(5);
    });

    it("marks a point whose position lies outside the outline", async () => {
      const { service, answer, rows } = setup();
      answer([toilet(1), toilet(2, {}, 50.85)]);
      await service.importPark(PARK);
      expect(rows.map((r) => r.insideOutline)).toEqual([true, false]);
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

    it("refuses an outline that does not exist or does not close", async () => {
      const { service, answer, outlines, rows } = setup();
      answer([toilet(1)]);
      outlines([]);
      await expect(service.importPark(PARK)).rejects.toThrow(
        /way\/31052062 is not a closed outline/,
      );
      outlines([{ ...outline, geometry: square.slice(0, 3) }]);
      await expect(service.importPark(PARK)).rejects.toThrow(ConflictException);
      expect(rows).toEqual([]);
    });

    it("uses the outline an admin names, a relation too, without looking for one", async () => {
      const { service, answer, queries, state } = setup([
        { id: PARK, name: "Phantasialand", latitude: null as never },
      ]);
      answer([toilet(1)]);
      await service.importPark(PARK, parseBoundaryRef("relation/42"));
      expect(queries).toHaveLength(1);
      expect(queries[0]).toContain("rel(42)->.b;");
      expect(state()).toMatchObject({
        boundaryOsmType: "relation",
        boundaryOsmId: "42",
        boundaryName: "Phantasialand",
      });
    });

    it("answers 404 for a park we do not have", async () => {
      await expect(setup([]).service.importPark(PARK)).rejects.toThrow(
        NotFoundException,
      );
    });
  });

  describe("runImport, the state a run leaves", () => {
    it("records the points of a run that went through", async () => {
      const { service, answer, state } = setup();
      answer([toilet(1), toilet(2, { amenity: "drinking_water" })]);
      expect(await service.runImport(PARK)).toEqual({
        parkId: PARK,
        outcome: "ok",
        points: 2,
        message: null,
        overpassDown: false,
      });
      expect(state()).toMatchObject({ status: "imported", lastRun: "ok" });
    });

    it("lists a park without a named outline as such, with what was found", async () => {
      const { service, state, rows } = setup([
        // decimal columns arrive as strings
        {
          id: PARK,
          name: "Toverland",
          latitude: "51.397" as never,
          longitude: "5.984" as never,
        },
      ]);
      const result = await service.runImport(PARK);
      expect(result).toMatchObject({
        outcome: "no_outline",
        overpassDown: false,
      });
      expect(result.message).toMatch(/way\/31052062 "Phantasialand"/);
      expect(state()).toMatchObject({
        status: "pending",
        lastRun: "no_outline",
        boundaryOsmId: null,
      });
      expect(rows).toEqual([]);
    });

    it("lists a park without coordinates as having no outline", async () => {
      const { service } = setup([
        { id: PARK, name: "Phantasialand", latitude: null as never },
      ]);
      expect((await service.runImport(PARK)).outcome).toBe("no_outline");
    });

    it("keeps an approved park approved and its rows when a later run fails", async () => {
      const { service, answer, overpass, state, rows } = setup();
      answer([toilet(1)]);
      await service.runImport(PARK);
      await service.setApproved(PARK, true, "admin@example.org");
      overpass.query.mockRejectedValueOnce(
        new BadGatewayException("Overpass did not answer: 504"),
      );
      expect(await service.runImport(PARK)).toMatchObject({
        outcome: "failed",
        overpassDown: true,
        message: "Overpass did not answer: 504",
      });
      expect(state()).toMatchObject({
        status: "approved",
        lastRun: "failed",
        lastRunMessage: "Overpass did not answer: 504",
      });
      expect(rows).toHaveLength(1);
      expect((await service.publicFor(PARK))!.available).toBe(true);
    });

    it("cannot approve a park whose only run failed", async () => {
      const { service, overpass } = setup();
      overpass.query.mockRejectedValueOnce(new BadGatewayException("down"));
      await service.runImport(PARK);
      await expect(
        service.setApproved(PARK, true, "admin@example.org"),
      ).rejects.toThrow(NotFoundException);
    });
  });

  describe("the bulk run's parks", () => {
    const three: ParkSeed[] = [
      { id: OTHER, name: "Phantasialand", latitude: 50.8, longitude: 6.88 },
      { id: PARK, name: "Phantasialand", latitude: 50.8, longitude: 6.88 },
      {
        id: "33333333-3333-4333-8333-333333333333",
        name: "Closed",
        retiredAt: new Date(),
      },
    ];

    it("takes every open park in id order, and skips the ones already done", async () => {
      const { service, answer } = setup(three);
      expect(await service.parkIdsForBulkRun(false)).toEqual([PARK, OTHER]);
      answer([toilet(1)]);
      await service.runImport(PARK);
      expect(await service.parkIdsForBulkRun(false)).toEqual([OTHER]);
      expect(await service.parkIdsForBulkRun(true)).toEqual([PARK, OTHER]);
    });
  });

  describe("the list and the bulk approval", () => {
    const counts = {
      toilets: 3,
      drinkingWater: 0,
      sights: 0,
      hidden: 0,
      outside: 0,
      awaitingReview: 3,
    };

    it.each([
      ["three points, all inside", {}, true],
      [
        "toilets and water count together",
        { counts: { ...counts, toilets: 2, drinkingWater: 1 } },
        true,
      ],
      [
        "sights count with them",
        { counts: { ...counts, toilets: 1, sights: 2 } },
        true,
      ],
      ["two points", { counts: { ...counts, toilets: 2 } }, false],
      [
        "one point outside the outline",
        { counts: { ...counts, toilets: 5, outside: 1 } },
        false,
      ],
      ["already approved", { status: "approved" as const }, false],
      ["never imported", { status: "pending" as const }, false],
      ["last run failed", { lastRun: "failed" as const }, false],
    ])("%s", (_name, over, expected) => {
      expect(
        isBulkApprovable({
          status: "imported",
          lastRun: "ok",
          counts,
          ...over,
        }),
      ).toBe(expected);
    });

    async function twoParks() {
      const ctx = setup([
        {
          id: PARK,
          name: "Phantasialand",
          country: "Germany",
          latitude: 50.8,
          longitude: 6.88,
        },
        {
          id: OTHER,
          name: "Phantasialand",
          curatedName: "Zweitpark",
          latitude: 50.8,
          longitude: 6.88,
        },
        { id: "33333333-3333-4333-8333-333333333333", name: "Untouched" },
      ]);
      ctx.answer([toilet(1), toilet(2), toilet(3), toilet(4)]);
      await ctx.service.runImport(PARK);
      await ctx.service.setHidden(PARK, ctx.rows[3].id, true);
      // Three points, one of them outside.
      ctx.answer([toilet(5), toilet(6), toilet(7, {}, 50.85)]);
      await ctx.service.runImport(OTHER);
      return ctx;
    }

    it("lists every park with state and counts, hidden points apart", async () => {
      const { service } = await twoParks();
      const list = await service.listParks();
      expect(list.map((p) => [p.name, p.status, p.bulkApprovable])).toEqual([
        ["Phantasialand", "imported", true],
        ["Zweitpark", "imported", false],
        ["Untouched", "none", false],
      ]);
      expect(list[0]).toMatchObject({
        country: "Germany",
        lastRun: "ok",
        boundary: { type: "way", id: "31052062", name: "Phantasialand" },
        counts: {
          toilets: 3,
          drinkingWater: 0,
          sights: 0,
          hidden: 1,
          outside: 0,
        },
      });
      expect(list[1].counts).toMatchObject({ toilets: 3, outside: 1 });
      expect(list[2]).toMatchObject({ lastRun: null, boundary: null });
    });

    it("counts a sight as a sight, not as drinking water", async () => {
      const { service, answer } = setup();
      answer([
        toilet(1),
        toilet(2, { amenity: "drinking_water" }),
        toilet(3, { tourism: "artwork", amenity: "", name: "Pardoesbeeld" }),
      ]);
      await service.importPark(PARK);
      expect((await service.listParks())[0].counts).toEqual({
        toilets: 1,
        drinkingWater: 1,
        sights: 1,
        hidden: 0,
        outside: 0,
        // A first import: everything waits for the first approval.
        awaitingReview: 3,
      });
    });

    it("previews without approving, then approves exactly those parks", async () => {
      const { service, state, rows } = await twoParks();
      expect(await service.approveAll("admin@example.org", true)).toEqual({
        dryRun: true,
        count: 1,
        parks: ["Phantasialand"],
      });
      expect(state(PARK)!.status).toBe("imported");

      expect(
        await service.approveAll("admin@example.org", false),
      ).toMatchObject({
        dryRun: false,
        count: 1,
      });
      expect(state(PARK)).toMatchObject({
        status: "approved",
        approvedBy: "admin@example.org",
      });
      expect(
        rows.filter((r) => r.parkId === PARK).map((r) => r.awaitingReview),
      ).toEqual([false, false, false, false]);
      expect(state(OTHER)!.status).toBe("imported");
      expect((await service.approveAll("admin@example.org", false)).count).toBe(
        0,
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
