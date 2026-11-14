import { AdminContentController } from "./admin-content.controller";

function aRow(overrides: Record<string, unknown> = {}) {
  return {
    id: "ride-1",
    name: "Winni Splash",
    slug: "winni-splash",
    parkId: "park-1",
    retiredAt: null,
    updatedAt: new Date("2026-10-01T00:00:00Z"),
    ...overrides,
  };
}

function build(rows: unknown[], parks: object = {}) {
  const qb: Record<string, jest.Mock> = {};
  for (const method of ["where", "andWhere", "orderBy"]) {
    qb[method] = jest.fn(() => qb);
  }
  qb.getMany = jest.fn(async () => rows);
  const attractions = { createQueryBuilder: jest.fn(() => qb) };
  const rideProfiles = {
    findIdsWithProfile: jest.fn(async () => new Set<string>()),
  };
  const controller = new AdminContentController(
    parks as never,
    attractions as never,
    {} as never,
    rideProfiles as never,
    {} as never,
    {} as never,
    {} as never,
  );
  return controller;
}

describe("AdminContentController.listAttractions", () => {
  it("carries the raw virtual line, single rider and indoor/outdoor values", async () => {
    const controller = build([
      aRow({
        hasVirtualLine: true,
        hasSingleRider: false,
        indoorOutdoor: "covered_queue",
        attractionKind: "TRANSPORT",
      }),
    ]);

    const { attractions } = await controller.listAttractions("park-1");

    expect(attractions[0]).toMatchObject({
      hasVirtualLine: true,
      hasSingleRider: false,
      indoorOutdoor: "covered_queue",
      attractionKind: "TRANSPORT",
    });
  });

  it("names the season that binds a ride and the one that pauses it, on the park's today", async () => {
    const PARK = "11111111-1111-4111-8111-111111111111";
    const season = {
      id: "s1",
      kind: "winter",
      name: "Wintertraum",
      status: "confirmed",
      startDate: "2026-11-20",
      endDate: "2027-01-18",
      dates: null,
    };
    const parks = {
      findOne: jest.fn(async () => ({ id: PARK, timezone: "Europe/Berlin" })),
    };
    const controller = build(
      [
        aRow({ id: "rink", seasonBindings: [season] }),
        aRow({ id: "river-quest", seasonPauses: [season] }),
        aRow({ id: "taron" }),
      ],
      parks,
    );

    const { attractions } = await controller.listAttractions(PARK);

    expect(parks.findOne).toHaveBeenCalledWith(
      expect.objectContaining({ where: { id: PARK } }),
    );
    expect(attractions[0].boundSeason).toEqual(season);
    expect(attractions[1].pausedBySeason).toEqual(season);
    expect(attractions[2]).not.toHaveProperty("boundSeason");
    expect(attractions[2]).not.toHaveProperty("pausedBySeason");
  });

  it("reports null for a ride nobody has checked, not false", async () => {
    const controller = build([
      aRow({
        hasVirtualLine: undefined,
        hasSingleRider: null,
        indoorOutdoor: null,
        attractionKind: undefined,
      }),
    ]);

    const { attractions } = await controller.listAttractions("park-1");

    expect(attractions[0].hasVirtualLine).toBeNull();
    expect(attractions[0].hasSingleRider).toBeNull();
    expect(attractions[0].indoorOutdoor).toBeNull();
    expect(attractions[0].attractionKind).toBeNull();
  });
});
