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

function build(rows: unknown[]) {
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
    {} as never,
    attractions as never,
    {} as never,
    rideProfiles as never,
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
