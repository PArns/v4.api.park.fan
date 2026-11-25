import type { ParkFacilityOsm } from "./entities/park-facility-osm.entity";
import { planFacilityMerge } from "./facility-merge";
import type { ImportedFacility } from "./facility.types";

const facility = (over: Partial<ImportedFacility> = {}): ImportedFacility => ({
  type: "toilets",
  latitude: 50.8,
  longitude: 6.88,
  name: null,
  wheelchair: "yes",
  changingTable: null,
  fee: false,
  osmType: "node",
  osmId: "1",
  osmVersion: 3,
  osmTimestamp: new Date("2024-05-26T13:06:10Z"),
  insideOutline: true,
  ...over,
});

const stored = (
  id: string,
  over: Partial<ImportedFacility> = {},
  hidden = false,
): ParkFacilityOsm => ({
  ...facility(over),
  id,
  parkId: "park",
  fetchedAt: new Date("2026-01-01T00:00:00Z"),
  hidden,
});

describe("planFacilityMerge", () => {
  it("inserts everything into an empty park", () => {
    const plan = planFacilityMerge([], [facility(), facility({ osmId: "2" })]);
    expect(plan).toMatchObject({ update: [], removeIds: [], unchanged: 0 });
    expect(plan.insert).toHaveLength(2);
  });

  it("plans nothing when the same answer comes in again", () => {
    const plan = planFacilityMerge(
      [stored("a"), stored("b", { osmId: "2" })],
      [facility(), facility({ osmId: "2" })],
    );
    expect(plan).toEqual({
      insert: [],
      update: [],
      removeIds: [],
      unchanged: 2,
    });
  });

  it("removes what vanished from OSM and nothing else", () => {
    const plan = planFacilityMerge(
      [stored("a"), stored("b", { osmId: "2" })],
      [facility()],
    );
    expect(plan.removeIds).toEqual(["b"]);
    expect(plan.unchanged).toBe(1);
  });

  it("tells a visible change from a bare version bump", () => {
    const plan = planFacilityMerge(
      [stored("a"), stored("b", { osmId: "2" })],
      [
        facility({ osmVersion: 4, wheelchair: "no" }),
        facility({ osmId: "2", osmVersion: 4 }),
      ],
    );
    expect(plan.update.map((u) => [u.id, u.visible])).toEqual([
      ["a", true],
      ["b", false],
    ]);
  });

  it("sees a way's centre move while its version stays", () => {
    const way = { osmType: "way" as const, osmId: "9" };
    const plan = planFacilityMerge(
      [stored("a", way)],
      [facility({ ...way, latitude: 50.8001 })],
    );
    expect(plan.update).toEqual([
      { id: "a", facility: expect.any(Object), visible: true },
    ]);
  });

  it("updates a point that moved across the outline, without asking for review", () => {
    const plan = planFacilityMerge(
      [stored("a")],
      [facility({ insideOutline: false })],
    );
    expect(plan.update.map((u) => u.visible)).toEqual([false]);
  });

  it("keeps node 1 and way 1 apart", () => {
    const plan = planFacilityMerge(
      [stored("a")],
      [facility(), facility({ osmType: "way" })],
    );
    expect(plan.insert).toHaveLength(1);
    expect(plan.unchanged).toBe(1);
  });

  it("never carries `hidden`, so an update cannot reset it", () => {
    const plan = planFacilityMerge(
      [stored("a", {}, true)],
      [facility({ osmVersion: 4, fee: true })],
    );
    expect(plan.update[0].facility).not.toHaveProperty("hidden");
  });

  it("takes an element once when the answer lists it twice", () => {
    expect(planFacilityMerge([], [facility(), facility()]).insert).toHaveLength(
      1,
    );
  });
});
