import { Attraction } from "../../src/attractions/entities/attraction.entity";

export const createTestAttraction = (
  parkId: string,
  overrides?: Partial<Attraction>,
): Attraction => {
  const attraction = new Attraction();
  attraction.externalId = "test-attr-space-mountain";
  attraction.name = "Test Space Mountain";
  attraction.slug = "test-space-mountain";
  attraction.parkId = parkId;
  attraction.latitude = 28.4194;
  attraction.longitude = -81.5776;

  return Object.assign(attraction, overrides);
};

/**
 * Creates a set of test attractions for a given park
 * Returns 5 attractions: Space Mountain, Pirates, Haunted Mansion, Big Thunder, Splash Mountain
 */
export const createTestAttractions = (
  parkId: string,
  parkIndex = 0,
): Attraction[] => {
  const suffix = parkIndex > 0 ? `-p${parkIndex}` : "";
  return [
    createTestAttraction(parkId, {
      externalId: `test-attr-space-mountain${suffix}`,
      name: `Test Space Mountain${parkIndex > 0 ? ` (Park ${parkIndex})` : ""}`,
      slug: `test-space-mountain${suffix}`,
      latitude: 28.4194,
      longitude: -81.5776,
    }),
    createTestAttraction(parkId, {
      externalId: `test-attr-pirates${suffix}`,
      name: `Test Pirates of the Caribbean${parkIndex > 0 ? ` (Park ${parkIndex})` : ""}`,
      slug: `test-pirates-of-the-caribbean${suffix}`,
      latitude: 28.4182,
      longitude: -81.5831,
    }),
    createTestAttraction(parkId, {
      externalId: `test-attr-haunted-mansion${suffix}`,
      name: `Test Haunted Mansion${parkIndex > 0 ? ` (Park ${parkIndex})` : ""}`,
      slug: `test-haunted-mansion${suffix}`,
      latitude: 28.4203,
      longitude: -81.582,
    }),
    createTestAttraction(parkId, {
      externalId: `test-attr-big-thunder${suffix}`,
      name: `Test Big Thunder Mountain Railroad${parkIndex > 0 ? ` (Park ${parkIndex})` : ""}`,
      slug: `test-big-thunder-mountain-railroad${suffix}`,
      latitude: 28.4197,
      longitude: -81.5845,
    }),
    createTestAttraction(parkId, {
      externalId: `test-attr-splash-mountain${suffix}`,
      name: `Test Splash Mountain${parkIndex > 0 ? ` (Park ${parkIndex})` : ""}`,
      slug: `test-splash-mountain${suffix}`,
      latitude: 28.4191,
      longitude: -81.5838,
    }),
  ];
};
