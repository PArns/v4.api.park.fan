import { Park } from "../../src/parks/entities/park.entity";
import { Destination } from "../../src/destinations/entities/destination.entity";

export const createTestDestination = (
  overrides?: Partial<Destination>,
): Destination => {
  const destination = new Destination();
  destination.externalId = "test-dest-wdw";
  destination.name = "Test Walt Disney World Resort";
  destination.slug = "test-walt-disney-world-resort";

  return Object.assign(destination, overrides);
};

export const createTestPark = (overrides?: Partial<Park>): Park => {
  const park = new Park();
  park.externalId = "test-park-mk";
  park.name = "Test Magic Kingdom";
  park.slug = "test-magic-kingdom";
  park.latitude = 28.4177;
  park.longitude = -81.5812;
  park.timezone = "America/New_York";
  park.continent = "North America";
  park.continentSlug = "north-america";
  park.country = "United States";
  park.countrySlug = "united-states";
  park.city = "Orlando";
  park.citySlug = "orlando";
  park.influencingCountries = ["US"];
  park.influenceRadiusKm = 200;

  return Object.assign(park, overrides);
};

/**
 * Creates a set of test parks for E2E tests
 * Returns 2 parks: Magic Kingdom and EPCOT
 */
export const createTestParks = (): Park[] => [
  createTestPark({
    externalId: "test-park-mk",
    name: "Test Magic Kingdom",
    slug: "test-magic-kingdom",
    timezone: "America/New_York",
  }),
  createTestPark({
    externalId: "test-park-epcot",
    name: "Test EPCOT",
    slug: "test-epcot",
    timezone: "America/New_York",
  }),
];

/**
 * Creates a test destination with parks
 * Includes Walt Disney World Resort with Magic Kingdom and EPCOT
 */
export const createTestDestinationWithParks = (): {
  destination: Destination;
  parks: Park[];
} => {
  const destination = createTestDestination();
  const parks = createTestParks();

  return { destination, parks };
};
