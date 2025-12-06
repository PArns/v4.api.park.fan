import {
  DestinationsApiResponse,
  EntityResponse,
  EntityChildrenResponse,
  EntityLiveResponse,
  EntityType,
  LiveStatus,
  QueueType,
} from "../../src/external-apis/themeparks/themeparks.types";

/**
 * Mock ThemeParks.wiki API Client for testing
 * Returns predictable, deterministic test data
 */
export class MockThemeParksClient {
  async getDestinations(): Promise<DestinationsApiResponse> {
    return {
      destinations: [
        {
          id: "test-dest-wdw",
          name: "Test Walt Disney World Resort",
          slug: "test-walt-disney-world-resort",
          parks: [
            { id: "test-park-mk", name: "Test Magic Kingdom" },
            { id: "test-park-epcot", name: "Test EPCOT" },
          ],
        },
      ],
    };
  }

  async getEntity(entityId: string): Promise<EntityResponse> {
    // Return mock entity based on ID pattern
    if (entityId.includes("park")) {
      return {
        id: entityId,
        name: `Test Park ${entityId}`,
        entityType: EntityType.PARK,
        slug: `test-park-${entityId}`,
        destinationId: "test-dest-wdw",
        location: {
          latitude: 28.4177,
          longitude: -81.5812,
        },
        timezone: "America/New_York",
      };
    }

    if (entityId.includes("attr")) {
      return {
        id: entityId,
        name: `Test Attraction ${entityId}`,
        entityType: EntityType.ATTRACTION,
        slug: `test-attraction-${entityId}`,
        parentId: "test-park-mk",
        location: {
          latitude: 28.4194,
          longitude: -81.5776,
        },
      };
    }

    // Default entity
    return {
      id: entityId,
      name: `Test Entity ${entityId}`,
      entityType: EntityType.PARK,
      slug: `test-entity-${entityId}`,
    };
  }

  async getEntityChildren(entityId: string): Promise<EntityChildrenResponse> {
    return {
      children: [
        {
          id: "test-attr-space-mountain",
          name: "Test Space Mountain",
          entityType: EntityType.ATTRACTION,
          slug: "test-space-mountain",
          parentId: entityId,
          location: {
            latitude: 28.4194,
            longitude: -81.5776,
          },
        },
        {
          id: "test-attr-pirates",
          name: "Test Pirates of the Caribbean",
          entityType: EntityType.ATTRACTION,
          slug: "test-pirates-of-the-caribbean",
          parentId: entityId,
          location: {
            latitude: 28.4182,
            longitude: -81.5831,
          },
        },
        {
          id: "test-show-fireworks",
          name: "Test Fireworks Show",
          entityType: EntityType.SHOW,
          slug: "test-fireworks-show",
          parentId: entityId,
        },
      ],
    };
  }

  async getLiveData(entityId: string): Promise<EntityLiveResponse> {
    return {
      id: entityId,
      name: `Test Entity ${entityId}`,
      entityType: EntityType.ATTRACTION,
      status: LiveStatus.OPERATING,
      lastUpdated: new Date().toISOString(),
      queue: {
        [QueueType.STANDBY]: {
          waitTime: 30,
        },
        [QueueType.SINGLE_RIDER]: {
          waitTime: 15,
        },
      },
    };
  }

  async getParkLiveData(parkId: string): Promise<any> {
    return {
      liveData: [
        {
          id: "test-attr-space-mountain",
          name: "Test Space Mountain",
          entityType: EntityType.ATTRACTION,
          status: LiveStatus.OPERATING,
          lastUpdated: new Date().toISOString(),
          queue: {
            [QueueType.STANDBY]: {
              waitTime: 45,
            },
          },
        },
        {
          id: "test-attr-pirates",
          name: "Test Pirates of the Caribbean",
          entityType: EntityType.ATTRACTION,
          status: LiveStatus.OPERATING,
          lastUpdated: new Date().toISOString(),
          queue: {
            [QueueType.STANDBY]: {
              waitTime: 20,
            },
          },
        },
      ],
    };
  }
}

/**
 * Factory function to create a mock ThemeParks client
 */
export const createMockThemeParksClient = () => new MockThemeParksClient();
