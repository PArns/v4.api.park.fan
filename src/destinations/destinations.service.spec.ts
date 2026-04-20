import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { DestinationsService } from "./destinations.service";
import { Destination } from "./entities/destination.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";

describe("DestinationsService (Optimized)", () => {
  let service: DestinationsService;
  let destinationRepository: any;
  let themeParksClient: any;

  beforeEach(async () => {
    destinationRepository = {
      findOne: jest.fn(),
      find: jest.fn(),
      update: jest.fn(),
      save: jest.fn(),
      createQueryBuilder: jest.fn(() => ({
        insert: jest.fn().mockReturnThis(),
        into: jest.fn().mockReturnThis(),
        values: jest.fn().mockReturnThis(),
        orIgnore: jest.fn().mockReturnThis(),
        execute: jest.fn().mockResolvedValue({}),
      })),
    };

    themeParksClient = {
      getDestinations: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DestinationsService,
        {
          provide: getRepositoryToken(Destination),
          useValue: destinationRepository,
        },
        {
          provide: ThemeParksClient,
          useValue: themeParksClient,
        },
        ThemeParksMapper,
      ],
    }).compile();

    service = module.get<DestinationsService>(DestinationsService);
  });

  it("should have significantly fewer database calls for syncDestinations", async () => {
    const numDestinations = 50;
    const destinations = Array.from({ length: numDestinations }, (_, i) => ({
      id: `ext-${i}`,
      name: `Destination ${i}`,
      slug: `destination-${i}`,
      parks: [],
    }));

    themeParksClient.getDestinations.mockResolvedValue({ destinations });

    // Setup existing destinations (half exist, and one name changed)
    const existing = Array.from({ length: numDestinations / 2 }, (_, i) => ({
      id: `uuid-${i}`,
      externalId: `ext-${i}`,
      name: i === 0 ? `Old Name ${i}` : `Destination ${i}`, // Only the first one changed
      slug: `slug-${i}`
    }));

    destinationRepository.find.mockResolvedValue(existing);
    destinationRepository.save.mockResolvedValue([]);

    await service.syncDestinations();

    console.log(`findOne calls: ${destinationRepository.findOne.mock.calls.length}`);
    console.log(`find calls: ${destinationRepository.find.mock.calls.length}`);
    console.log(`save calls: ${destinationRepository.save.mock.calls.length}`);
    console.log(`insert calls (createQueryBuilder): ${destinationRepository.createQueryBuilder.mock.calls.length}`);

    // Expectation:
    // 0 findOne calls (was 50)
    // 1 find call (was 25+ if many new)
    // 1 save call (was 25 update calls)
    // 1 insert call (was 25 insert calls)

    expect(destinationRepository.findOne.mock.calls.length).toBe(0);
    expect(destinationRepository.find.mock.calls.length).toBe(1);
    expect(destinationRepository.save.mock.calls.length).toBe(1);
    expect(destinationRepository.createQueryBuilder.mock.calls.length).toBe(1);
  });
});
