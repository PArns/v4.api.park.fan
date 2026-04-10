import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { QueuePercentileProcessor } from "./queue-percentile.processor";
import { QueueDataAggregate } from "../../analytics/entities/queue-data-aggregate.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { Show } from "../../shows/entities/show.entity";

describe("QueuePercentileProcessor", () => {
  let processor: QueuePercentileProcessor;
  let dataSource: DataSource;

  const mockQuery = jest.fn();
  const mockUpdate = jest.fn();

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        QueuePercentileProcessor,
        {
          provide: getRepositoryToken(QueueDataAggregate),
          useValue: { query: mockQuery },
        },
        {
          provide: getRepositoryToken(Attraction),
          useValue: { update: mockUpdate },
        },
        {
          provide: getRepositoryToken(Show),
          useValue: { update: mockUpdate },
        },
        {
          provide: DataSource,
          useValue: { query: mockQuery },
        },
      ],
    }).compile();

    processor = module.get<QueuePercentileProcessor>(QueuePercentileProcessor);
    dataSource = module.get<DataSource>(DataSource);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("handleDetectSeasonal", () => {
    it("should use parameterized queries for attractions and shows with correct indices", async () => {
      // Mock data for Step 1: recently operating attractions
      mockQuery.mockResolvedValueOnce([
        { attractionId: "attr-1" },
        { attractionId: "attr-2" },
      ]);

      // Mock data for Step 2: reset result
      mockQuery.mockResolvedValueOnce([[], 2]);

      // Mock data for Step 3: candidates
      mockQuery.mockResolvedValueOnce([
        { attractionId: "attr-3", parkId: "park-1" },
      ]);

      // Mock data for Step 4: season months for candidates
      mockQuery.mockResolvedValueOnce([{ month: 1 }, { month: 2 }]);

      // Mock data for Step S1: recently updated shows
      mockQuery.mockResolvedValueOnce([{ showId: "show-1" }]);

      // Mock data for Step S1: reset shows result
      mockQuery.mockResolvedValueOnce([[], 1]);

      // Mock data for Step S2: show candidates
      mockQuery.mockResolvedValueOnce([{ showId: "show-2" }]);

      // Mock data for Step S3: season months for show candidates
      mockQuery.mockResolvedValueOnce([{ month: 6 }]);

      await processor.handleDetectSeasonal({} as any);

      // Verify Step 1: recently operating attractions
      expect(mockQuery).toHaveBeenNthCalledWith(
        1,
        expect.stringContaining("SELECT DISTINCT q.\"attractionId\""),
        [14],
      );
      expect(mockQuery.mock.calls[0][0]).toContain("$1 * INTERVAL '1 day'");

      // Verify Step 2: reset attractions
      expect(mockQuery).toHaveBeenNthCalledWith(
        2,
        expect.stringContaining("UPDATE attractions"),
        [expect.arrayContaining(["attr-1", "attr-2"])],
      );

      // Verify Step 3: candidate selection
      const candidateSelectionCall = mockQuery.mock.calls[2];
      expect(candidateSelectionCall[0]).toContain("WHERE q.status = 'OPERATING' AND q.timestamp >= NOW() - ($1 * INTERVAL '1 day')");
      expect(candidateSelectionCall[0]).toContain("HAVING COUNT(*) >= $2");
      expect(candidateSelectionCall[0]).toContain("WHERE d.fully_closed_days >= $3");
      expect(candidateSelectionCall[0]).toContain("AND NOT (d.\"attractionId\" = ANY($4::uuid[]))");
      expect(candidateSelectionCall[1]).toEqual([60, 20, 7, expect.arrayContaining(["attr-1", "attr-2"])]);

      // Verify Step S1: recently updated shows
      expect(mockQuery).toHaveBeenNthCalledWith(
        5,
        expect.stringContaining("SELECT DISTINCT \"showId\""),
        [14],
      );
      expect(mockQuery.mock.calls[4][0]).toContain("$1 * INTERVAL '1 day'");

      // Verify Step S1: reset shows
      expect(mockQuery).toHaveBeenNthCalledWith(
        6,
        expect.stringContaining("UPDATE shows"),
        [expect.arrayContaining(["show-1"])],
      );

      // Verify Step S2: show candidate selection
      const showCandidateSelectionCall = mockQuery.mock.calls[6];
      expect(showCandidateSelectionCall[0]).toContain("WHERE q.status = 'OPERATING' AND q.timestamp >= NOW() - ($1 * INTERVAL '1 day')");
      expect(showCandidateSelectionCall[0]).toContain("WHERE stale_open_days >= $2");
      expect(showCandidateSelectionCall[0]).toContain("AND NOT (\"showId\" = ANY($3::uuid[]))");
      expect(showCandidateSelectionCall[1]).toEqual([60, 7, expect.arrayContaining(["show-1"])]);

      // Ensure no string interpolation of IDs in queries
      const allQueryCalls = mockQuery.mock.calls.map(call => call[0]);
      allQueryCalls.forEach(query => {
        expect(query).not.toContain("'attr-1'");
        expect(query).not.toContain("'attr-2'");
        expect(query).not.toContain("'show-1'");
      });
    });

    it("should handle cases with no recently operating IDs correctly", async () => {
        // Step 1: no recently operating attractions
        mockQuery.mockResolvedValueOnce([]);

        // Step 3: candidates
        mockQuery.mockResolvedValueOnce([]);

        // Step S1: no recently updated shows
        mockQuery.mockResolvedValueOnce([]);

        // Step S2: no show candidates
        mockQuery.mockResolvedValueOnce([]);

        await processor.handleDetectSeasonal({} as any);

        // Should call Step 1
        expect(mockQuery).toHaveBeenCalledWith(
            expect.stringContaining("SELECT DISTINCT q.\"attractionId\""),
            [14]
        );

        // Should NOT call Step 2 (reset)
        const updateAttractionsCall = mockQuery.mock.calls.find(call => call[0].includes("UPDATE attractions"));
        expect(updateAttractionsCall).toBeUndefined();

        // Should call Step 3 with empty array parameter
        const candidateSelectionCall = mockQuery.mock.calls.find(call => call[0].includes("SELECT d.\"attractionId\""));
        expect(candidateSelectionCall[1]).toEqual([60, 20, 7, []]);
    });
  });
});
