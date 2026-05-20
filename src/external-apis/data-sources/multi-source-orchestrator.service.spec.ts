import { Test, TestingModule } from "@nestjs/testing";
import { MultiSourceOrchestrator } from "./multi-source-orchestrator.service";
import { EntityMatcherService } from "./entity-matcher.service";
import { ConflictResolverService } from "./conflict-resolver.service";
import { IDataSource } from "./interfaces/data-source.interface";

/**
 * Coverage for the central orchestrator that fans out live-data
 * fetches across multiple third-party sources (ThemeParks.wiki,
 * Queue-Times, Wartezeiten). The contract that matters in
 * production:
 *   1. One failing source must NOT kill the whole sync — we collect
 *      data from whoever responded.
 *   2. Sources fire in parallel (not serial — that's the whole
 *      point).
 *   3. When EVERY source fails the orchestrator throws explicitly
 *      so the cron retries.
 *   4. Health checks isolate per-source failures.
 */
describe("MultiSourceOrchestrator", () => {
  let orchestrator: MultiSourceOrchestrator;

  const conflictResolver = {
    aggregateParkData: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MultiSourceOrchestrator,
        { provide: EntityMatcherService, useValue: {} },
        { provide: ConflictResolverService, useValue: conflictResolver },
      ],
    }).compile();

    orchestrator = module.get(MultiSourceOrchestrator);
  });

  /** Build a minimal IDataSource stub. */
  const stubSource = (
    name: string,
    opts: {
      live?: () => Promise<unknown>;
      healthy?: () => Promise<boolean>;
    } = {},
  ): IDataSource =>
    ({
      name,
      completeness: 5,
      fetchParkLiveData:
        opts.live ??
        jest.fn().mockResolvedValue({
          entities: [],
          operatingHours: [],
        }),
      isHealthy: opts.healthy ?? jest.fn().mockResolvedValue(true),
    }) as unknown as IDataSource;

  describe("fetchParkLiveData", () => {
    it("aggregates data from every source that responds successfully", async () => {
      const wikiData = { entities: [{ id: "a1" }], operatingHours: [] };
      const qtData = { entities: [{ id: "a2" }], operatingHours: [] };
      orchestrator.registerSource(
        stubSource("themeparks-wiki", {
          live: () => Promise.resolve(wikiData),
        }),
      );
      orchestrator.registerSource(
        stubSource("queue-times", { live: () => Promise.resolve(qtData) }),
      );
      conflictResolver.aggregateParkData.mockReturnValue({
        entities: [...wikiData.entities, ...qtData.entities],
        operatingHours: [],
      });

      const externalIds = new Map([
        ["themeparks-wiki", "wiki-1"],
        ["queue-times", "qt-1"],
      ]);

      const result = await orchestrator.fetchParkLiveData("p-1", externalIds);

      expect(conflictResolver.aggregateParkData).toHaveBeenCalledTimes(1);
      // Both sources reached the aggregator.
      const aggregateArg = conflictResolver.aggregateParkData.mock
        .calls[0][0] as Map<string, unknown>;
      expect(aggregateArg.has("themeparks-wiki")).toBe(true);
      expect(aggregateArg.has("queue-times")).toBe(true);
      expect(result.entities).toHaveLength(2);
    });

    it("returns aggregated data from surviving sources when one fails", async () => {
      orchestrator.registerSource(
        stubSource("themeparks-wiki", {
          live: () => Promise.reject(new Error("503 from wiki")),
        }),
      );
      orchestrator.registerSource(
        stubSource("queue-times", {
          live: () =>
            Promise.resolve({ entities: [{ id: "a1" }], operatingHours: [] }),
        }),
      );
      conflictResolver.aggregateParkData.mockReturnValue({
        entities: [{ id: "a1" }],
        operatingHours: [],
      });

      const externalIds = new Map([
        ["themeparks-wiki", "wiki-1"],
        ["queue-times", "qt-1"],
      ]);

      const result = await orchestrator.fetchParkLiveData("p-1", externalIds);

      // The QT data still flowed through — wiki's failure is tolerated.
      expect(result.entities).toHaveLength(1);
      const aggregateArg = conflictResolver.aggregateParkData.mock
        .calls[0][0] as Map<string, unknown>;
      expect(aggregateArg.has("queue-times")).toBe(true);
      expect(aggregateArg.has("themeparks-wiki")).toBe(false);
    });

    it("throws when EVERY source fails (cron must retry)", async () => {
      orchestrator.registerSource(
        stubSource("themeparks-wiki", {
          live: () => Promise.reject(new Error("503")),
        }),
      );
      orchestrator.registerSource(
        stubSource("queue-times", {
          live: () => Promise.reject(new Error("ECONNREFUSED")),
        }),
      );

      const externalIds = new Map([
        ["themeparks-wiki", "wiki-1"],
        ["queue-times", "qt-1"],
      ]);

      await expect(
        orchestrator.fetchParkLiveData("p-1", externalIds),
      ).rejects.toThrow(/No live data/);
      // No aggregation call — we didn't have anything to aggregate.
      expect(conflictResolver.aggregateParkData).not.toHaveBeenCalled();
    });

    it("warns + skips unknown source names without throwing", async () => {
      orchestrator.registerSource(
        stubSource("themeparks-wiki", {
          live: () =>
            Promise.resolve({ entities: [{ id: "a1" }], operatingHours: [] }),
        }),
      );
      conflictResolver.aggregateParkData.mockReturnValue({
        entities: [{ id: "a1" }],
        operatingHours: [],
      });

      const externalIds = new Map([
        ["themeparks-wiki", "wiki-1"],
        // Source that was never registered — orchestrator should skip
        // it rather than throw.
        ["unknown-source", "x-1"],
      ]);

      const result = await orchestrator.fetchParkLiveData("p-1", externalIds);
      expect(result.entities).toHaveLength(1);
    });

    it("runs source fetches in parallel (concurrency, not serial)", async () => {
      // Two sources that each take ~50 ms. If serial, total ≈ 100 ms;
      // parallel ≈ 50 ms. We assert the total is closer to 50 than 100.
      const delay = (ms: number, value: unknown) =>
        new Promise((resolve) => setTimeout(() => resolve(value), ms));

      orchestrator.registerSource(
        stubSource("themeparks-wiki", {
          live: () =>
            delay(50, { entities: [], operatingHours: [] }) as Promise<{
              entities: unknown[];
              operatingHours: unknown[];
            }>,
        }),
      );
      orchestrator.registerSource(
        stubSource("queue-times", {
          live: () =>
            delay(50, { entities: [], operatingHours: [] }) as Promise<{
              entities: unknown[];
              operatingHours: unknown[];
            }>,
        }),
      );
      conflictResolver.aggregateParkData.mockReturnValue({
        entities: [],
        operatingHours: [],
      });

      const start = Date.now();
      await orchestrator.fetchParkLiveData(
        "p-1",
        new Map([
          ["themeparks-wiki", "x"],
          ["queue-times", "y"],
        ]),
      );
      const elapsed = Date.now() - start;

      // Parallel: should finish well under serial sum (90 ms is a
      // generous upper bound for two 50 ms fetches in parallel).
      expect(elapsed).toBeLessThan(90);
    });
  });

  describe("checkHealth", () => {
    it("collects per-source health into a map", async () => {
      orchestrator.registerSource(
        stubSource("a", { healthy: () => Promise.resolve(true) }),
      );
      orchestrator.registerSource(
        stubSource("b", { healthy: () => Promise.resolve(false) }),
      );

      const result = await orchestrator.checkHealth();

      expect(result.get("a")).toBe(true);
      expect(result.get("b")).toBe(false);
    });

    it("marks a source unhealthy when isHealthy throws", async () => {
      orchestrator.registerSource(
        stubSource("a", { healthy: () => Promise.reject(new Error("boom")) }),
      );

      const result = await orchestrator.checkHealth();
      expect(result.get("a")).toBe(false);
    });
  });

  describe("registerSource", () => {
    it("preserves registration order in getSources()", () => {
      const s1 = stubSource("s1");
      const s2 = stubSource("s2");
      orchestrator.registerSource(s1);
      orchestrator.registerSource(s2);
      expect(orchestrator.getSources()).toEqual([s1, s2]);
    });
  });
});
