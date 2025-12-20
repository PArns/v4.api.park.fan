import { Injectable, Logger } from "@nestjs/common";
import {
  IDataSource,
  ParkMetadata,
  LiveDataResponse,
} from "./interfaces/data-source.interface";
import { EntityMatcherService } from "./entity-matcher.service";
import { ConflictResolverService } from "./conflict-resolver.service";

/**
 * Multi-Source Orchestrator
 *
 * Coordinates multiple data sources, performs entity matching,
 * and aggregates complementary data from all sources.
 */
@Injectable()
export class MultiSourceOrchestrator {
  private readonly logger = new Logger(MultiSourceOrchestrator.name);
  private sources: IDataSource[] = [];

  constructor(
    private readonly entityMatcher: EntityMatcherService,
    private readonly conflictResolver: ConflictResolverService,
  ) {}

  /**
   * Register a data source
   */
  registerSource(source: IDataSource): void {
    this.sources.push(source);
    this.logger.log(
      `Registered data source: ${source.name} (completeness: ${source.completeness}/10)`,
    );
  }

  /**
   * Get all registered sources
   */
  getSources(): IDataSource[] {
    return this.sources;
  }

  /**
   * Get a specific source by name
   */
  getSource(name: string): IDataSource | undefined {
    return this.sources.find((s) => s.name === name);
  }

  /**
   * Discover and match parks from all sources
   *
   * @returns All parks with matching information
   */
  async discoverAllParks(): Promise<{
    matched: Array<{
      wiki: ParkMetadata;
      qt: ParkMetadata;
      confidence: number;
    }>;
    wikiOnly: ParkMetadata[];
    qtOnly: ParkMetadata[];
  }> {
    this.logger.log("Starting park discovery from all sources...");

    // Fetch from all sources in parallel
    const results = await Promise.allSettled(
      this.sources.map((source) => source.fetchAllParks()),
    );

    const parksBySource = new Map<string, ParkMetadata[]>();
    results.forEach((result, i) => {
      const source = this.sources[i];
      if (result.status === "fulfilled") {
        parksBySource.set(source.name, result.value);
      } else {
        this.logger.error(
          `Failed to fetch parks from ${source.name}: ${result.reason}`,
        );
      }
    });

    // Match parks (currently supports Wiki + QueueTimes)
    const wikiParks = parksBySource.get("themeparks-wiki") || [];
    const qtParks = parksBySource.get("queue-times") || [];

    const matchResult = this.entityMatcher.matchParks(wikiParks, qtParks);

    this.logger.log(
      `Park discovery complete: ${matchResult.matched.length} matched, ` +
        `${matchResult.wikiOnly.length} wiki-only, ${matchResult.qtOnly.length} qt-only`,
    );

    return matchResult;
  }

  /**
   * Fetch live data for a park from all applicable sources
   *
   * @param parkId - Internal park ID
   * @param externalIds - Map of source name to external ID
   * @returns Aggregated live data
   */
  async fetchParkLiveData(
    parkId: string,
    externalIds: Map<string, string>,
  ): Promise<LiveDataResponse> {
    const liveDataBySource = new Map<string, LiveDataResponse>();

    // Fetch from all sources in parallel
    const fetchPromises = Array.from(externalIds.entries()).map(
      async ([sourceName, externalId]) => {
        const source = this.getSource(sourceName);
        if (!source) {
          this.logger.warn(`Source ${sourceName} not found`);
          return;
        }

        try {
          const liveData = await source.fetchParkLiveData(externalId);
          liveDataBySource.set(sourceName, liveData);
        } catch (error) {
          this.logger.error(`Failed to fetch from ${sourceName}: ${error}`);
        }
      },
    );

    await Promise.all(fetchPromises);

    // If no data from any source, throw error
    if (liveDataBySource.size === 0) {
      throw new Error(`No live data available for park ${parkId}`);
    }

    // Aggregate data from all sources
    return this.conflictResolver.aggregateParkData(liveDataBySource);
  }

  /**
   * Check health of all data sources
   */
  async checkHealth(): Promise<Map<string, boolean>> {
    const healthMap = new Map<string, boolean>();

    for (const source of this.sources) {
      try {
        const isHealthy = await source.isHealthy();
        healthMap.set(source.name, isHealthy);
      } catch {
        healthMap.set(source.name, false);
      }
    }

    return healthMap;
  }
}
