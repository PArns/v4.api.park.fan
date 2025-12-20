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
      wiki?: ParkMetadata;
      qt?: ParkMetadata;
      wz?: ParkMetadata;
      confidence: number;
    }>;
    wikiOnly: ParkMetadata[];
    qtOnly: ParkMetadata[];
    wzOnly: ParkMetadata[];
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

    // 1. Get raw lists
    const wikiParks = parksBySource.get("themeparks-wiki") || [];
    const qtParks = parksBySource.get("queue-times") || [];
    const wzParks = parksBySource.get("wartezeiten-app") || [];

    // 2. Match Wiki vs Queue-Times
    // Result: matched (Wiki+QT), wikiOnly (Wiki-QT), qtOnly (QT-Wiki)
    const qtMatchResult = this.entityMatcher.matchParks(wikiParks, qtParks);

    // 3. Match Wiki vs Wartezeiten.app
    // Result: matched (Wiki+WZ), wikiOnly (Wiki-WZ), qtOnly (WZ-Wiki)
    const wzMatchResult = this.entityMatcher.matchParks(wikiParks, wzParks);

    // 4. Match Remaining QT vs Remaining WZ (Parks NOT in Wiki)
    // We use the "unmatched" lists from previous steps
    const qtNoWiki = qtMatchResult.qtOnly;
    const wzNoWiki = wzMatchResult.qtOnly;

    // Use matchParks but interpret the result carefully:
    // "wiki" arg -> QT parks
    // "qt" arg -> WZ parks
    const noWikiMatchResult = this.entityMatcher.matchParks(qtNoWiki, wzNoWiki);
    // matched.wiki -> QT
    // matched.qt -> WZ

    // 5. Construct Unified Result
    const mergedMatches: Array<{
      wiki?: ParkMetadata;
      qt?: ParkMetadata;
      wz?: ParkMetadata;
      confidence: number;
    }> = [];

    // Map by Wiki External ID for easy merging of Wiki-based matches
    const wikiMap = new Map<
      string,
      {
        wiki: ParkMetadata;
        qt?: ParkMetadata;
        wz?: ParkMetadata;
        confidence: number;
      }
    >();

    // Start with all Wiki parks as potential base
    for (const wiki of wikiParks) {
      wikiMap.set(wiki.externalId, { wiki, confidence: 1.0 });
    }

    // Merge QT matches
    for (const m of qtMatchResult.matched) {
      const entry = wikiMap.get(m.wiki.externalId)!;
      entry.qt = m.qt;
      entry.confidence = m.confidence;
    }

    // Merge WZ matches
    for (const m of wzMatchResult.matched) {
      const entry = wikiMap.get(m.wiki.externalId)!;
      entry.wz = m.qt; // standard matchParks puts 2nd arg in 'qt' prop
      // Average confidence if both exist?
      if (entry.qt) {
        entry.confidence = (entry.confidence + m.confidence) / 2;
      } else {
        entry.confidence = m.confidence;
      }
    }

    // Filter wikiMap for entries that have >1 source
    // These are "Matched" parks involving Wiki
    for (const entry of wikiMap.values()) {
      if (entry.qt || entry.wz) {
        mergedMatches.push(entry);
      }
    }

    // Add No-Wiki Matches (QT + WZ)
    for (const m of noWikiMatchResult.matched) {
      mergedMatches.push({
        wiki: undefined,
        qt: m.wiki, // First arg was QT
        wz: m.qt, // Second arg was WZ
        confidence: m.confidence,
      });
    }

    // 6. Calculate Residuals (Truly Single Source)
    // Wiki Only: Parks in wikiParks that are NOT in mergedMatches
    // Helper set of processed IDs
    const processedWikiIds = new Set(
      mergedMatches.filter((m) => m.wiki).map((m) => m.wiki!.externalId),
    );
    const finalWikiOnly = wikiParks.filter(
      (p) => !processedWikiIds.has(p.externalId),
    );

    // QT Only: From noWikiMatchResult.wikiOnly (since we passed matching QT as first arg)
    const finalQtOnly = noWikiMatchResult.wikiOnly;

    // WZ Only: From noWikiMatchResult.qtOnly (since we passed matching WZ as second arg)
    const finalWzOnly = noWikiMatchResult.qtOnly;

    this.logger.log(
      `Park discovery complete: ${mergedMatches.length} combined, ` +
        `${finalWikiOnly.length} wiki-only, ` +
        `${finalQtOnly.length} qt-only, ` +
        `${finalWzOnly.length} wz-only`,
    );

    return {
      matched: mergedMatches,
      wikiOnly: finalWikiOnly,
      qtOnly: finalQtOnly,
      wzOnly: finalWzOnly,
    };
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
