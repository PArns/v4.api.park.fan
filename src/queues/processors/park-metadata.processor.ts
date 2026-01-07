import { Processor, Process, InjectQueue } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job, Queue } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { ParksService } from "../../parks/parks.service";
import { DestinationsService } from "../../destinations/destinations.service";
import { ThemeParksClient } from "../../external-apis/themeparks/themeparks.client";
import { GoogleGeocodingClient } from "../../external-apis/geocoding/google-geocoding.client";
import { MultiSourceOrchestrator } from "../../external-apis/data-sources/multi-source-orchestrator.service";
import { ExternalEntityMapping } from "../../database/entities/external-entity-mapping.entity";
import { Park } from "../../parks/entities/park.entity";
import { ParkMetadata } from "../../external-apis/data-sources/interfaces/data-source.interface";
import { generateSlug } from "../../common/utils/slug.util";
import { WARTEZEITEN_CREATION_WHITELIST } from "../../external-apis/data-sources/config/wartezeiten-only-parks";
import { ParkValidatorService } from "../../parks/services/park-validator.service";

/**
 * Park Metadata Processor (Multi-Source)
 *
 * Processes jobs in the 'park-metadata' queue.
 * Fetches parks from BOTH ThemeParks.wiki AND Queue-Times.com
 * Automatically matches parks across sources
 * Enriches all parks with geocoding data
 */
@Processor("park-metadata")
export class ParkMetadataProcessor {
  private readonly logger = new Logger(ParkMetadataProcessor.name);

  constructor(
    private parksService: ParksService,
    private destinationsService: DestinationsService,
    private themeParksClient: ThemeParksClient,
    private geocodingClient: GoogleGeocodingClient,
    private orchestrator: MultiSourceOrchestrator,
    private parkValidatorService: ParkValidatorService,
    @InjectRepository(ExternalEntityMapping)
    private mappingRepository: Repository<ExternalEntityMapping>,
    @InjectRepository(Park)
    private parkRepository: Repository<Park>,
    @InjectQueue("weather") private weatherQueue: Queue,
    @InjectQueue("children-metadata") private childrenQueue: Queue,
    @InjectQueue("park-enrichment") private enrichmentQueue: Queue,
    @InjectQueue("holidays") private holidaysQueue: Queue,
  ) {}

  @Process("sync-all-parks")
  async handleFetchParks(_job: Job): Promise<void> {
    this.logger.log("🚀 Starting MULTI-SOURCE park metadata sync...");

    try {
      // Step 1: Sync destinations (Wiki only)
      const destinationCount =
        await this.destinationsService.syncDestinations();
      this.logger.log(`✅ Synced ${destinationCount} destinations`);

      // Step 2: Build Known Matches Map (DB-based)
      this.logger.log("🔒 Building ID-based match maps from DB...");
      const dbParks = await this.parkRepository.find();

      const wikiToQt = new Map<string, string>();
      const wikiToWz = new Map<string, string>();
      const qtToWz = new Map<string, string>();

      for (const p of dbParks) {
        if (p.wikiEntityId) {
          if (p.queueTimesEntityId) {
            wikiToQt.set(p.wikiEntityId, p.queueTimesEntityId);
          }
          if (p.wartezeitenEntityId) {
            wikiToWz.set(p.wikiEntityId, p.wartezeitenEntityId);
          }
        } else if (p.queueTimesEntityId && p.wartezeitenEntityId) {
          // No Wiki, but QT <-> WZ match exists
          qtToWz.set(p.queueTimesEntityId, p.wartezeitenEntityId);
        }
      }

      this.logger.log(
        `🔒 Found known matches: Wiki-QT=${wikiToQt.size}, Wiki-WZ=${wikiToWz.size}, QT-WZ=${qtToWz.size}`,
      );

      // Step 3: Discover parks from ALL sources
      this.logger.log("🔍 Discovering parks from all sources...");
      const { matched, wikiOnly, qtOnly, wzOnly } =
        await this.orchestrator.discoverAllParks({
          wikiToQt,
          wikiToWz,
          qtToWz,
        });

      this.logger.log(
        `📊 Discovery complete: ${matched.length} matched, ${wikiOnly.length} wiki-only, ${qtOnly.length} qt-only, ${wzOnly.length} wz-only`,
      );

      // --- PHASE 1: THEMEPARKS.WIKI (The "Truth" Base) ---
      this.logger.log("🔹 Phase 1: Processing Wiki-based parks...");

      // 1a. Process Wiki-Only
      for (const parkMeta of wikiOnly) {
        await this.processWikiOnlyPark(parkMeta);
      }

      // 1b. Process Matched (Wiki-Anchored)
      const wikiMatched = matched.filter((m) => m.wiki !== undefined);
      for (const match of wikiMatched) {
        await this.processMatchedPark(
          match.wiki,
          match.qt,
          match.wz,
          match.confidence,
        );
      }

      // --- PHASE 2: QUEUE-TIMES (Fill gaps) ---
      this.logger.log("🔹 Phase 2: Processing Queue-Times-based parks...");

      // 2a. Process QT-Only
      for (const parkMeta of qtOnly) {
        await this.processQueueTimesOnlyPark(parkMeta);
      }

      // 2b. Process Matched (QT-Anchored, No Wiki)
      const qtMatched = matched.filter(
        (m) => m.wiki === undefined && m.qt !== undefined,
      );
      for (const match of qtMatched) {
        await this.processMatchedPark(
          undefined, // No Wiki
          match.qt,
          match.wz,
          match.confidence,
        );
      }

      // --- PHASE 3: WARTEZEITEN.APP (Enrichment / Leftovers) ---
      this.logger.log("🔹 Phase 3: Processing Wartezeiten-based parks...");

      // 3a. Process Matched (WZ-Anchored, No Wiki, No QT - rare)
      const wzMatched = matched.filter(
        (m) => m.wiki === undefined && m.qt === undefined && m.wz !== undefined,
      );
      for (const match of wzMatched) {
        await this.processMatchedPark(
          undefined,
          undefined,
          match.wz,
          match.confidence,
        );
      }

      // 3b. Process Wartezeiten-only parks (Selective Creation)
      // Some parks only exist in Wartezeiten but are valuable (e.g. Nigloland)
      // We use a whitelist config to determine which ones to create and provide missing data (lat/lon)

      const wzToCreate: ParkMetadata[] = [];
      const wzIgnored: ParkMetadata[] = [];

      for (const park of wzOnly) {
        // Clean name for check (although DataSource handles most cases)
        // Ensure "Nigloland (FR)" -> "Nigloland" just in case
        const cleanedName = park.name.replace(/\s*\([A-Z]{2}\)$/, "").trim();

        // Check if this park is in our whitelist (either by raw name or cleaned name)
        const isWhitelisted =
          WARTEZEITEN_CREATION_WHITELIST[cleanedName] !== undefined ||
          WARTEZEITEN_CREATION_WHITELIST[park.name] !== undefined;

        if (isWhitelisted) {
          wzToCreate.push(park);
        } else {
          wzIgnored.push(park);
        }
      }

      // Process Allowed Wartezeiten Parks
      for (const park of wzToCreate) {
        await this.processWartezeitenOnlyPark(park);
      }

      // Log ignored parks
      if (wzIgnored.length > 0) {
        this.logger.warn(
          `⚠️  Found ${wzIgnored.length} parks only in Wartezeiten.app (not creating - enrichment only):`,
        );
        wzIgnored.forEach((park) => {
          this.logger.warn(
            `   - ${park.name} (${park.country || "unknown country"})`,
          );
        });
        this.logger.log(
          `💡 Tip: These parks should be added to Wiki or Queue-Times first, then Wartezeiten can enrich them.`,
        );
      }

      // Step 7: Sync schedules for all parks
      await this.syncSchedulesForAllParks();

      // Step 8: Geocode parks that need it
      await this.geocodeParks();

      this.logger.log("🎉 Multi-source park metadata sync complete!");

      // Step 9: Enrich parks with ISO codes and influencing countries
      this.logger.log("🌍 Triggering park enrichment...");
      await this.enrichmentQueue.add("enrich-all", {}, { priority: 3 });

      // Step 10: Trigger holidays sync (AFTER parks are ready)
      this.logger.log("🎉 Triggering holidays sync...");
      await this.holidaysQueue.add("fetch-holidays", {}, { priority: 4 });

      // Step 11: Trigger children metadata sync
      this.logger.log("🎢 Triggering children metadata sync...");
      await this.childrenQueue.add("fetch-all-children", {}, { priority: 2 });

      // Step 12: Trigger weather sync
      await this.weatherQueue.add("fetch-weather", {}, { priority: 2 });

      // Step 13: Validate parks (warnings only, no auto-fix)
      await this.validateParksAfterSync();
    } catch (error) {
      this.logger.error("❌ Park metadata sync failed", error);
      throw error;
    }
  }

  /**
   * Validates parks after sync (warnings only, no auto-fix)
   */
  private async validateParksAfterSync(): Promise<void> {
    try {
      this.logger.log("🔍 Validating parks after sync...");
      const validationReport = await this.parkValidatorService.validateAll();

      if (validationReport.summary.issuesFound > 0) {
        this.logger.warn(
          `⚠️ Found ${validationReport.summary.issuesFound} issues after sync:`,
        );
        this.logger.warn(
          `   - ${validationReport.mismatchedQtIds.length} mismatched QT-IDs`,
        );
        this.logger.warn(
          `   - ${validationReport.mismatchedWzIds.length} mismatched WZ-IDs`,
        );
        this.logger.warn(
          `   - ${validationReport.missingQtIds.length} missing QT-IDs`,
        );
        this.logger.warn(
          `   - ${validationReport.missingWzIds.length} missing WZ-IDs`,
        );
        this.logger.warn(
          `   - ${validationReport.duplicates.length} duplicates`,
        );
        this.logger.warn(
          `💡 Use POST /v1/admin/validate-and-repair-parks to see details and repair`,
        );
      } else {
        this.logger.log("✅ No validation issues found");
      }
    } catch (error) {
      // Don't fail the sync if validation fails
      this.logger.warn(`⚠️ Park validation failed (non-critical): ${error}`);
    }
  }

  @Process("fill-all-gaps")
  async handleFillAllGaps(_job: Job): Promise<void> {
    this.logger.log("🔄 Starting global schedule gap filling...");
    try {
      await this.parksService.fillAllParksGaps();
      this.logger.log("✅ Global schedule gap filling complete!");
    } catch (error) {
      this.logger.error("❌ Global schedule gap filling failed", error);
      throw error;
    }
  }

  /**
   * Process a park that exists in Wiki + others (or just QT+WZ)
   */
  private async processMatchedPark(
    wiki: ParkMetadata | undefined,
    qt: ParkMetadata | undefined,
    wz: ParkMetadata | undefined,
    confidence: number,
  ): Promise<void> {
    // Determine "Anchor" source (Wiki > QT > WZ)
    const anchor = wiki || qt || wz;
    if (!anchor) return; // Should not happen based on calling logic

    // Find or create destination (from wiki data if available)
    let destination = null;
    if (wiki?.destinationId) {
      destination = await this.destinationsService.findByExternalId(
        wiki.destinationId,
      );
    }

    // Check if park already exists (using Anchor's external ID)
    // NOTE: This assumes we use the Anchor's ID as the park's externalId
    // If we have Wiki, we use Wiki ID.
    // If no Wiki, we use QT ID (prefixed?) or WZ ID (prefixed?)
    // This aligns with processQueueTimesOnlyPark / processWartezeitenOnlyPark logic

    let parkExternalId = anchor.externalId;
    if (!wiki) {
      if (qt) parkExternalId = `qt-${qt.externalId}`;
      else if (wz) parkExternalId = `wz-${wz.externalId}`;
    }

    // CRITICAL: Check for existing park by multiple criteria to prevent duplicates
    // 1. Check by externalId (primary)
    let park = await this.parkRepository.findOne({
      where: { externalId: parkExternalId },
    });

    // 2. If not found, check by entity IDs (prevents duplicates when park was created with different externalId)
    // Example: Park created as QT-only with externalId="qt-123", later matched with Wiki
    // We need to find it by queueTimesEntityId to avoid creating a duplicate
    if (!park) {
      // Build query to find park by any matching entity ID
      const queryBuilder = this.parkRepository.createQueryBuilder("park");
      const conditions: string[] = [];
      const params: Record<string, any> = {};

      if (wiki?.externalId) {
        conditions.push("park.wikiEntityId = :wikiId");
        params.wikiId = wiki.externalId;
      }
      if (qt?.externalId) {
        conditions.push("park.queueTimesEntityId = :qtId");
        params.qtId = qt.externalId;
      }
      if (wz?.externalId) {
        conditions.push("park.wartezeitenEntityId = :wzId");
        params.wzId = wz.externalId;
      }

      if (conditions.length > 0) {
        queryBuilder.where(conditions.join(" OR "), params);
        park = await queryBuilder.getOne();

        if (park) {
          this.logger.warn(
            `⚠️ Found existing park "${park.name}" by entity ID (not externalId). ` +
              `This indicates a potential duplicate scenario. Updating instead of creating duplicate.`,
          );
        }
      }
    }

    // Helper to get best name
    // CRITICAL: Wiki ALWAYS has priority, even if other names are longer
    // Priority: Wiki (Source of Truth) > Longest Name (Fallback)
    let bestName = wiki?.name;

    if (!bestName) {
      const candidates: ParkMetadata[] = [];
      if (qt) candidates.push(qt);
      if (wz) candidates.push(wz);

      if (candidates.length > 0) {
        bestName = candidates.reduce((prev, curr) =>
          curr.name.length > prev.name.length ? curr : prev,
        ).name;
      }
    }

    // Collect Data Sources
    const dataSourceList: string[] = [];
    if (wiki) dataSourceList.push("themeparks-wiki");
    if (qt) dataSourceList.push("queue-times");
    if (wz) dataSourceList.push("wartezeiten-app");

    // Determine Primary Source
    const effectivePrimary =
      dataSourceList.length > 1 ? "multi-source" : dataSourceList[0];

    if (!bestName) return; // Safety check

    if (!park) {
      // Double-check: Verify no duplicate exists with same entity IDs
      // This prevents race conditions where park might have been created between checks
      const duplicateCheck = await this.parkRepository
        .createQueryBuilder("park")
        .where(
          "(park.wikiEntityId = :wikiId OR park.queueTimesEntityId = :qtId OR park.wartezeitenEntityId = :wzId)",
          {
            wikiId: wiki?.externalId || null,
            qtId: qt?.externalId || null,
            wzId: wz?.externalId || null,
          },
        )
        .getOne();

      if (duplicateCheck) {
        this.logger.warn(
          `⚠️ Duplicate park detected during creation: "${bestName}". ` +
            `Found existing park "${duplicateCheck.name}" with matching entity IDs. ` +
            `Updating existing park instead of creating duplicate.`,
        );
        park = duplicateCheck;
      } else {
        // Create park
        try {
          park = await this.parkRepository.save({
            externalId: parkExternalId,
            name: bestName,
            slug: generateSlug(bestName),
            destinationId: destination?.id || undefined,
            timezone: anchor.timezone || "UTC", // Default to UTC if missing to avoid DB constraint error
            latitude: anchor.latitude,
            longitude: anchor.longitude,
            continent: anchor.continent,
            continentSlug: anchor.continent
              ? generateSlug(anchor.continent)
              : undefined,
            country: anchor.country,
            countrySlug: anchor.country
              ? generateSlug(anchor.country)
              : undefined,
            primaryDataSource: effectivePrimary,
            dataSources: dataSourceList,
            wikiEntityId: wiki?.externalId || null,
            queueTimesEntityId: qt?.externalId || null,
            wartezeitenEntityId: wz?.externalId || null,
          });
          this.logger.verbose(
            `✓ Created matched park: ${park.name} (${park.id})`,
          );
        } catch (error: any) {
          // Handle race condition: Another process might have created the park
          if (
            error.code === "23505" ||
            error.message?.includes("duplicate key")
          ) {
            this.logger.warn(
              `Race condition detected: Park "${bestName}" was created by another process. ` +
                `Refetching and updating...`,
            );
            // Refetch by entity IDs
            park = await this.parkRepository
              .createQueryBuilder("park")
              .where(
                "(park.wikiEntityId = :wikiId OR park.queueTimesEntityId = :qtId OR park.wartezeitenEntityId = :wzId)",
                {
                  wikiId: wiki?.externalId || null,
                  qtId: qt?.externalId || null,
                  wzId: wz?.externalId || null,
                },
              )
              .getOne();

            if (!park) {
              // Still not found - try by externalId as last resort
              park = await this.parkRepository.findOne({
                where: { externalId: parkExternalId },
              });
            }

            if (!park) {
              this.logger.error(
                `Failed to find park after race condition for "${bestName}". Skipping.`,
              );
              return;
            }
          } else {
            throw error;
          }
        }
      }
    }

    // Ensure park exists at this point
    if (!park) {
      this.logger.error(
        `Failed to create or find park "${bestName}". This should not happen.`,
      );
      return;
    }

    // Update existing park - consolidate all entity IDs
    const needsUpdate =
      JSON.stringify(park.dataSources?.sort()) !==
        JSON.stringify(dataSourceList.sort()) ||
      park.primaryDataSource !== effectivePrimary ||
      (wiki && park.wikiEntityId !== wiki.externalId) ||
      (qt && park.queueTimesEntityId !== qt.externalId) ||
      (wz && park.wartezeitenEntityId !== wz.externalId) ||
      park.name !== bestName;

    if (needsUpdate) {
      park.dataSources = dataSourceList;
      park.primaryDataSource = effectivePrimary;
      if (wiki) park.wikiEntityId = wiki.externalId;
      if (qt) park.queueTimesEntityId = qt.externalId;
      if (wz) park.wartezeitenEntityId = wz.externalId;

      // Update name if changed
      // CRITICAL: Always use Wiki name if available, even if current name is longer
      // This ensures Wiki remains the source of truth for park names
      if (park.name !== bestName) {
        this.logger.verbose(
          `Updating park name: "${park.name}" -> "${bestName}" ` +
            `(Wiki priority: ${wiki ? "YES" : "NO"})`,
        );
        park.name = bestName;
        park.slug = generateSlug(bestName);
      }

      // Update externalId if it doesn't match the anchor (e.g., park was created with different ID)
      if (park.externalId !== parkExternalId) {
        this.logger.warn(
          `⚠️ Park "${park.name}" has mismatched externalId: ` +
            `"${park.externalId}" vs expected "${parkExternalId}". ` +
            `Updating to match anchor source.`,
        );
        park.externalId = parkExternalId;
      }

      await this.parkRepository.save(park);
    }

    // 1. Create Wiki mapping
    if (wiki) {
      await this.createMapping(
        park.id,
        "park",
        "themeparks-wiki",
        wiki.externalId,
        1.0,
        "exact",
      );
    }

    // 2. Create QT mapping
    if (qt) {
      await this.createMapping(
        park.id,
        "park",
        "queue-times",
        qt.externalId,
        confidence,
        "fuzzy",
      );
    }

    // 3. Create WZ mapping
    if (wz) {
      await this.createMapping(
        park.id,
        "park",
        "wartezeiten-app",
        wz.externalId,
        confidence,
        "fuzzy",
      );
    }
  }

  /**
   * Process a park that exists ONLY in ThemeParks.wiki
   */
  private async processWikiOnlyPark(wiki: ParkMetadata): Promise<void> {
    // Check if park already exists (Try ID first, then fallback to Slug for self-healing)
    let park = await this.findParkWithFallback(
      wiki.externalId,
      wiki.name,
      "themeparks-wiki",
    );

    // Also check by wikiEntityId to prevent duplicates
    // (e.g., if park was created as matched but later became Wiki-only)
    if (!park && wiki.externalId) {
      park = await this.parkRepository.findOne({
        where: { wikiEntityId: wiki.externalId },
      });

      if (park) {
        this.logger.warn(
          `⚠️ Found existing park "${park.name}" by wikiEntityId. ` +
            `Updating externalId to match Wiki format.`,
        );
        // Update externalId to Wiki format for consistency
        park.externalId = wiki.externalId;
      }
    }

    if (park) {
      const needsUpdate = !park.wikiEntityId || park.name !== wiki.name;

      if (!park.wikiEntityId) {
        park.wikiEntityId = wiki.externalId;
        park.queueTimesEntityId = null;
      }

      // CRITICAL: Always update name to Wiki name (Wiki has priority)
      if (park.name !== wiki.name) {
        this.logger.verbose(
          `Updating park name to Wiki: "${park.name}" -> "${wiki.name}"`,
        );
        park.name = wiki.name;
        park.slug = generateSlug(wiki.name);
      }

      if (needsUpdate) {
        await this.parkRepository.save(park);
        this.logger.debug(
          `✓ Backfilled columns for wiki-only park: ${park.name}`,
        );
      }
      return;
    }

    let destination = null;
    if (wiki.destinationId) {
      destination = await this.destinationsService.findByExternalId(
        wiki.destinationId,
      );
    }

    park = await this.parkRepository.save({
      externalId: wiki.externalId,
      name: wiki.name,
      slug: generateSlug(wiki.name),
      destinationId: destination?.id || undefined,
      timezone: wiki.timezone,
      latitude: wiki.latitude,
      longitude: wiki.longitude,
      continent: wiki.continent,
      continentSlug: wiki.continent ? generateSlug(wiki.continent) : undefined,
      country: wiki.country,
      countrySlug: wiki.country ? generateSlug(wiki.country) : undefined,
      primaryDataSource: "themeparks-wiki",
      dataSources: ["themeparks-wiki"],
      wikiEntityId: wiki.externalId,
      queueTimesEntityId: null,
    });

    await this.createMapping(
      park.id,
      "park",
      "themeparks-wiki",
      wiki.externalId,
      1.0,
      "exact",
    );

    this.logger.debug(`✓ Created wiki-only park: ${park.name}`);
  }

  /**
   * Determine a best-guess timezone based on country code
   */
  private getTimezoneForCountry(countryCode: string | undefined): string {
    if (!countryCode) return "UTC";

    const normalizedCode = countryCode.toUpperCase();
    const timezoneMap: Record<string, string> = {
      DE: "Europe/Berlin",
      FR: "Europe/Paris",
      ES: "Europe/Madrid",
      IT: "Europe/Rome",
      NL: "Europe/Amsterdam",
      BE: "Europe/Brussels",
      DK: "Europe/Copenhagen",
      SE: "Europe/Stockholm",
      GB: "Europe/London",
      UK: "Europe/London",
      IE: "Europe/Dublin",
      US: "America/New_York", // Default for US, hard to guess better without state
      CA: "America/Toronto",
      JP: "Asia/Tokyo",
      CN: "Asia/Shanghai",
      HK: "Asia/Hong_Kong",
      SG: "Asia/Singapore",
      AE: "Asia/Dubai",
      AU: "Australia/Sydney",
    };

    return timezoneMap[normalizedCode] || "UTC";
  }

  /**
   * Process a park that exists ONLY in Queue-Times
   */
  private async processQueueTimesOnlyPark(qt: ParkMetadata): Promise<void> {
    const qtExternalId = `qt-${qt.externalId}`; // Prefix to avoid collision

    // Check if park already exists by externalId
    let park = await this.parkRepository.findOne({
      where: { externalId: qtExternalId },
    });

    // Also check by queueTimesEntityId to prevent duplicates
    // (e.g., if park was created as matched but later became QT-only)
    if (!park && qt.externalId) {
      park = await this.parkRepository.findOne({
        where: { queueTimesEntityId: qt.externalId },
      });

      if (park) {
        this.logger.warn(
          `⚠️ Found existing park "${park.name}" by queueTimesEntityId. ` +
            `Updating externalId to match QT-only format.`,
        );
        // Update externalId to QT format for consistency
        park.externalId = qtExternalId;
      }
    }

    if (park) {
      // If park has Wiki ID, preserve Wiki name (Wiki has priority)
      // Only update if Wiki ID is missing (QT-only park)
      if (!park.queueTimesEntityId) {
        park.queueTimesEntityId = qt.externalId;
        // Only set wikiEntityId to null if it's truly a QT-only park
        // If park has wikiEntityId, keep it and don't overwrite with null
        if (!park.wikiEntityId) {
          park.wikiEntityId = null;
        }
        // If park has Wiki ID, don't update name (Wiki name has priority)
        if (!park.wikiEntityId && park.name !== qt.name) {
          park.name = qt.name;
          park.slug = generateSlug(qt.name);
        }
        try {
          await this.parkRepository.save(park);
        } catch (error: any) {
          if (error.message && error.message.includes("duplicate key")) {
            return;
          }
          throw error;
        }
      }
      return;
    }

    park = await this.parkRepository.save({
      externalId: qtExternalId,
      name: qt.name,
      slug: generateSlug(qt.name),
      timezone: qt.timezone || this.getTimezoneForCountry(qt.country),
      latitude: qt.latitude,
      longitude: qt.longitude,
      continent: qt.continent,
      continentSlug: qt.continent ? generateSlug(qt.continent) : undefined,
      country: qt.country,
      countrySlug: qt.country ? generateSlug(qt.country) : undefined,
      primaryDataSource: "queue-times",
      dataSources: ["queue-times"],
      wikiEntityId: null,
      queueTimesEntityId: qt.externalId,
    });

    await this.createMapping(
      park.id,
      "park",
      "queue-times",
      qt.externalId,
      1.0,
      "exact",
    );

    this.logger.debug(`✓ Created qt-only park: ${park.name}`);
  }

  /**
   * Process a park that exists ONLY in Wartezeiten.app
   * (Allowed only for specific parks)
   */
  private async processWartezeitenOnlyPark(wz: ParkMetadata): Promise<void> {
    const wzExternalId = `wz-${wz.externalId}`; // Prefix to avoid collision

    // Check if park already exists by externalId
    let park = await this.parkRepository.findOne({
      where: { externalId: wzExternalId },
    });

    // Also check by wartezeitenEntityId to prevent duplicates
    // (e.g., if park was created as matched but later became WZ-only)
    if (!park && wz.externalId) {
      park = await this.parkRepository.findOne({
        where: { wartezeitenEntityId: wz.externalId },
      });

      if (park) {
        this.logger.warn(
          `⚠️ Found existing park "${park.name}" by wartezeitenEntityId. ` +
            `Updating externalId to match WZ-only format.`,
        );
        // Update externalId to WZ format for consistency
        park.externalId = wzExternalId;
      }
    }

    if (park) {
      // If park has Wiki ID, preserve Wiki name (Wiki has priority)
      // Only update name if park doesn't have Wiki ID
      const cleanedWzName = wz.name.replace(/\s*\([A-Z]{2}\)$/, "").trim();
      const shouldUpdateName =
        !park.wikiEntityId && park.name !== cleanedWzName;

      if (!park.wartezeitenEntityId) {
        park.wartezeitenEntityId = wz.externalId;

        // Only update name if park doesn't have Wiki ID (Wiki name has priority)
        if (shouldUpdateName) {
          park.name = cleanedWzName;
          park.slug = generateSlug(cleanedWzName);
        }

        try {
          await this.parkRepository.save(park);
        } catch (error: any) {
          if (error.message && error.message.includes("duplicate key")) {
            return;
          }
          throw error;
        }
      }
      return;
    }

    // Determine Timezone & Coordinates from Whitelist/Config
    const cleanedName = wz.name.replace(/\s*\([A-Z]{2}\)$/, "").trim();
    const config =
      WARTEZEITEN_CREATION_WHITELIST[cleanedName] ||
      WARTEZEITEN_CREATION_WHITELIST[wz.name];

    let timezone =
      config?.timezone ||
      wz.timezone ||
      (wz.country ? this.getTimezoneForCountry(wz.country) : undefined);
    let latitude = config?.latitude;
    let longitude = config?.longitude;

    // Clean name for the actual park entity (prioritize overrideName from config if present)
    const name = config?.overrideName || cleanedName;

    park = await this.parkRepository.save({
      externalId: wzExternalId,
      name: name,
      slug: generateSlug(name),
      timezone: timezone || "UTC",
      latitude: latitude,
      longitude: longitude,
      country: wz.country,
      countrySlug: wz.country ? generateSlug(wz.country) : undefined,
      primaryDataSource: "wartezeiten-app",
      dataSources: ["wartezeiten-app"],
      wikiEntityId: null,
      queueTimesEntityId: null,
      wartezeitenEntityId: wz.externalId,
    });

    await this.createMapping(
      park.id,
      "park",
      "wartezeiten-app",
      wz.externalId,
      1.0,
      "exact",
    );

    this.logger.debug(
      `✓ Created wz-only park: ${park.name} (with hardcoded Geo: ${!!latitude})`,
    );
  }

  /**
   * Create an external entity mapping
   * Automatically resolves conflicts by updating stale mappings
   */
  private async createMapping(
    internalEntityId: string,
    internalEntityType: "park" | "attraction" | "show" | "restaurant",
    externalSource: string,
    externalEntityId: string,
    matchConfidence: number,
    matchMethod: "exact" | "fuzzy" | "manual" | "geographic",
  ): Promise<void> {
    // Check if mapping already exists for this external entity
    const existing = await this.mappingRepository.findOne({
      where: {
        externalSource,
        externalEntityId,
      },
    });

    if (existing) {
      // Check if it points to the correct internal entity
      if (existing.internalEntityId === internalEntityId) {
        // this.logger.verbose(
        //   `Mapping already exists: ${externalSource}:${externalEntityId}`,
        // );
        return;
      }

      // CONFLICT: External ID is mapped to a different internal entity
      // This happens when park IDs rotate or parks merge/split
      this.logger.warn(
        `⚠️ Mapping conflict detected: ${externalSource}:${externalEntityId} ` +
          `currently maps to ${existing.internalEntityId} but should map to ${internalEntityId}. ` +
          `Updating mapping...`,
      );

      // Update the existing mapping to point to the correct entity
      existing.internalEntityId = internalEntityId;
      existing.internalEntityType = internalEntityType;
      existing.matchConfidence = matchConfidence;
      existing.matchMethod = matchMethod;
      existing.verified = matchMethod === "exact" || matchMethod === "manual";

      await this.mappingRepository.save(existing);
      this.logger.verbose(
        `✅ Resolved mapping conflict: ${externalSource}:${externalEntityId} now correctly maps to ${internalEntityId}`,
      );
      return;
    }

    // No conflict, create new mapping
    await this.mappingRepository.save({
      internalEntityId,
      internalEntityType,
      externalSource,
      externalEntityId,
      matchConfidence,
      matchMethod,
      verified: matchMethod === "exact" || matchMethod === "manual",
    });
  }

  /**
   * Sync schedules for all parks (Wiki only)
   */
  private async syncSchedulesForAllParks(): Promise<void> {
    this.logger.log("📅 Syncing schedules...");
    const parks = await this.parkRepository.find();
    let totalScheduleEntries = 0;

    for (const park of parks) {
      // Only sync schedules for parks with Wiki data
      if (!park.dataSources || !park.dataSources.includes("themeparks-wiki")) {
        continue;
      }

      try {
        // Try to get Wiki external ID from park entity first (faster)
        let wikiExternalId = park.wikiEntityId;

        // Fallback to mapping if wikiEntityId is not set
        if (!wikiExternalId) {
          const mapping = await this.mappingRepository.findOne({
            where: {
              internalEntityId: park.id,
              internalEntityType: "park",
              externalSource: "themeparks-wiki",
            },
          });

          if (!mapping) {
            this.logger.warn(
              `No Wiki ID found for park ${park.name}, skipping schedule sync`,
            );
            continue;
          }

          wikiExternalId = mapping.externalEntityId;
        }

        const scheduleResponse =
          await this.themeParksClient.getSchedule(wikiExternalId);
        const savedEntries = await this.parksService.saveScheduleData(
          park.id,
          scheduleResponse.schedule,
        );
        totalScheduleEntries += savedEntries;

        // Fill gaps for Holidays/Bridge Days
        await this.parksService.fillScheduleGaps(park.id);
      } catch (error) {
        this.logger.error(`Failed to sync schedule for ${park.name}: ${error}`);
      }
    }

    this.logger.log(`✅ Synced ${totalScheduleEntries} schedule entries`);
  }

  /**
   * Geocode parks that need geographic data
   */
  private async geocodeParks(): Promise<void> {
    const parksWithoutGeodata =
      await this.parksService.findParksWithoutGeodata();

    if (parksWithoutGeodata.length === 0) {
      this.logger.log("✅ All parks already have geocoding data");
      return;
    }

    this.logger.log(`🌍 Geocoding ${parksWithoutGeodata.length} parks...`);
    let geocodedCount = 0;

    for (const park of parksWithoutGeodata) {
      // Initialize an object to hold updates for the park
      const updates: Partial<Park> = {};
      let needsUpdate = false;

      // 3. Reverse Geocoding (if coordinates exist)
      if (park.latitude && park.longitude) {
        // Smart Check: If we have coordinates but no region data, we should try to geocode again
        // regardless of geocodingAttemptedAt, because we might have cached data without regions.
        // The GoogleGeocodingClient handles the smart caching (fetch only if missing in cache).
        const missingRegionData = !park.region || !park.regionCode;

        const shouldGeocode =
          !park.city ||
          !park.country ||
          !park.geocodingAttemptedAt ||
          missingRegionData; // Retry if we are missing regional data

        if (shouldGeocode) {
          try {
            const geoData = await this.geocodingClient.reverseGeocode(
              Number(park.latitude),
              Number(park.longitude),
            );

            if (geoData) {
              updates.city = geoData.city;
              updates.citySlug = generateSlug(geoData.city);
              updates.country = geoData.country;
              updates.countrySlug = geoData.country
                ? generateSlug(geoData.country)
                : undefined;
              updates.countryCode = geoData.countryCode; // Now available directly from geocoding
              updates.continent = geoData.continent;
              updates.continentSlug = geoData.continent
                ? generateSlug(geoData.continent)
                : undefined;
              if (geoData.region) updates.region = geoData.region;
              if (geoData.regionCode) {
                updates.regionCode = geoData.regionCode.substring(0, 50);
              }

              // Reset retry count only if we got the most important fields back
              // (regionCode might be missing for some countries, but countryCode and city are usually there)
              if (geoData.countryCode && geoData.city) {
                updates.metadataRetryCount = 0;
              }

              updates.geocodingAttemptedAt = new Date();
              needsUpdate = true;
              this.logger.verbose(
                `Geocoded ${park.name}: ${geoData.city}, ${geoData.regionCode || geoData.region || ""}, ${geoData.country} (${geoData.continent})`,
              );
            } else {
              // Mark as attempted even if failed to avoid constant retries (unless smart retry logic overrides)
              if (!missingRegionData) {
                updates.geocodingAttemptedAt = new Date();
                needsUpdate = true;
              }
            }
          } catch (error: any) {
            this.logger.warn(
              `Geocoding failed for ${park.name}: ${error.message}`,
            );
            // Mark as attempted even if failed to avoid constant retries (unless smart retry logic overrides)
            if (!missingRegionData) {
              updates.geocodingAttemptedAt = new Date();
              needsUpdate = true;
            }
          }
        }
      }

      if (needsUpdate) {
        await this.parksService.updateGeodata(park.id, updates);
        geocodedCount++;
      } else if (!park.geocodingAttemptedAt) {
        // If no geocoding was performed and it was never attempted, mark it as attempted
        await this.parksService.markGeocodingAttempted(park.id);
      }

      // Rate limiting (100ms delay)
      await this.sleep(100);
    }

    this.logger.log(
      `✅ Geocoded ${geocodedCount}/${parksWithoutGeodata.length} parks`,
    );
  }

  /**
   * Find a park by External ID, with fallback to Slug for self-healing
   * If found by slug but ID differs, it updates the ID (Self-Healing)
   */
  private async findParkWithFallback(
    externalId: string,
    name: string,
    source: string,
  ): Promise<Park | null> {
    // 1. Try finding by External ID (Fastest, Standard)
    let park = await this.parkRepository.findOne({
      where: { externalId },
    });

    if (park) {
      return park;
    }

    // 2. Fallback: Try finding by Slug (Self-Healing for ID rotation)
    // Only do this if the name is reasonably long to avoid false positives on overly generic names
    if (name.length < 5) return null;

    const slug = generateSlug(name);
    park = await this.parkRepository.findOne({
      where: { slug },
    });

    if (park) {
      // 3. We found the park, but the ID has changed!
      this.logger.warn(
        `⚠️ Park ID rotation detected for "${park.name}" (${source}). Updating: ${park.externalId} -> ${externalId}`,
      );

      // Verify this isn't a collision with another valid park (sanity check)
      // (The unique constraint on externalId would catch this on save, but good to check)

      park.externalId = externalId;
      if (source === "themeparks-wiki") {
        park.wikiEntityId = externalId;
      } else if (source === "queue-times") {
        park.queueTimesEntityId = externalId;
      }

      await this.parkRepository.save(park);

      // We must also update the mapping immediately to prevent unique constraint errors later
      await this.updateMapping(park.id, source, externalId);
    }

    return park;
  }

  /**
   * Update existing mapping for ID rotation
   */
  private async updateMapping(
    internalEntityId: string,
    externalSource: string,
    newExternalId: string,
  ): Promise<void> {
    // Find any existing mapping for this internal entity + source
    const existing = await this.mappingRepository.findOne({
      where: {
        internalEntityId,
        externalSource,
        internalEntityType: "park",
      },
    });

    if (existing) {
      this.logger.log(
        `🔄 Updating mapping for ${internalEntityId} [${externalSource}]: ${existing.externalEntityId} -> ${newExternalId}`,
      );
      // Update the external ID
      await this.mappingRepository.update(existing.id, {
        externalEntityId: newExternalId,
      });
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
