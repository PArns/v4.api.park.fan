import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Destination } from "./entities/destination.entity";
import { ThemeParksClient } from "../external-apis/themeparks/themeparks.client";
import { ThemeParksMapper } from "../external-apis/themeparks/themeparks.mapper";
import { generateSlug, generateUniqueSlug } from "../common/utils/slug.util";

@Injectable()
export class DestinationsService {
  private readonly logger = new Logger(DestinationsService.name);

  constructor(
    @InjectRepository(Destination)
    private destinationRepository: Repository<Destination>,
    private themeParksClient: ThemeParksClient,
    private themeParksMapper: ThemeParksMapper,
  ) {}

  /**
   * Fetches all destinations from ThemeParks.wiki and saves to DB
   */
  async syncDestinations(): Promise<number> {
    this.logger.log("Syncing destinations from ThemeParks.wiki...");

    const apiResponse = await this.themeParksClient.getDestinations();

    // 1. Fetch all existing destinations to avoid N+1 queries
    const existingDestinations = await this.destinationRepository.find();
    const existingMap = new Map<string, Destination>(
      existingDestinations.map((d) => [d.externalId, d]),
    );
    const existingSlugs = existingDestinations.map((d) => d.slug);

    const toUpdate: Destination[] = [];
    const toInsert: Partial<Destination>[] = [];

    for (const apiDestination of apiResponse.destinations) {
      const mappedData = this.themeParksMapper.mapDestination(apiDestination);

      const existing = existingMap.get(mappedData.externalId!);

      if (existing) {
        // Update existing destination if name changed (keep existing slug)
        if (existing.name !== mappedData.name) {
          existing.name = mappedData.name!;
          toUpdate.push(existing);
        }
      } else {
        // Generate unique slug
        const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

        // Generate unique slug (append -2, -3, etc. if needed)
        const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
        mappedData.slug = uniqueSlug;

        // Add to insert list and track the new slug
        toInsert.push(mappedData);
        existingSlugs.push(uniqueSlug);
      }
    }

    // 2. Perform bulk updates if any
    if (toUpdate.length > 0) {
      this.logger.log(`Updating ${toUpdate.length} destinations...`);
      await this.destinationRepository.save(toUpdate);
    }

    // 3. Perform bulk inserts if any
    if (toInsert.length > 0) {
      this.logger.log(`Inserting ${toInsert.length} new destinations...`);
      // Use query builder for ON CONFLICT DO NOTHING to prevent race conditions
      await this.destinationRepository
        .createQueryBuilder()
        .insert()
        .into(Destination)
        .values(toInsert)
        .orIgnore() // PostgreSQL: ON CONFLICT DO NOTHING
        .execute();
    }

    const syncedCount = apiResponse.destinations.length;
    this.logger.log(`✅ Synced ${syncedCount} destinations`);
    return syncedCount;
  }

  /**
   * Finds destination by externalId
   */
  async findByExternalId(externalId: string): Promise<Destination | null> {
    return this.destinationRepository.findOne({
      where: { externalId },
      relations: ["parks"],
    });
  }

  /**
   * Finds all destinations
   */
  /**
   * Finds all destinations with pagination
   */
  async findAll(
    page: number = 1,
    limit: number = 10,
  ): Promise<{ data: Destination[]; total: number }> {
    const [data, total] = await this.destinationRepository.findAndCount({
      relations: ["parks"],
      order: { name: "ASC" },
      take: limit,
      skip: (page - 1) * limit,
    });

    return { data, total };
  }

  /**
   * Finds destination by slug
   */
  async findBySlug(slug: string): Promise<Destination | null> {
    return this.destinationRepository.findOne({
      where: { slug },
      relations: ["parks"],
    });
  }
}
