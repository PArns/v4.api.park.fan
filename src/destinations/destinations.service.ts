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
    let syncedCount = 0;

    for (const apiDestination of apiResponse.destinations) {
      const mappedData = this.themeParksMapper.mapDestination(apiDestination);

      // Check if destination exists (by externalId)
      const existing = await this.destinationRepository.findOne({
        where: { externalId: mappedData.externalId },
      });

      if (existing) {
        // Update existing destination (keep existing slug)
        await this.destinationRepository.update(existing.id, {
          name: mappedData.name,
        });
      } else {
        // Generate unique slug
        const baseSlug = mappedData.slug || generateSlug(mappedData.name!);

        // Get all existing slugs
        const existingDestinations = await this.destinationRepository.find({
          select: ["slug"],
        });
        const existingSlugs = existingDestinations.map((d) => d.slug);

        // Generate unique slug (append -2, -3, etc. if needed)
        const uniqueSlug = generateUniqueSlug(baseSlug, existingSlugs);
        mappedData.slug = uniqueSlug;

        // Insert new destination
        try {
          await this.destinationRepository.save(mappedData);
        } catch (error: unknown) {
          // Handle race condition where another process created the destination
          if (
            error instanceof Error &&
            error.message.includes("duplicate key")
          ) {
            this.logger.warn(
              `Destination ${mappedData.name} already exists (race condition), skipping`,
            );
            // Already exists, skip this iteration
            continue;
          } else {
            throw error;
          }
        }
      }

      syncedCount++;
    }

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
  async findAll(): Promise<Destination[]> {
    return this.destinationRepository.find({
      relations: ["parks"],
      order: { name: "ASC" },
    });
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
