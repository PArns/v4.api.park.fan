import { Destination } from "../entities/destination.entity";

/**
 * Destination Response DTO
 *
 * Used for API responses when returning destination data.
 */
export class DestinationResponseDto {
  id: string;
  externalId: string;
  slug: string;

  static fromEntity(destination: Destination): DestinationResponseDto {
    return {
      id: destination.id,
      externalId: destination.externalId,
      slug: destination.slug,
    };
  }
}
