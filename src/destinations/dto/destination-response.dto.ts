import { Destination } from "../entities/destination.entity";

/**
 * Destination Response DTO
 *
 * Used for API responses when returning destination data.
 */
export class DestinationResponseDto {
  id: string;
  name: string;
  slug: string;
  createdAt: Date;
  updatedAt: Date;

  static fromEntity(destination: Destination): DestinationResponseDto {
    return {
      id: destination.id,
      name: destination.name,
      slug: destination.slug,
      createdAt: destination.createdAt,
      updatedAt: destination.updatedAt,
    };
  }
}
