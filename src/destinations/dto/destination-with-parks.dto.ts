import { Destination } from "../entities/destination.entity";

/**
 * Destination with Parks Response DTO
 *
 * Used when returning destination data with its parks included.
 */
export class DestinationWithParksDto {
  id: string;
  name: string;
  slug: string;
  parks: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
  }[];
  createdAt: Date;
  updatedAt: Date;

  static fromEntity(destination: Destination): DestinationWithParksDto {
    return {
      id: destination.id,
      name: destination.name,
      slug: destination.slug,
      parks: destination.parks
        ? destination.parks.map((park) => ({
            id: park.id,
            name: park.name,
            slug: park.slug,
            timezone: park.timezone,
          }))
        : [],
      createdAt: destination.createdAt,
      updatedAt: destination.updatedAt,
    };
  }
}
