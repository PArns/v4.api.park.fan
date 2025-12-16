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
  externalId: string;
  parks: {
    id: string;
    name: string;
    slug: string;
    externalId: string;
    timezone: string;
  }[];

  static fromEntity(destination: Destination): DestinationWithParksDto {
    return {
      id: destination.id,
      name: destination.name,
      slug: destination.slug,
      externalId: destination.externalId,
      parks: destination.parks
        ? destination.parks.map((park) => ({
            id: park.id,
            name: park.name,
            slug: park.slug,
            externalId: park.externalId,
            timezone: park.timezone,
          }))
        : [],
    };
  }
}
