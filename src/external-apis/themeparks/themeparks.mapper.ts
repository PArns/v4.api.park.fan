import { Injectable } from "@nestjs/common";
import {
  DestinationResponse,
  EntityResponse,
  EntityType,
} from "./themeparks.types";
import { Destination } from "../../destinations/entities/destination.entity";
import { Park } from "../../parks/entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { Show } from "../../shows/entities/show.entity";
import { Restaurant } from "../../restaurants/entities/restaurant.entity";
import { generateSlug } from "../../common/utils/slug.util";

/**
 * ThemeParks.wiki Data Mapper
 *
 * Maps API responses to our domain entities.
 *
 * Field Mappings:
 * API → Entity
 * - id → externalId
 * - name → name
 * - slug → ALWAYS generated from name (for SEO-friendly hyphens)
 * - location.latitude → latitude
 * - location.longitude → longitude
 *
 * Note: We ignore API slugs (e.g., "disneylandparis") and always generate our own
 * (e.g., "disneyland-paris") for better SEO and consistency.
 */
@Injectable()
export class ThemeParksMapper {
  /**
   * Maps DestinationResponse to Destination entity
   */
  mapDestination(apiData: DestinationResponse): Partial<Destination> {
    return {
      externalId: apiData.id,
      name: apiData.name,
      slug: generateSlug(apiData.name),
    };
  }

  /**
   * Maps EntityResponse (type PARK) to Park entity
   */
  mapPark(apiData: EntityResponse, destinationId: string): Partial<Park> {
    if (apiData.entityType !== EntityType.PARK) {
      throw new Error(`Expected PARK entity, got ${apiData.entityType}`);
    }

    return {
      externalId: apiData.id,
      name: apiData.name,
      slug: generateSlug(apiData.name),
      destinationId,
      latitude:
        apiData.location?.latitude !== null
          ? apiData.location?.latitude
          : undefined,
      longitude:
        apiData.location?.longitude !== null
          ? apiData.location?.longitude
          : undefined,
      timezone: apiData.timezone || "UTC",
    };
  }

  /**
   * Maps EntityResponse (type ATTRACTION) to Attraction entity
   */
  mapAttraction(apiData: EntityResponse, parkId: string): Partial<Attraction> {
    if (apiData.entityType !== EntityType.ATTRACTION) {
      throw new Error(`Expected ATTRACTION entity, got ${apiData.entityType}`);
    }

    return {
      externalId: apiData.id,
      name: apiData.name,
      slug: generateSlug(apiData.name),
      parkId,
      latitude:
        apiData.location?.latitude !== null
          ? apiData.location?.latitude
          : undefined,
      longitude:
        apiData.location?.longitude !== null
          ? apiData.location?.longitude
          : undefined,
      attractionType: apiData.attractionType,
    };
  }

  /**
   * Maps EntityResponse (type SHOW) to Show entity
   */
  mapShow(apiData: EntityResponse, parkId: string): Partial<Show> {
    if (apiData.entityType !== EntityType.SHOW) {
      throw new Error(`Expected SHOW entity, got ${apiData.entityType}`);
    }

    return {
      externalId: apiData.id,
      name: apiData.name,
      slug: generateSlug(apiData.name),
      parkId,
      latitude:
        apiData.location?.latitude !== null
          ? apiData.location?.latitude
          : undefined,
      longitude:
        apiData.location?.longitude !== null
          ? apiData.location?.longitude
          : undefined,
      // Show-specific fields
    };
  }

  /**
   * Maps EntityResponse (type RESTAURANT) to Restaurant entity
   */
  mapRestaurant(apiData: EntityResponse, parkId: string): Partial<Restaurant> {
    if (apiData.entityType !== EntityType.RESTAURANT) {
      throw new Error(`Expected RESTAURANT entity, got ${apiData.entityType}`);
    }

    return {
      externalId: apiData.id,
      name: apiData.name,
      slug: generateSlug(apiData.name),
      parkId,
      latitude:
        apiData.location?.latitude !== null
          ? apiData.location?.latitude
          : undefined,
      longitude:
        apiData.location?.longitude !== null
          ? apiData.location?.longitude
          : undefined,
      // Restaurant-specific fields
      cuisineType: apiData.cuisines ? apiData.cuisines.join(", ") : undefined,
      cuisines: apiData.cuisines ? apiData.cuisines : undefined,
      requiresReservation: false, // Default to false
    };
  }
}
