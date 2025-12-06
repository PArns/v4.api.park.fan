import { Module } from "@nestjs/common";
import { GoogleGeocodingClient } from "./google-geocoding.client";

/**
 * Geocoding Module
 *
 * Provides reverse geocoding services using Google Geocoding API.
 * Used to enrich parks with continent, country, and city data.
 */
@Module({
  providers: [GoogleGeocodingClient],
  exports: [GoogleGeocodingClient],
})
export class GeocodingModule {}
