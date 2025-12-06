/**
 * Google Geocoding API Types
 *
 * Documentation: https://developers.google.com/maps/documentation/geocoding/requests-reverse-geocoding
 */

/**
 * Address component from Google Geocoding API
 */
export interface AddressComponent {
  long_name: string;
  short_name: string;
  types: string[];
}

/**
 * Geometry information
 */
export interface Geometry {
  location: {
    lat: number;
    lng: number;
  };
  location_type: string;
  viewport: {
    northeast: { lat: number; lng: number };
    southwest: { lat: number; lng: number };
  };
}

/**
 * Single result from Google Geocoding API
 */
export interface GeocodingResult {
  address_components: AddressComponent[];
  formatted_address: string;
  geometry: Geometry;
  place_id: string;
  types: string[];
}

/**
 * Full response from Google Geocoding API
 */
export interface GoogleGeocodingResponse {
  results: GeocodingResult[];
  status: string;
  error_message?: string;
}

/**
 * Simplified geocoding data for our needs
 */
export interface GeographicData {
  continent: string;
  country: string;
  city: string;
}
