/**
 * Nager.Date API Types
 *
 * API Documentation: https://date.nager.at/Api
 *
 * Free public holiday API supporting 100+ countries.
 * No API key required, no rate limits.
 */

/**
 * Public Holiday Response
 *
 * GET /api/v3/PublicHolidays/{year}/{countryCode}
 */
export interface NagerPublicHoliday {
  /**
   * Holiday date (ISO 8601 format: YYYY-MM-DD)
   */
  date: string;

  /**
   * Local holiday name (in country's language)
   */
  localName: string;

  /**
   * English holiday name
   */
  name: string;

  /**
   * ISO 3166-1 alpha-2 country code
   */
  countryCode: string;

  /**
   * Is this holiday fixed (same date every year)?
   */
  fixed: boolean;

  /**
   * Is this a global/nationwide holiday?
   * False for regional holidays
   */
  global: boolean;

  /**
   * ISO 3166-2 region codes (if regional holiday)
   * Example: ["US-CA", "US-NY"] for holidays only in California and New York
   */
  counties: string[] | null;

  /**
   * Launch year of this holiday (when it started being observed)
   */
  launchYear: number | null;

  /**
   * Holiday types
   * - Public: Official public holiday
   * - Bank: Bank holiday
   * - School: School holiday
   * - Authorities: Government offices closed
   * - Optional: Optional holiday
   * - Observance: Observed but not official
   */
  types: HolidayType[];
}

export enum HolidayType {
  PUBLIC = "Public",
  BANK = "Bank",
  SCHOOL = "School",
  AUTHORITIES = "Authorities",
  OPTIONAL = "Optional",
  OBSERVANCE = "Observance",
}

/**
 * Country Info Response
 *
 * GET /api/v3/AvailableCountries
 */
export interface NagerCountryInfo {
  /**
   * ISO 3166-1 alpha-2 country code
   */
  countryCode: string;

  /**
   * Country name in English
   */
  name: string;
}

/**
 * Long Weekend Response
 *
 * GET /api/v3/LongWeekend/{year}/{countryCode}
 */
export interface NagerLongWeekend {
  /**
   * Start date of long weekend
   */
  startDate: string;

  /**
   * End date of long weekend
   */
  endDate: string;

  /**
   * Number of days in long weekend
   */
  dayCount: number;

  /**
   * Whether this requires a bridge day (taking day off between holiday and weekend)
   */
  needBridgeDay: boolean;
}
