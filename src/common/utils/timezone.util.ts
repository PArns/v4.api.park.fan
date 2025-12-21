import { DateTime } from "luxon";

/**
 * Country to Timezone mapping
 * Maps country ISO codes to their primary timezone
 */
export const COUNTRY_TIMEZONES: Record<string, string> = {
  US: "America/New_York",
  DE: "Europe/Berlin",
  FR: "Europe/Paris",
  GB: "Europe/London",
  ES: "Europe/Madrid",
  IT: "Europe/Rome",
  NL: "Europe/Amsterdam",
  BE: "Europe/Brussels",
  JP: "Asia/Tokyo",
  CN: "Asia/Shanghai",
  KR: "Asia/Seoul",
  AU: "Australia/Sydney",
  CA: "America/Toronto",
  MX: "America/Mexico_City",
  BR: "America/Sao_Paulo",
  AE: "Asia/Dubai",
  SA: "Asia/Riyadh",
  SG: "Asia/Singapore",
  TH: "Asia/Bangkok",
  MY: "Asia/Kuala_Lumpur",
  ID: "Asia/Jakarta",
  PH: "Asia/Manila",
  VN: "Asia/Ho_Chi_Minh",
  IN: "Asia/Kolkata",
  TR: "Europe/Istanbul",
  RU: "Europe/Moscow",
  PL: "Europe/Warsaw",
  AT: "Europe/Vienna",
  CH: "Europe/Zurich",
  SE: "Europe/Stockholm",
  NO: "Europe/Oslo",
  DK: "Europe/Copenhagen",
  FI: "Europe/Helsinki",
  PT: "Europe/Lisbon",
  GR: "Europe/Athens",
  CZ: "Europe/Prague",
  HU: "Europe/Budapest",
  RO: "Europe/Bucharest",
  BG: "Europe/Sofia",
  HR: "Europe/Zagreb",
  RS: "Europe/Belgrade",
  UA: "Europe/Kiev",
  IL: "Asia/Jerusalem",
  EG: "Africa/Cairo",
  ZA: "Africa/Johannesburg",
  MA: "Africa/Casablanca",
  TN: "Africa/Tunis",
  AR: "America/Argentina/Buenos_Aires",
  CL: "America/Santiago",
  CO: "America/Bogota",
  PE: "America/Lima",
  NZ: "Pacific/Auckland",
  HK: "Asia/Hong_Kong",
  IE: "Europe/Dublin",
  SK: "Europe/Bratislava",
  SI: "Europe/Ljubljana",
  TW: "Asia/Taipei",
  QA: "Asia/Qatar",
  BH: "Asia/Bahrain",
  KW: "Asia/Kuwait",
  OM: "Asia/Muscat",
  JO: "Asia/Amman",
  UY: "America/Montevideo",
};

/**
 * Convert a date string to a Date object at midnight in a specific timezone
 *
 * The Nager.Date API returns dates as "YYYY-MM-DD" strings representing local dates.
 * We need to store them as UTC Date objects that represent midnight in the local timezone.
 *
 * @param dateString - Date string in format "YYYY-MM-DD" (e.g., "2025-12-25")
 * @param timezone - IANA timezone identifier (e.g., "Europe/Berlin")
 * @returns Date object representing midnight in the specified timezone, as UTC
 *
 * @example
 * parseDateInTimezone("2025-12-25", "Europe/Berlin")
 * // Returns: Date object for 2025-12-24T23:00:00.000Z
 * // (which is midnight on Dec 25 in Europe/Berlin timezone)
 */
export function parseDateInTimezone(
  dateString: string,
  timezone: string,
): Date {
  // Parse the date in the specified timezone at midnight
  const dt = DateTime.fromISO(dateString, { zone: timezone });

  // Convert to JavaScript Date (will be in UTC)
  return dt.toJSDate();
}

/**
 * Get timezone for a country code
 *
 * @param countryCode - ISO country code (e.g., "DE", "US")
 * @returns IANA timezone identifier or null if not found
 */
export function getTimezoneForCountry(countryCode: string): string | null {
  return COUNTRY_TIMEZONES[countryCode] || null;
}
