/**
 * Country Name to ISO 3166-1 alpha-2 Code Mapping
 *
 * Maps full country names (as stored in database from Google Geocoding)
 * to ISO 3166-1 alpha-2 country codes (as required by Nager.Date API).
 *
 * Used for:
 * - Holiday data fetching from Nager.Date API
 * - Any other APIs that require ISO country codes
 *
 * Source: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
 */
export const COUNTRY_NAME_TO_ISO_CODE: Record<string, string> = {
  // North America
  "United States": "US",
  Canada: "CA",
  Mexico: "MX",

  // Europe
  "United Kingdom": "GB",
  England: "GB", // Geocoding alias for UK
  Germany: "DE",
  France: "FR",
  Spain: "ES",
  Italy: "IT",
  Netherlands: "NL",
  Belgium: "BE",
  Sweden: "SE",
  Denmark: "DK",
  Norway: "NO",
  Finland: "FI",
  Austria: "AT",
  Switzerland: "CH",
  Poland: "PL",
  "Czech Republic": "CZ",
  Portugal: "PT",
  Ireland: "IE",
  Greece: "GR",
  Hungary: "HU",
  Romania: "RO",
  Slovakia: "SK",
  Croatia: "HR",
  Bulgaria: "BG",
  Slovenia: "SI",

  // Asia
  China: "CN",
  Japan: "JP",
  "South Korea": "KR",
  "Hong Kong": "HK",
  Singapore: "SG",
  Thailand: "TH",
  India: "IN",
  Indonesia: "ID",
  Malaysia: "MY",
  Philippines: "PH",
  Vietnam: "VN",
  Taiwan: "TW",

  // Middle East
  "United Arab Emirates": "AE",
  "Saudi Arabia": "SA",
  Israel: "IL",
  Turkey: "TR",
  Qatar: "QA",
  Bahrain: "BH",
  Kuwait: "KW",
  Oman: "OM",
  Jordan: "JO",

  // Oceania
  Australia: "AU",
  "New Zealand": "NZ",

  // South America
  Brazil: "BR",
  Argentina: "AR",
  Chile: "CL",
  Colombia: "CO",
  Peru: "PE",
  Uruguay: "UY",

  // Africa
  "South Africa": "ZA",
  Egypt: "EG",
  Morocco: "MA",

  // Additional (for completeness)
  Russia: "RU",
};

/**
 * Converts a country name to ISO 3166-1 alpha-2 code
 *
 * @param countryName - Full country name (e.g., "United States")
 * @returns ISO code (e.g., "US") or null if not found
 */
export function getCountryISOCode(countryName: string): string | null {
  return COUNTRY_NAME_TO_ISO_CODE[countryName] || null;
}

/**
 * Gets country name from ISO code (reverse lookup)
 *
 * @param isoCode - ISO 3166-1 alpha-2 code (e.g., "US")
 * @returns Country name (e.g., "United States") or null if not found
 */
export function getCountryNameFromISO(isoCode: string): string | null {
  const entry = Object.entries(COUNTRY_NAME_TO_ISO_CODE).find(
    ([_, code]) => code === isoCode,
  );
  return entry ? entry[0] : null;
}
