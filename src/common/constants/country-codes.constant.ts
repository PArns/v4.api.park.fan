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
  "United States of America": "US",
  "Vereinigte Staaten": "US",
  USA: "US",
  "America": "US",
  Canada: "CA",
  Kanada: "CA",
  Mexico: "MX",
  Mexiko: "MX",

  // Europe
  "United Kingdom": "GB",
  "United Kingdom of Great Britain and Northern Ireland": "GB",
  "Großbritannien": "GB",
  England: "GB", // Geocoding alias for UK
  Scotland: "GB",
  Wales: "GB",
  "Northern Ireland": "GB",
  Germany: "DE",
  Deutschland: "DE",
  France: "FR",
  Frankreich: "FR",
  Spain: "ES",
  Spanien: "ES",
  España: "ES",
  Italy: "IT",
  Italien: "IT",
  Italia: "IT",
  Netherlands: "NL",
  Niederlande: "NL",
  "The Netherlands": "NL",
  Belgium: "BE",
  Belgien: "BE",
  België: "BE",
  Belgique: "BE",
  Sweden: "SE",
  Schweden: "SE",
  Denmark: "DK",
  Dänemark: "DK",
  Norway: "NO",
  Norwegen: "NO",
  Finland: "FI",
  Finnland: "FI",
  Austria: "AT",
  Österreich: "AT",
  Switzerland: "CH",
  Schweiz: "CH",
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
 * Converts a country name or code to ISO 3166-1 alpha-2 code
 *
 * @param country - Full country name (e.g., "United States") or ISO code (e.g., "US")
 * @returns ISO code (e.g., "US") or null if not found
 */
export function getCountryISOCode(country: string): string | null {
  if (!country) return null;

  const trimmed = country.trim();

  // 1. If it's already a 2-letter code, return it (normalized)
  if (trimmed.length === 2) {
    return trimmed.toUpperCase();
  }

  // 2. Check mapping for full names or aliases
  return COUNTRY_NAME_TO_ISO_CODE[trimmed] || null;
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
