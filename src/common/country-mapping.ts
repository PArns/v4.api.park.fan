/**
 * Country Name to ISO Code Mapping
 *
 * Maps full country names (as stored in parks.country) to ISO 3166-1 alpha-2 codes
 * (as used in holidays.country and weather APIs)
 */

export const COUNTRY_NAME_TO_ISO: Record<string, string> = {
  // Europe
  Germany: "DE",
  France: "FR",
  "United Kingdom": "GB",
  England: "GB", // Geocoding alias
  Spain: "ES",
  Italy: "IT",
  Netherlands: "NL",
  Belgium: "BE",
  Switzerland: "CH",
  Austria: "AT",
  Denmark: "DK",
  Sweden: "SE",
  Norway: "NO",
  Finland: "FI",
  Poland: "PL",
  Portugal: "PT",
  Ireland: "IE",
  Greece: "GR",
  "Czech Republic": "CZ",
  Hungary: "HU",
  Romania: "RO",
  Slovakia: "SK",
  Croatia: "HR",
  Bulgaria: "BG",
  Slovenia: "SI",

  // North America
  "United States": "US",
  Canada: "CA",
  Mexico: "MX",

  // Asia
  China: "CN",
  Japan: "JP",
  "South Korea": "KR",
  "Hong Kong": "HK",
  Singapore: "SG",
  Thailand: "TH",
  Malaysia: "MY",
  Indonesia: "ID",
  Taiwan: "TW",
  India: "IN",
  Philippines: "PH",
  Vietnam: "VN",

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

  // Additional
  Russia: "RU",
};

export const ISO_TO_COUNTRY_NAME: Record<string, string> = Object.fromEntries(
  Object.entries(COUNTRY_NAME_TO_ISO).map(([name, iso]) => [iso, name]),
);

export function getCountryISO(countryName: string): string | null {
  return COUNTRY_NAME_TO_ISO[countryName] || null;
}

export function getCountryName(iso: string): string | null {
  return ISO_TO_COUNTRY_NAME[iso] || null;
}
