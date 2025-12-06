/**
 * Intelligent Country Influence Mapping
 *
 * Maps countries to their influencing neighbors based on:
 * - Geographic proximity
 * - Tourism patterns
 * - Economic ties
 * - Language/cultural connections
 */

export const COUNTRY_INFLUENCES: Record<string, string[]> = {
  // Western Europe - High cross-border tourism
  DE: ["FR", "CH", "AT", "NL", "BE", "PL", "CZ"], // Germany
  FR: ["BE", "CH", "DE", "ES", "IT", "GB", "NL"], // France
  NL: ["BE", "DE", "GB", "FR"], // Netherlands
  BE: ["NL", "FR", "DE", "GB"], // Belgium
  CH: ["DE", "FR", "IT", "AT"], // Switzerland
  AT: ["DE", "CH", "IT", "CZ", "SK", "HU"], // Austria
  IT: ["FR", "CH", "AT", "DE", "SI", "HR"], // Italy
  ES: ["FR", "PT", "GB"], // Spain
  PT: ["ES"], // Portugal

  // UK & Ireland
  GB: ["FR", "IE", "NL", "BE", "ES"], // United Kingdom
  IE: ["GB"], // Ireland

  // Nordics
  DK: ["SE", "NO", "DE"], // Denmark
  SE: ["NO", "FI", "DK"], // Sweden
  NO: ["SE", "DK", "FI"], // Norway
  FI: ["SE", "NO", "RU"], // Finland

  // Eastern Europe
  PL: ["DE", "CZ", "SK", "AT"], // Poland
  CZ: ["DE", "AT", "SK", "PL"], // Czech Republic
  SK: ["CZ", "AT", "PL", "HU"], // Slovakia
  HU: ["AT", "SK", "RO", "HR", "SI"], // Hungary
  RO: ["HU", "BG", "RS"], // Romania
  HR: ["SI", "HU", "IT", "AT"], // Croatia
  SI: ["IT", "AT", "HR"], // Slovenia
  BG: ["RO", "GR", "TR"], // Bulgaria
  GR: ["BG", "TR", "IT"], // Greece

  // North America
  US: ["CA", "MX"], // United States
  CA: ["US"], // Canada
  MX: ["US"], // Mexico

  // Middle East
  AE: ["SA", "OM", "QA", "BH"], // UAE
  SA: ["AE", "BH", "KW", "QA", "OM", "JO"], // Saudi Arabia
  IL: ["JO", "EG"], // Israel
  TR: ["GR", "BG", "AE", "SA"], // Turkey
  QA: ["AE", "SA", "BH"], // Qatar
  BH: ["SA", "AE", "QA"], // Bahrain
  KW: ["SA", "AE"], // Kuwait
  OM: ["AE", "SA"], // Oman
  JO: ["IL", "SA", "EG"], // Jordan

  // Asia-Pacific
  JP: ["CN", "KR", "TW", "HK"], // Japan
  CN: ["HK", "TW", "JP", "KR", "VN", "TH"], // China
  KR: ["JP", "CN", "TW"], // South Korea
  TW: ["CN", "JP", "HK", "PH"], // Taiwan
  HK: ["CN", "TW", "SG", "JP"], // Hong Kong
  SG: ["MY", "ID", "TH", "HK"], // Singapore
  MY: ["SG", "ID", "TH", "PH"], // Malaysia
  TH: ["MY", "SG", "VN", "CN", "ID"], // Thailand
  ID: ["SG", "MY", "TH", "PH"], // Indonesia
  VN: ["TH", "CN", "SG"], // Vietnam
  PH: ["TW", "MY", "SG", "ID"], // Philippines
  IN: ["SG", "TH", "MY", "AE"], // India

  // Australia & NZ
  AU: ["NZ", "SG", "ID"], // Australia
  NZ: ["AU"], // New Zealand

  // South America
  BR: ["AR", "UY", "CL"], // Brazil
  AR: ["BR", "CL", "UY"], // Argentina
  CL: ["AR", "PE", "BR"], // Chile
  CO: ["PE", "BR", "UY"], // Colombia
  PE: ["CL", "BR", "CO"], // Peru
  UY: ["AR", "BR"], // Uruguay

  // Africa
  ZA: ["EG", "MA"], // South Africa
  EG: ["SA", "TR", "IL", "JO"], // Egypt
  MA: ["ES", "FR", "PT"], // Morocco

  // Additional
  RU: ["FI", "PL", "DE"], // Russia
};

/**
 * Get influencing countries for a park's country
 */
export function getInfluencingCountries(country: string): string[] {
  return COUNTRY_INFLUENCES[country] || [];
}

/**
 * Priority order for neighbor countries (closer = higher priority)
 */
export function prioritizeNeighbors(
  country: string,
  neighbors: string[],
): string[] {
  const influence = COUNTRY_INFLUENCES[country] || [];

  // Sort by order in COUNTRY_INFLUENCES (first = closest/most important)
  return neighbors.sort((a, b) => {
    const indexA = influence.indexOf(a);
    const indexB = influence.indexOf(b);

    if (indexA === -1 && indexB === -1) return 0;
    if (indexA === -1) return 1;
    if (indexB === -1) return -1;

    return indexA - indexB;
  });
}
