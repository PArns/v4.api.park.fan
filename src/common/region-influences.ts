import { normalizeRegionCode } from "./utils/region.util";

/**
 * Regional Influence Mapping (Bundesländer/States/Provinces)
 *
 * Maps specific regions to their influencing neighbors.
 * Used for finer-grained ML predictions (e.g. "Is it a school holiday in the neighboring state?").
 *
 * Format:
 * "CountryCode-RegionCode": [{ countryCode: "XX", regionCode: "XX-YY" }, ...]
 *
 * Notes:
 * - Codes must match OpenHolidays API subdivision codes exactly.
 * - If a neighbor country has no regional data in OpenHolidays (e.g. BE, DK), use regionCode: null.
 * - France uses "Zones" (A, B, C) for school holidays, but OpenHolidays also supports regions like "FR-GE".
 */

export interface RegionInfluence {
  countryCode: string; // ISO 3166-1 alpha-2
  regionCode: string | null; // ISO 3166-2 or OpenHolidays subdivision code
}

export const REGION_INFLUENCES: Record<string, RegionInfluence[]> = {
  // ==========================================
  // GERMANY (DE)
  // ==========================================

  // Baden-Württemberg (Europa-Park)
  "DE-BW": [
    { countryCode: "DE", regionCode: "DE-BY" }, // Bavaria
    { countryCode: "DE", regionCode: "DE-RP" }, // Rhineland-Palatinate
    { countryCode: "DE", regionCode: "DE-HE" }, // Hesse
    { countryCode: "FR", regionCode: "FR-GE" }, // Grand Est (Alsace)
    { countryCode: "CH", regionCode: "CH-BS" }, // Basel-Stadt
    { countryCode: "CH", regionCode: "CH-BL" }, // Basel-Landschaft
    { countryCode: "CH", regionCode: "CH-AG" }, // Aargau
  ],

  // Bavaria (Bayern-Park, Legoland, Skyline Park)
  "DE-BY": [
    { countryCode: "DE", regionCode: "DE-BW" },
    { countryCode: "DE", regionCode: "DE-HE" },
    { countryCode: "DE", regionCode: "DE-TH" }, // Thuringia
    { countryCode: "DE", regionCode: "DE-SN" }, // Saxony
    { countryCode: "AT", regionCode: "AT-SB" }, // Salzburg
    { countryCode: "AT", regionCode: "AT-TI" }, // Tyrol
    { countryCode: "AT", regionCode: "AT-OÖ" }, // Upper Austria
    { countryCode: "CZ", regionCode: "CZ-PL" }, // Plzen
    { countryCode: "CZ", regionCode: "CZ-JC" }, // South Bohemia
  ],

  // Berlin
  "DE-BE": [
    { countryCode: "DE", regionCode: "DE-BB" }, // Brandenburg (encircles Berlin)
  ],

  // Brandenburg (Tropical Islands)
  "DE-BB": [
    { countryCode: "DE", regionCode: "DE-BE" },
    { countryCode: "DE", regionCode: "DE-MV" },
    { countryCode: "DE", regionCode: "DE-SN" },
    { countryCode: "DE", regionCode: "DE-ST" }, // Saxony-Anhalt
    { countryCode: "PL", regionCode: "PL-LB" }, // Lubusz
    { countryCode: "PL", regionCode: "PL-ZP" }, // West Pomerania
  ],

  // Bremen
  "DE-HB": [
    { countryCode: "DE", regionCode: "DE-NI" }, // Lower Saxony (encircles Bremen)
  ],

  // Hamburg
  "DE-HH": [
    { countryCode: "DE", regionCode: "DE-SH" }, // Schleswig-Holstein
    { countryCode: "DE", regionCode: "DE-NI" }, // Lower Saxony
  ],

  // Hesse (Taunus Wunderland)
  "DE-HE": [
    { countryCode: "DE", regionCode: "DE-NW" },
    { countryCode: "DE", regionCode: "DE-NI" },
    { countryCode: "DE", regionCode: "DE-TH" },
    { countryCode: "DE", regionCode: "DE-BY" },
    { countryCode: "DE", regionCode: "DE-BW" },
    { countryCode: "DE", regionCode: "DE-RP" },
  ],

  // Mecklenburg-Western Pomerania
  "DE-MV": [
    { countryCode: "DE", regionCode: "DE-SH" },
    { countryCode: "DE", regionCode: "DE-BB" },
    { countryCode: "DE", regionCode: "DE-NI" },
    { countryCode: "PL", regionCode: "PL-ZP" }, // West Pomerania
  ],

  // Lower Saxony (Heide Park, Rasti-Land)
  "DE-NI": [
    { countryCode: "DE", regionCode: "DE-HB" },
    { countryCode: "DE", regionCode: "DE-HH" },
    { countryCode: "DE", regionCode: "DE-SH" },
    { countryCode: "DE", regionCode: "DE-NW" },
    { countryCode: "DE", regionCode: "DE-HE" },
    { countryCode: "DE", regionCode: "DE-TH" },
    { countryCode: "DE", regionCode: "DE-ST" },
    { countryCode: "NL", regionCode: "NL-GR" }, // Groningen
    { countryCode: "NL", regionCode: "NL-DR" }, // Drenthe
    { countryCode: "NL", regionCode: "NL-OV" }, // Overijssel
  ],

  // North Rhine-Westphalia (Phantasialand, Movie Park)
  "DE-NW": [
    { countryCode: "DE", regionCode: "DE-RP" },
    { countryCode: "DE", regionCode: "DE-HE" },
    { countryCode: "DE", regionCode: "DE-NI" },
    { countryCode: "NL", regionCode: "NL-LI" }, // Limburg
    { countryCode: "NL", regionCode: "NL-GE" }, // Gelderland
    { countryCode: "BE", regionCode: null }, // Belgium (Nationwide)
  ],

  // Rhineland-Palatinate (Holiday Park)
  "DE-RP": [
    { countryCode: "DE", regionCode: "DE-NW" },
    { countryCode: "DE", regionCode: "DE-BW" },
    { countryCode: "DE", regionCode: "DE-HE" },
    { countryCode: "DE", regionCode: "DE-SL" }, // Saarland
    { countryCode: "FR", regionCode: "FR-GE" }, // Grand Est
    { countryCode: "LU", regionCode: null }, // Luxembourg (Nationwide)
    { countryCode: "BE", regionCode: null }, // Belgium (Nationwide)
  ],

  // Saarland
  "DE-SL": [
    { countryCode: "DE", regionCode: "DE-RP" },
    { countryCode: "FR", regionCode: "FR-GE" },
    { countryCode: "LU", regionCode: null },
  ],

  // Saxony (Belantis)
  "DE-SN": [
    { countryCode: "DE", regionCode: "DE-BB" },
    { countryCode: "DE", regionCode: "DE-ST" },
    { countryCode: "DE", regionCode: "DE-TH" },
    { countryCode: "DE", regionCode: "DE-BY" },
    { countryCode: "CZ", regionCode: "CZ-KR" }, // Karlovy Vary
    { countryCode: "CZ", regionCode: "CZ-US" }, // Usti nad Labem
    { countryCode: "PL", regionCode: "PL-DS" }, // Lower Silesia
  ],

  // Saxony-Anhalt
  "DE-ST": [
    { countryCode: "DE", regionCode: "DE-BB" },
    { countryCode: "DE", regionCode: "DE-SN" },
    { countryCode: "DE", regionCode: "DE-TH" },
    { countryCode: "DE", regionCode: "DE-NI" },
  ],

  // Schleswig-Holstein (Hansa-Park)
  "DE-SH": [
    { countryCode: "DE", regionCode: "DE-HH" },
    { countryCode: "DE", regionCode: "DE-NI" },
    { countryCode: "DE", regionCode: "DE-MV" },
    { countryCode: "DK", regionCode: null }, // Denmark (Nationwide)
  ],

  // Thuringia
  "DE-TH": [
    { countryCode: "DE", regionCode: "DE-NI" },
    { countryCode: "DE", regionCode: "DE-ST" },
    { countryCode: "DE", regionCode: "DE-SN" },
    { countryCode: "DE", regionCode: "DE-BY" },
    { countryCode: "DE", regionCode: "DE-HE" },
  ],

  // ==========================================
  // AUSTRIA (AT)
  // ==========================================

  // Vienna (Prater)
  "AT-WI": [
    { countryCode: "AT", regionCode: "AT-NÖ" }, // Lower Austria (Surrounds Vienna)
  ],

  // Lower Austria (Familypark)
  "AT-NÖ": [
    { countryCode: "AT", regionCode: "AT-WI" }, // Vienna
    { countryCode: "AT", regionCode: "AT-BL" }, // Burgenland
    { countryCode: "AT", regionCode: "AT-SM" }, // Styria
    { countryCode: "AT", regionCode: "AT-OÖ" }, // Upper Austria
    { countryCode: "CZ", regionCode: "CZ-JM" }, // South Moravia
    { countryCode: "SK", regionCode: null }, // Slovakia
  ],

  // Upper Austria (Fantasiana)
  "AT-OÖ": [
    { countryCode: "AT", regionCode: "AT-NÖ" },
    { countryCode: "AT", regionCode: "AT-SM" },
    { countryCode: "AT", regionCode: "AT-SB" }, // Salzburg
    { countryCode: "DE", regionCode: "DE-BY" }, // Bavaria
    { countryCode: "CZ", regionCode: "CZ-JC" }, // South Bohemia
  ],

  // Salzburg
  "AT-SB": [
    { countryCode: "AT", regionCode: "AT-TI" }, // Tyrol
    { countryCode: "AT", regionCode: "AT-KÄ" }, // Carinthia
    { countryCode: "AT", regionCode: "AT-SM" },
    { countryCode: "AT", regionCode: "AT-OÖ" },
    { countryCode: "DE", regionCode: "DE-BY" }, // Bavaria
  ],

  // Tyrol
  "AT-TI": [
    { countryCode: "AT", regionCode: "AT-VA" }, // Vorarlberg
    { countryCode: "AT", regionCode: "AT-SB" },
    { countryCode: "AT", regionCode: "AT-KÄ" },
    { countryCode: "IT", regionCode: "IT-TR" }, // Trentino-South Tyrol
    { countryCode: "DE", regionCode: "DE-BY" },
    { countryCode: "CH", regionCode: "CH-GR" }, // Grisons
  ],

  // ==========================================
  // NETHERLANDS (NL)
  // ==========================================

  // North Brabant (Efteling)
  "NL-NB": [
    { countryCode: "NL", regionCode: "NL-LI" }, // Limburg
    { countryCode: "NL", regionCode: "NL-ZE" }, // Zeeland
    { countryCode: "NL", regionCode: "NL-GE" }, // Gelderland
    { countryCode: "NL", regionCode: "NL-ZH" }, // South Holland
    { countryCode: "BE", regionCode: null }, // Belgium (Nationwide)
  ],

  // Limburg (Toverland)
  "NL-LI": [
    { countryCode: "NL", regionCode: "NL-NB" },
    { countryCode: "NL", regionCode: "NL-GE" },
    { countryCode: "DE", regionCode: "DE-NW" }, // NRW
    { countryCode: "BE", regionCode: null }, // Belgium
  ],

  // South Holland (Duinrell)
  "NL-ZH": [
    { countryCode: "NL", regionCode: "NL-NH" }, // North Holland
    { countryCode: "NL", regionCode: "NL-UT" }, // Utrecht
    { countryCode: "NL", regionCode: "NL-GE" },
    { countryCode: "NL", regionCode: "NL-NB" },
  ],

  // Flevoland (Walibi Holland)
  "NL-FL": [
    { countryCode: "NL", regionCode: "NL-NH" },
    { countryCode: "NL", regionCode: "NL-UT" },
    { countryCode: "NL", regionCode: "NL-GE" },
    { countryCode: "NL", regionCode: "NL-OV" }, // Overijssel
    { countryCode: "NL", regionCode: "NL-FR" }, // Friesland
  ],

  // ==========================================
  // FRANCE (FR)
  // ==========================================

  // Île-de-France (Disneyland Paris, Parc Astérix)
  "FR-IF": [
    { countryCode: "FR", regionCode: "FR-HF" }, // Hauts-de-France
    { countryCode: "FR", regionCode: "FR-GE" }, // Grand Est
    { countryCode: "FR", regionCode: "FR-BF" }, // Bourgogne-Franche-Comté
    { countryCode: "FR", regionCode: "FR-CV" }, // Centre-Val de Loire
    { countryCode: "FR", regionCode: "FR-NO" }, // Normandie
  ],

  // Grand Est (Walygator, Nigloland)
  "FR-GE": [
    { countryCode: "FR", regionCode: "FR-HF" },
    { countryCode: "FR", regionCode: "FR-IF" },
    { countryCode: "FR", regionCode: "FR-BF" },
    { countryCode: "BE", regionCode: null },
    { countryCode: "LU", regionCode: null },
    { countryCode: "DE", regionCode: "DE-BW" },
    { countryCode: "DE", regionCode: "DE-RP" },
    { countryCode: "DE", regionCode: "DE-SL" },
    { countryCode: "CH", regionCode: "CH-BS" }, // Basel
  ],

  // Hauts-de-France (Parc Bagatelle, Dennlys Parc)
  "FR-HF": [
    { countryCode: "FR", regionCode: "FR-NO" },
    { countryCode: "FR", regionCode: "FR-IF" },
    { countryCode: "FR", regionCode: "FR-GE" },
    { countryCode: "BE", regionCode: null }, // Belgium
  ],

  // Auvergne-Rhône-Alpes (Walibi Rhône-Alpes)
  "FR-AR": [
    { countryCode: "FR", regionCode: "FR-BF" },
    { countryCode: "FR", regionCode: "FR-OC" }, // Occitanie
    { countryCode: "FR", regionCode: "FR-PC" }, // Provence-Alpes-Côte d'Azur
    { countryCode: "CH", regionCode: "CH-GE" }, // Geneva
    { countryCode: "IT", regionCode: "IT-VA" }, // Aosta Valley
    { countryCode: "IT", regionCode: "IT-PI" }, // Piedmont
  ],

  // ==========================================
  // SPAIN (ES)
  // ==========================================

  // Catalonia (PortAventura World)
  "ES-CT": [
    { countryCode: "ES", regionCode: "ES-AR" }, // Aragon
    { countryCode: "ES", regionCode: "ES-VC" }, // Valencian Community
    { countryCode: "FR", regionCode: "FR-OC" }, // Occitanie
  ],

  // Community of Madrid (Parque Warner)
  "ES-MD": [
    { countryCode: "ES", regionCode: "ES-CL" }, // Castile and León
    { countryCode: "ES", regionCode: "ES-CM" }, // Castilla-La Mancha
  ],

  // ==========================================
  // ITALY (IT)
  // ==========================================

  // Veneto (Gardaland)
  "IT-VE": [
    { countryCode: "IT", regionCode: "IT-FV" }, // Friuli Venezia Giulia
    { countryCode: "IT", regionCode: "IT-TR" }, // Trentino-Alto Adige
    { countryCode: "IT", regionCode: "IT-LO" }, // Lombardy
    { countryCode: "IT", regionCode: "IT-ER" }, // Emilia-Romagna
  ],

  // Emilia-Romagna (Mirabilandia)
  "IT-ER": [
    { countryCode: "IT", regionCode: "IT-VE" },
    { countryCode: "IT", regionCode: "IT-LO" },
    { countryCode: "IT", regionCode: "IT-PI" }, // Piedmont
    { countryCode: "IT", regionCode: "IT-LI" }, // Liguria
    { countryCode: "IT", regionCode: "IT-TO" }, // Tuscany
    { countryCode: "IT", regionCode: "IT-MA" }, // Marche
  ],

  // ==========================================
  // POLAND (PL)
  // ==========================================

  // Lesser Poland (Energylandia)
  "PL-MA": [
    { countryCode: "PL", regionCode: "PL-SK" }, // Silesia
    { countryCode: "PL", regionCode: "PL-SL" }, // Holy Cross
    { countryCode: "PL", regionCode: "PL-PD" }, // Subcarpathia
    { countryCode: "SK", regionCode: null }, // Slovakia
  ],

  // ==========================================
  // UNITED KINGDOM (GB)
  // ==========================================

  // England (All major UK parks)
  "GB-ENG": [
    { countryCode: "GB", regionCode: "GB-WLS" }, // Wales
    { countryCode: "GB", regionCode: "GB-SCT" }, // Scotland
    { countryCode: "IE", regionCode: null }, // Ireland
    { countryCode: "FR", regionCode: "FR-HF" }, // Northern France (Eurostar)
    { countryCode: "BE", regionCode: null }, // Belgium
    { countryCode: "NL", regionCode: null }, // Netherlands
  ],

  // ==========================================
  // UNITED STATES (US)
  // ==========================================

  // Florida (Walt Disney World, Universal Orlando, SeaWorld, Busch Gardens)
  "US-FL": [
    { countryCode: "US", regionCode: "US-GA" }, // Georgia
    { countryCode: "US", regionCode: "US-AL" }, // Alabama
    { countryCode: "US", regionCode: "US-SC" }, // South Carolina
    { countryCode: "US", regionCode: "US-NC" }, // North Carolina
    { countryCode: "US", regionCode: "US-TN" }, // Tennessee
  ],

  // California (Disneyland, Universal Studios Hollywood, Knott's, Six Flags)
  "US-CA": [
    { countryCode: "US", regionCode: "US-AZ" }, // Arizona
    { countryCode: "US", regionCode: "US-NV" }, // Nevada
    { countryCode: "US", regionCode: "US-OR" }, // Oregon
    { countryCode: "MX", regionCode: null }, // Mexico (Baja California proximity)
  ],

  // Texas (Six Flags Over Texas, SeaWorld San Antonio)
  "US-TX": [
    { countryCode: "US", regionCode: "US-OK" }, // Oklahoma
    { countryCode: "US", regionCode: "US-LA" }, // Louisiana
    { countryCode: "US", regionCode: "US-NM" }, // New Mexico
    { countryCode: "US", regionCode: "US-AR" }, // Arkansas
    { countryCode: "MX", regionCode: null }, // Mexico
  ],

  // Ohio (Cedar Point, Kings Island)
  "US-OH": [
    { countryCode: "US", regionCode: "US-PA" }, // Pennsylvania
    { countryCode: "US", regionCode: "US-MI" }, // Michigan
    { countryCode: "US", regionCode: "US-IN" }, // Indiana
    { countryCode: "US", regionCode: "US-WV" }, // West Virginia
    { countryCode: "US", regionCode: "US-KY" }, // Kentucky
    { countryCode: "CA", regionCode: "CA-ON" }, // Ontario (Canada)
  ],

  // Pennsylvania (Hersheypark, Dorney Park)
  "US-PA": [
    { countryCode: "US", regionCode: "US-NY" }, // New York
    { countryCode: "US", regionCode: "US-NJ" }, // New Jersey
    { countryCode: "US", regionCode: "US-MD" }, // Maryland
    { countryCode: "US", regionCode: "US-OH" }, // Ohio
    { countryCode: "US", regionCode: "US-DE" }, // Delaware
  ],

  // New Jersey (Six Flags Great Adventure)
  "US-NJ": [
    { countryCode: "US", regionCode: "US-NY" },
    { countryCode: "US", regionCode: "US-PA" },
    { countryCode: "US", regionCode: "US-CT" },
    { countryCode: "US", regionCode: "US-DE" },
  ],

  // Virginia (Busch Gardens Williamsburg, Kings Dominion)
  "US-VA": [
    { countryCode: "US", regionCode: "US-MD" },
    { countryCode: "US", regionCode: "US-NC" },
    { countryCode: "US", regionCode: "US-WV" },
    { countryCode: "US", regionCode: "US-DC" },
  ],

  // Georgia (Six Flags Over Georgia)
  "US-GA": [
    { countryCode: "US", regionCode: "US-FL" },
    { countryCode: "US", regionCode: "US-SC" },
    { countryCode: "US", regionCode: "US-NC" },
    { countryCode: "US", regionCode: "US-AL" },
    { countryCode: "US", regionCode: "US-TN" },
  ],

  // ==========================================
  // JAPAN (JP)
  // ==========================================

  // Chiba (Tokyo Disney Resort)
  "JP-12": [
    { countryCode: "JP", regionCode: "JP-13" }, // Tokyo
    { countryCode: "JP", regionCode: "JP-14" }, // Kanagawa
    { countryCode: "JP", regionCode: "JP-11" }, // Saitama
    { countryCode: "JP", regionCode: "JP-08" }, // Ibaraki
  ],

  // Osaka (Universal Studios Japan)
  "JP-27": [
    { countryCode: "JP", regionCode: "JP-26" }, // Kyoto
    { countryCode: "JP", regionCode: "JP-28" }, // Hyogo
    { countryCode: "JP", regionCode: "JP-29" }, // Nara
    { countryCode: "JP", regionCode: "JP-30" }, // Wakayama
  ],

  // Aichi (Legoland Japan)
  "JP-23": [
    { countryCode: "JP", regionCode: "JP-24" }, // Mie
    { countryCode: "JP", regionCode: "JP-21" }, // Gifu
    { countryCode: "JP", regionCode: "JP-22" }, // Shizuoka
  ],

  // ==========================================
  // AUSTRALIA (AU)
  // ==========================================

  // Queensland (Dreamworld, Warner Bros. Movie World, Sea World)
  "AU-QLD": [
    { countryCode: "AU", regionCode: "AU-NSW" }, // New South Wales
    { countryCode: "NZ", regionCode: null }, // New Zealand
  ],

  // ==========================================
  // BELGIUM (BE)
  // ==========================================

  // Flanders (Bobbejaanland, Plopsaland, Walibi Belgium)
  "BE-VLG": [
    { countryCode: "NL", regionCode: "NL-NB" }, // Noord-Brabant
    { countryCode: "NL", regionCode: "NL-LI" }, // Limburg
    { countryCode: "DE", regionCode: "DE-NW" }, // NRW
    { countryCode: "FR", regionCode: "FR-HF" }, // Hauts-de-France
  ],

  // Wallonia
  "BE-WAL": [
    { countryCode: "FR", regionCode: "FR-GE" }, // Grand Est
    { countryCode: "FR", regionCode: "FR-HF" },
    { countryCode: "LU", regionCode: null }, // Luxembourg
    { countryCode: "DE", regionCode: "DE-RP" }, // Rhineland-Palatinate
  ],

  // ==========================================
  // DENMARK (DK)
  // ==========================================

  // Central Denmark (Legoland Billund)
  "DK-82": [
    { countryCode: "DK", regionCode: "DK-83" }, // Region of Southern Denmark
    { countryCode: "DK", regionCode: "DK-81" }, // North Denmark
    { countryCode: "DE", regionCode: "DE-SH" }, // Schleswig-Holstein
  ],

  // Region of Southern Denmark (Legoland proximity)
  "DK-83": [
    { countryCode: "DK", regionCode: "DK-82" },
    { countryCode: "DE", regionCode: "DE-SH" },
  ],

  // ==========================================
  // CANADA (CA)
  // ==========================================

  // Ontario (Canada's Wonderland)
  "CA-ON": [
    { countryCode: "CA", regionCode: "CA-QC" }, // Quebec
    { countryCode: "US", regionCode: "US-NY" }, // New York
    { countryCode: "US", regionCode: "US-MI" }, // Michigan
    { countryCode: "US", regionCode: "US-OH" }, // Ohio
  ],

  // Quebec (La Ronde)
  "CA-QC": [
    { countryCode: "CA", regionCode: "CA-ON" },
    { countryCode: "US", regionCode: "US-NY" },
    { countryCode: "US", regionCode: "US-VT" }, // Vermont
  ],

  // ==========================================
  // SOUTH KOREA (KR)
  // ==========================================

  // Gyeonggi-do (Everland, Lotte World)
  "KR-41": [
    { countryCode: "KR", regionCode: "KR-11" }, // Seoul
    { countryCode: "KR", regionCode: "KR-42" }, // Gangwon
    { countryCode: "KR", regionCode: "KR-43" }, // North Chungcheong
  ],

  // Seoul
  "KR-11": [
    { countryCode: "KR", regionCode: "KR-41" }, // Gyeonggi
    { countryCode: "KR", regionCode: "KR-28" }, // Incheon
  ],

  // ==========================================
  // SWEDEN (SE)
  // ==========================================

  // Stockholm (Gröna Lund)
  "SE-AB": [
    { countryCode: "SE", regionCode: "SE-C" }, // Uppsala
    { countryCode: "SE", regionCode: "SE-D" }, // Södermanland
    { countryCode: "FI", regionCode: null }, // Finland
  ],

  // Västra Götaland (Liseberg)
  "SE-O": [
    { countryCode: "SE", regionCode: "SE-N" }, // Halland
    { countryCode: "DK", regionCode: null }, // Denmark
    { countryCode: "NO", regionCode: null }, // Norway
  ],

  // ==========================================
  // CHINA (CN)
  // ==========================================

  // Shanghai
  "CN-31": [
    { countryCode: "CN", regionCode: "CN-32" }, // Jiangsu
    { countryCode: "CN", regionCode: "CN-33" }, // Zhejiang
  ],

  // Beijing
  "CN-11": [
    { countryCode: "CN", regionCode: "CN-12" }, // Tianjin
    { countryCode: "CN", regionCode: "CN-13" }, // Hebei
  ],

  // Guangdong (Chimelong)
  "CN-44": [
    { countryCode: "CN", regionCode: "CN-45" }, // Guangxi
    { countryCode: "HK", regionCode: null }, // Hong Kong
    { countryCode: "MO", regionCode: null }, // Macau
  ],

  // ==========================================
  // MEXICO (MX)
  // ==========================================

  // Mexico City (Six Flags Mexico)
  "MX-CMX": [
    { countryCode: "MX", regionCode: "MX-MEX" }, // State of Mexico
    { countryCode: "MX", regionCode: "MX-PUE" }, // Puebla
    { countryCode: "MX", regionCode: "MX-MOR" }, // Morelos
  ],
};

/**
 * Get influencing regions for a specific region
 */
export function getInfluencingRegions(
  countryCode: string,
  regionCode: string,
): RegionInfluence[] {
  // Normalize region code to handle variants (e.g. NRW -> NW)
  const normalizedRegion = normalizeRegionCode(regionCode);

  if (!normalizedRegion) {
    return [];
  }

  // If regionCode already defines the uniqueness (e.g. "DE-BW"), use it directly.
  // Otherwise, prepend countryCode (e.g. "BW" -> "DE-BW").
  const key = normalizedRegion.includes("-")
    ? normalizedRegion
    : `${countryCode}-${normalizedRegion}`;
  return REGION_INFLUENCES[key] || [];
}
