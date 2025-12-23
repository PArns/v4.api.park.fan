/**
 * Manual Park Matching Overrides
 *
 * This file contains a list of manual park name aliases to force matching
 * between data sources when fuzzy matching fails or names are significantly different
 * (e.g. strict rebranding like "Walt Disney Studios Park" -> "Disney Adventure World").
 *
 * Key: Normalized Name (usually from ThemeParks.wiki, the "Anchor")
 * Value: Array of Normalized Alias Names (from other sources like Queue-Times/Wartezeiten)
 *
 * Note: Names should be lowercase strings. Normalization (removing special chars)
 * happens at runtime, but keeping keys simple here helps.
 */
export const MANUALLY_MATCHED_PARKS: Record<string, string[]> = {
  // Disney Adventure World (Wartezeiten) <-> Walt Disney Studios Park (Wiki)
  waltdisneystudiospark: ["disneyadventureworld"],
  disneyadventureworld: ["waltdisneystudiospark"], // Bidirectional

  // Universal Studios
  universalstudiosflorida: ["universalstudiosorlando"],
  universalstudiosjapan: ["universalstudiosjapanusj"],

  // Major renaming / differences checks
  disneylandparis: ["disneylandparkparis"],
  parcasterix: ["asterixpark"],

  // Specific overrides for ambiguous matches
  plopsalandbelgium: [
    "plopsadepanne",
    "plopsalanddepanne",
    "plopsalanddepannebe",
  ],

  // Note: Traumatica removed as it is now globally excluded
};
