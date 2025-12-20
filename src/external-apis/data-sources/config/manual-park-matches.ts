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
    "walt disney studios park": ["disney adventure world"],
    "disney adventure world": ["walt disney studios park"], // Bidirectional safety

    // Other common divergences
    "disneys hollywood studios": ["hollywood studios"],
    "universal studios florida": ["universal studios orlando"],
    "universal studios japan": ["universal studios japan usj"],
    "movie park germany": ["movie park"],
    "holiday park": ["holiday park pfalz"],
};
