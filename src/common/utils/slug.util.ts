import { transliterate } from "transliteration";
import slugify from "slugify";

/**
 * Generates a URL-safe slug from any string (supports international characters).
 *
 * Process:
 * 1. Remove trademark symbols (®, ™, ©, ℠)
 * 2. Transliterate non-Latin characters (東京 → tokyo)
 * 3. Slugify (lowercase, remove special chars, spaces → hyphens)
 * 4. Enforce strict mode (only a-z, 0-9, hyphens)
 *
 * Examples:
 * - "東京ディズニーランド" → "dong-jing-dizunirando"
 * - "Walt Disney World" → "walt-disney-world"
 * - "Spider-Man®" → "spider-man"
 * - "Disneyland™ Paris" → "disneyland-paris"
 *
 * @param name - Original name (any language)
 * @returns URL-safe slug
 */
export function generateSlug(name: string): string {
  if (!name || name.trim().length === 0) {
    throw new Error("Cannot generate slug from empty string");
  }

  // Step 1: Remove trademark/copyright symbols (before transliteration)
  // These would otherwise be converted to letters (® → r, ™ → tm)
  // Step 1: Remove trademark/copyright symbols (before transliteration)
  // These would otherwise be converted to letters (® → r, ™ → tm)
  let cleaned = name.replace(/[®™©℠]/g, "");

  // German umlaut handling (explicit replacements before transliteration)
  cleaned = cleaned
    .replace(/ä/g, "ae")
    .replace(/Ä/g, "Ae")
    .replace(/ö/g, "oe")
    .replace(/Ö/g, "Oe")
    .replace(/ü/g, "ue")
    .replace(/Ü/g, "Ue")
    .replace(/ß/g, "ss");

  // Step 2: Transliterate (handles Chinese, Japanese, Korean, Cyrillic, etc.)
  const transliterated = transliterate(cleaned);

  // Step 3: Slugify with strict mode
  const slug = slugify(transliterated, {
    lower: true, // Convert to lowercase
    strict: true, // Only a-z, 0-9, hyphens
    trim: true, // Remove leading/trailing hyphens
  });

  if (!slug || slug.length === 0) {
    throw new Error(
      `Failed to generate valid slug from: "${name}" (cleaned: "${cleaned}", transliterated: "${transliterated}")`,
    );
  }

  return slug;
}

/**
 * Generates a unique slug by appending a suffix if needed.
 *
 * Used by entity hooks to resolve slug conflicts.
 *
 * Example:
 * - "phantasialand" exists → "phantasialand-2"
 * - "phantasialand-2" exists → "phantasialand-3"
 *
 * @param baseSlug - Base slug without suffix
 * @param existingSlugs - Array of already used slugs
 * @returns Unique slug
 */
export function generateUniqueSlug(
  baseSlug: string,
  existingSlugs: string[],
): string {
  let slug = baseSlug;
  let counter = 2;

  while (existingSlugs.includes(slug)) {
    slug = `${baseSlug}-${counter}`;
    counter++;
  }

  return slug;
}

/**
 * Normalize string for fuzzy matching comparison
 *
 * Used by entity matching and conflict resolution to compare park/attraction names
 * ignoring case, symbols, and extra whitespace.
 *
 * Process:
 * - Convert to lowercase
 * - Remove trademark symbols (®, ™, &, ', -)
 * - Normalize whitespace to single spaces
 * - Trim
 *
 * Examples:
 * - "Spider-Man® Web Slingers" → "spiderman web slingers"
 * - "Walt Disney's Magic Kingdom" → "walt disneys magic kingdom"
 *
 * @param text - Original text to normalize
 * @returns Normalized text for comparison
 */
export function normalizeForMatching(text: string): string {
  // Transliterate first to handle accents (e.g. Astérix -> Asterix)
  const transliterated = transliterate(text);

  return (
    transliterated
      .toLowerCase()
      // Remove country code suffixes like " (FR)", " (US)", etc. before other normalization
      .replace(/\s*\(\s*[A-Z]{2}\s*\)\s*$/g, "")
      .replace(/[®™&'\-]/g, "")
      .replace(/\s+/g, " ")
      .trim()
  );
}
