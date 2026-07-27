/**
 * Unit a park's operator publishes ride heights in.
 *
 * We always store centimetres, but the number a guest sees on park signage is
 * what a ride page should show. The stored values make the split obvious:
 * German parks carry round metric figures (80, 90, 100, 110, 120, 130, 140),
 * while US parks carry 76, 81, 86, 91, 97, 102, 107, 112, 122, 137 — the
 * exact conversions of 30", 32", 34", 36", 38", 40", 42", 44", 48", 54".
 */
const IMPERIAL_HEIGHT_COUNTRIES = new Set(["US"]);

export function publishedHeightUnit(
  countryCode: string | null | undefined,
): "cm" | "in" {
  return countryCode && IMPERIAL_HEIGHT_COUNTRIES.has(countryCode.toUpperCase())
    ? "in"
    : "cm";
}
