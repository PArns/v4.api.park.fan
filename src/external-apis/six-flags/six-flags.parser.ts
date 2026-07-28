/**
 * Reads a ride's minimum height off a sixflags.com attraction page.
 *
 * The chain renders ride facts server-side as label/value pairs:
 *
 *   <div ...>Min Height</div><p ...><span ...>52”</span></p>
 *
 * Two details matter. The number is followed by a typographic quote, not an
 * ASCII one; and "Min Height Accompanied" is a separate fact whose label
 * contains "Min Height", so the label has to be matched up to its closing tag
 * or a ride carrying both (Woodstock Express: 48" alone, 36" with an adult)
 * yields the wrong number.
 *
 * ThemeParks.wiki carries no height for these parks at all — the field is
 * absent from their entity documents — so this is the only available source
 * for roughly 1400 rides across the Six Flags and former Cedar Fair parks.
 */
const MIN_HEIGHT =
  />Min Height<\/div>\s*<p[^>]*>\s*<span[^>]*>\s*(\d{2,3})\s*[”"″]/i;

/** Outside this range the match is an artefact, not a height requirement. */
const PLAUSIBLE_INCHES = { min: 24, max: 84 };

export function parseMinHeightInches(html: string): number | null {
  const match = MIN_HEIGHT.exec(html);
  if (!match) return null;

  const inches = Number.parseInt(match[1], 10);
  if (inches < PLAUSIBLE_INCHES.min || inches > PLAUSIBLE_INCHES.max) {
    return null;
  }

  return inches;
}

export function inchesToCentimetres(inches: number): number {
  return Math.round(inches * 2.54);
}
