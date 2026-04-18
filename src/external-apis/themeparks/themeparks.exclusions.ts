/**
 * ThemeParks.wiki Attraction Exclusions
 *
 * List of external IDs to ignore during metadata synchronization.
 * Used to handle duplicate entries or erroneous data from the source.
 */
export const THEMEPARKS_EXCLUSIONS = [
  // Phantasialand: Duplicate "Wavy Battle" entry
  // (ID 239 was created later, ID 142 is the original/active one matched with other sources)
  "6f58dc9c-18f8-409b-84e8-d23e3d009295",
];
