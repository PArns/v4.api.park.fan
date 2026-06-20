/**
 * ThemeParks.wiki Exclusions (parks AND attractions)
 *
 * List of external IDs to ignore during metadata synchronization. Used to
 * handle duplicate entries or erroneous data from the source. Applied to
 * attractions (children-metadata processor) and to parks
 * (themeparks data source `discoverParks`).
 */
export const THEMEPARKS_EXCLUSIONS = [
  // Phantasialand: Duplicate "Wavy Battle" entry
  // (ID 239 was created later, ID 142 is the original/active one matched with other sources)
  "6f58dc9c-18f8-409b-84e8-d23e3d009295",

  // Six Flags Over Texas: Wiki lists "Hurricane Harbor Arlington" twice. We keep
  // the "!" variant (a96eb7c6…, matched with qt-park-40); this bare duplicate
  // (08e5d95c) would otherwise risk a duplicate park row on a future sync.
  "08e5d95c-7c73-4c65-b17a-06fede1801fb",
];
