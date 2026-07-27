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

  // Hurricane Harbor New Jersey: the wiki carries the single Six Flags
  // attraction "Big Bambu and Reef Runner" twice, once spelled with "&".
  // Six Flags' own attraction page uses "and", so we keep e7f88035… and
  // drop the ampersand variant; the two names do not normalise to each
  // other, so without this the merged row would simply be recreated.
  "61193a3a-32fa-4620-9385-b1eb25611123",

  // LEGOLAND Deutschland: "Power Builder Halloween Special: Monster Trail"
  // is the Power Builder itself under a seasonal overlay (Oct 3 – Nov 9),
  // not a second ride. We keep "Power Builder" (95909742…, matched with
  // qt-ride-6887) and drop the seasonal entry.
  "caf4902f-4a8f-4df8-bcaf-0b46a986983c",
];
