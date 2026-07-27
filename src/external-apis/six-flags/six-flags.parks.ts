/**
 * Our park slug → the park's slug on sixflags.com.
 *
 * Six Flags absorbed Cedar Fair, so cedarpoint.com and kingsisland.com now
 * redirect there and all of these live under one site with one page layout.
 * Every entry was resolved by requesting the landing page rather than guessed
 * from the name — which is how Hersheypark and Dollywood were caught: they
 * look like they belong in this list but are operated by Hershey
 * Entertainment and Herschend, and have no page here at all.
 *
 * Keyed by our park slug; several chains reuse city names, so the resolver
 * matches on park slug and treats a miss as "not a Six Flags park".
 */
export const SIX_FLAGS_PARK_SLUGS: Readonly<Record<string, string>> = {
  "cedar-point": "cedarpoint",
  "kings-island": "kingsisland",
  "kings-dominion": "kingsdominion",
  carowinds: "carowinds",
  "dorney-park": "dorneypark",
  "worlds-of-fun": "worldsoffun",
  valleyfair: "valleyfair",
  "knotts-berry-farm": "knotts",
  "six-flags-fiesta-texas": "fiestatexas",
  "six-flags-over-texas": "overtexas",
  "six-flags-over-georgia": "overgeorgia",
  "six-flags-new-england": "newengland",
  "six-flags-great-america": "greatamerica",
  "six-flags-great-adventure": "greatadventure",
  "six-flags-great-escape": "greatescape",
  "six-flags-magic-mountain": "magicmountain",
  "six-flags-discovery-kingdom": "discoverykingdom",
  "six-flags-darien-lake": "darienlake",
  "six-flags-frontier-city": "frontiercity",
  "six-flags-mexico": "mexico",
  "six-flags-white-water-atlanta": "whitewater",
  "schlitterbahn-nb": "schlitterbahnnewbraunfels",
  "schlitterbahn-gv": "schlitterbahngalveston",
  "hurricane-harbor-arlington": "hurricaneharbortexas",
  "hurricane-harbor-splashtown": "splashtown",
  "hurricane-harbor-oklahoma-city": "hurricaneharborokc",
};

export function sixFlagsSlugFor(parkSlug: string): string | null {
  return SIX_FLAGS_PARK_SLUGS[parkSlug] ?? null;
}
