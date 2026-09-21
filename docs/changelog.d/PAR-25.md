### Changed — one park walk behind the attraction, show and restaurant syncs

`syncAttractions`, `syncShows` and `syncRestaurants` each carried the same
skeleton: load the parks, skip the ones that are not from ThemeParks.wiki, fetch
their children, keep one entity type, map, decide update-or-insert, mint a slug
that is unique inside the park, write. That walk now lives once, in
`ThemeParksEntitySync` (`src/common/sync/theme-parks-entity-sync.ts`), and the
three services implement only what they disagree about — which children they
consume, how they read a park's existing rows, how they match a row across
sources, and how they write.

Attractions' Queue-Times and Wartezeiten branches stay outside the template;
they claim their park before the wiki path sees it, so a park still reaches
exactly one of the three sources and in the same order as before.

Two things behave differently. A park with no external ID is now skipped by the
wiki path, where it used to ask the client about `undefined`. And a park's
writes are now collected and issued after its children are mapped rather than
one ride at a time, so a park that throws half way through writes nothing
instead of its first few rides — the rows themselves, and the order of the
parks, are unchanged.
