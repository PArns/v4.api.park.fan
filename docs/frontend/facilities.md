# Park facilities from OpenStreetMap (`/facilities`)

Toilets, drinking water and a few other visitor facilities inside a park, read from
OpenStreetMap, for a map layer and for "the nearest toilet". Open this before showing the
points anywhere: the data is under the ODbL, and showing it comes with a credit that is not
optional. The feature is a draft and switched off (`FACILITIES_ENABLED`, default `false`).

```
GET /v1/parks/{continent}/{country}/{city}/{park}/facilities
```

```jsonc
{
  "parkId": "…",
  "available": true,
  "importedAt": "2026-11-25T14:02:11.000Z",
  "facilities": [
    {
      "id": "…",
      "type": "toilets",
      "latitude": 50.79991,
      "longitude": 6.87934,
      "wheelchair": "yes",
      "changingTable": true,
      "fee": false,
      "osm": {
        "type": "node",
        "id": "1382986304",
        "version": 6,
        "timestamp": "2023-09-08T10:29:36.000Z",
      },
    },
  ],
  "attribution": {
    "text": "© OpenStreetMap contributors",
    "url": "https://www.openstreetmap.org/copyright",
    "license": "ODbL-1.0",
    "licenseUrl": "https://opendatacommons.org/licenses/odbl/1-0/",
  },
}
```

- **`available: false` with an empty list is not "this park has no toilets".** It means nobody
  has reviewed the park's import. A client shows no layer and no "nearest toilet" then
  ([absent facts](../rules/absent-facts.md)).
- **404** for an unknown park, and for every park while `FACILITIES_ENABLED` is not `true`.
- `type` is one of `toilets`, `drinking_water`, `baby_care`, `first_aid`, `lockers`, `atm`,
  `defibrillator`. A client ignores a type it does not know.
- `name`, `wheelchair` (`yes` | `limited` | `no`), `changingTable` and `fee` are absent when
  OpenStreetMap does not say (null is stripped). Absent is unknown, never "no".
- `osm` names the element the point was read from, with its version and the time it was last
  edited there. `https://www.openstreetmap.org/{type}/{id}` opens it.
- No key and no permission: the route is open. The answer is OpenStreetMap data, and the ODbL
  allows no further terms on it (4.7, 4.8), so park.fan's data terms do not cover this route.
  Cache: the global five-minute policy, no Redis.

## The credit a client owes

Whoever shows these points credits OpenStreetMap and says the data is under the Open Database
License. That is ODbL 4.3 and the OSMF's
[attribution guidelines](https://osmfoundation.org/wiki/Licence/Attribution_Guidelines):

- **On a map:** `attribution.text` in a corner of the map or directly beside it, linked to
  `attribution.url`, readable without the visitor having to tap anything first. It may collapse
  after a dismiss, after the map is moved, or after five seconds, as long as it stays findable
  (an "i" button, the data sources page).
- **The base map does not matter.** The credit is for the points. On an Apple map it stands
  beside Apple's own legal link and must not cover it; write it so nobody reads it as the
  credit for the map itself, e.g. "Toilets and drinking water: © OpenStreetMap contributors".
- **In a list or a line of text** ("nearest toilet 80 m"): the credit sits on the same screen
  or in the sources sheet the screen links to, and OpenStreetMap is in the app's and the site's
  list of data sources with the link.
- Only where the points are shown. A park page without the layer carries no OSM credit.

## Import and review (admin)

Nothing is imported on a schedule or at boot. An editor imports one park and approves it.

| Route (`/v1/admin/facilities/…`)    | Role   | What it does                                                     |
| ----------------------------------- | ------ | ---------------------------------------------------------------- |
| `GET {parkId}`                      | viewer | the import's state and every row, hidden ones included           |
| `POST {parkId}/import[?boundary=…]` | editor | reads the park from Overpass now and answers with the difference |
| `POST {parkId}/approve`             | editor | serves the park's rows                                           |
| `POST {parkId}/withdraw`            | editor | stops serving them                                               |
| `POST {parkId}/points/{id}/hide`    | editor | leaves one point out of the public answer                        |
| `POST {parkId}/points/{id}/show`    | editor | serves it again                                                  |

- **The outline.** A park is cut with its own outline in OSM (`tourism=theme_park` or `zoo`,
  `leisure=water_park`): the one that contains or lies within 2 km of the park's coordinate
  and carries its name (an equal name before one that only contains it); of several, the
  smallest. No outline is ever chosen by position alone.
  When none matches, the import answers 409 and lists what it found, and the editor names one:
  `?boundary=way/31052062`. The outline is remembered for later imports.
- **Idempotent.** Rows are matched by OSM element. A second import of the same data changes
  nothing; an element gone from OSM is deleted; `hidden` survives every import. An empty
  answer for a park that has rows changes nothing and answers 409.
- **Review state.** A first import is `imported` and not served. A park without a visible
  row cannot be approved (409): an approved empty list would say "no toilets here". A later import that brings a
  new point, or changes something a client shows, puts an approved park back to `imported`;
  a removal or a bare version bump does not.
- **The request does the work** (one or two Overpass queries, usually seconds, 90 s at most)
  and Overpass is not retried: a busy server answers 502 and the editor tries again later.
- **Wrong point?** Correct it in OpenStreetMap and import again. `hide` is for a point that is
  right there and wrong for a visitor, e.g. a toilet inside the outline and outside the gates.

### What the import keeps

The rules are `parseFacilities` in `src/facilities/overpass.parser.ts`, and the query is
`buildFacilitiesQuery` beside it:

1. Inside the outline: `amenity` = `toilets`, `drinking_water`, `nursing_room` / `baby_care`,
   `first_aid`, `locker` / `lockers` / `luggage_locker`, `atm`; `emergency` = `first_aid`,
   `defibrillator`; `healthcare=first_aid`. Nodes, ways and relations; an outline is placed at
   its centre.
2. Not what a visitor cannot use: `access` = `private`, `no`, `staff`, `employees`, `delivery`.
   `customers` stays.
3. No drinking water tagged `drinking_water=no` or `drinking_water:legal=no`.
4. Of two of one type within 15 m, one: the one with more of the tags we show, a node before
   an outline.

## The ODbL and how we meet it

OpenStreetMap data is under the [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/).
What follows is park.fan's reading of the licence and the OSMF guidelines, not legal advice.

- **`park_facilities_osm` is a database derived from OSM** (a systematic extract, ODbL 4.4 b),
  and it holds OSM-derived rows only. It is under the ODbL. Parks, rides and waits are a
  separate database beside it that refers to it by `park_id`: a Collective Database (4.5 a,
  [guideline](https://osmfoundation.org/wiki/Licence/Community_Guidelines/Collective_Database_Guideline_Guideline)),
  so they stay ours.
- **Notice (4.2, 4.3):** `attribution` in every answer, this page, and the credit above.
- **Share alike (4.4) and access (4.6):** the public route is the derived database, in machine
  readable form, free, without a key and without added terms; the query and the rules above
  are the method (4.6 b), in a public repository.
- **No hand-typed rows, ever.** A point added or corrected by hand in this table, or a second
  table of "our own toilets" filling OSM's gaps, would be an improvement of OSM data that has
  to be shared under the ODbL as well
  ([horizontal layers guideline](https://osmfoundation.org/wiki/Licence/Community_Guidelines/Horizontal_Map_Layers_-_Guideline)).
  Corrections go to OpenStreetMap, where the next import picks them up. And nothing is ever
  copied into OSM from a park's own map, from Apple Maps or from Google Maps: OSM's terms and
  theirs both forbid it. Being there, or imagery OSM licenses, is the source.
- **`hidden` and the review state** are park.fan's own columns. They leave rows out and change
  none; what is served is always a subset of OSM.

## Code

`src/facilities/`: `overpass.parser.ts` (queries, rules), `facility-merge.ts` (what an import
changes), `facilities.service.ts` (import, review, what is served), the two controllers.
Tables `park_facilities_osm` and `park_facility_imports` are created by schema sync on deploy
([the entities are the schema](../rules/the-entities-are-the-schema.md)).
