# Park facilities from OpenStreetMap (`/facilities`)

Toilets and drinking water inside a park, read from OpenStreetMap, for a map layer and for "the
nearest toilet". Open this before showing the points anywhere: the data is under the ODbL, and
showing it comes with a credit that is not optional. The feature is switched off until the owner
turns it on (`FACILITIES_ENABLED`, default `false`); [the steps](#after-a-deploy-the-owners-steps)
are below.

## The contract a client reads

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
    {
      "id": "…",
      "type": "drinking_water",
      "latitude": 50.80012,
      "longitude": 6.88102,
      "osm": { "type": "node", "id": "…", "version": 1, "timestamp": "…" },
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

- **Open route:** no key, no permission. The answer is OpenStreetMap data, and the ODbL allows no
  further terms on it (4.7, 4.8), so park.fan's data terms do not cover this route. Cache: the
  global five-minute policy, no Redis. The points change a few times a year; a client fetches them
  once per park visit, not with the live poll.
- **404** for an unknown park, and for every park while `FACILITIES_ENABLED` is not `true`. A
  client treats 404 as "no layer", silently.
- **`available: false` with an empty list is not "this park has no toilets".** Nobody has
  reviewed the park's import, or it has no outline in OSM. A client shows no layer and no "nearest
  toilet" then ([absent facts](../rules/absent-facts.md)). `available: true` always comes with at
  least one facility.
- **`type`** is `toilets` or `drinking_water` (`FACILITY_TYPES`, the one list). A park may have
  toilets and no drinking water: that type is then simply absent, and a client offers "nearest
  drinking water" only when the list holds one. A client ignores a type it does not know.
- **Per facility:** `id` (ours, stable across imports while the OSM element lives), `latitude`,
  `longitude` (WGS84; a building is placed at its centre), and, absent when OpenStreetMap does not
  say (null is stripped): `name`, `wheelchair` (`yes` | `limited` | `no`), `changingTable`, `fee`.
  Absent is unknown, never "no". `changingTable` and `wheelchair` are worth showing on a toilet;
  on drinking water they are almost never set.
- **`osm`** names the element the point was read from, its version and when it was last edited
  there. `https://www.openstreetmap.org/{type}/{id}` opens it.
- **Order:** by type, then OSM id. Distance is the client's to compute.

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

## After a deploy: the owner's steps

1. **Deploy.** Schema sync creates `park_facilities_osm` and `park_facility_imports`, empty. The
   public route answers 404; nothing has been fetched.
2. **Start the import** in the admin under Kuratieren → "Toiletten und Wasser" → "Alle Parks
   importieren" (`POST /v1/admin/facilities/import-all`). It runs for one to several hours
   ([below](#the-import-job)); the page shows the tally. If it stops because Overpass is down,
   press the button again later: it takes only what is not done.
3. **Parks marked "Kein Umriss":** look the park up on openstreetmap.org, enter its outline as
   `way/<id>` or `relation/<id>` in the park's row and import it. A park with no outline there
   stays without facilities.
4. **Review.** Open a park, compare the plotted points with the park's own map, hide a point that
   is of no use to a visitor (before the gates), approve. "Sammelfreigabe" approves in one step
   every park with at least three visible points that all lie inside the outline; it says how
   many before it does.
5. **Switch it on:** set `FACILITIES_ENABLED=true` in the API's environment (Coolify) and
   redeploy. Approved parks are served from then on; every other park answers
   `available: false`.
6. **Later:** "Neu einlesen" on a park after correcting something in OpenStreetMap (Overpass lags
   the edit by a few minutes). A re-import that brings a new point puts the park back to review.

## Import and review (admin)

Nothing is imported on a schedule or at boot. Imports are jobs on the `facilities` queue.

| Route (`/v1/admin/facilities/…`)   | Role   | What it does                                                            |
| ---------------------------------- | ------ | ----------------------------------------------------------------------- |
| `GET` (the bare prefix)            | viewer | every open park with state, last run, counts, `bulkApprovable`; `queue` |
| `POST import-all` `{ all? }`       | editor | queues the bulk run; 409 while one is queued or running                 |
| `POST approve-all` `{ dryRun? }`   | editor | the bulk approval; `dryRun` defaults to `true` and only counts          |
| `GET {parkId}`                     | viewer | the import's state with the outline's rings, and every row, hidden too  |
| `POST {parkId}/import[?boundary=]` | editor | queues this park's import, optionally with a named outline              |
| `POST {parkId}/approve`            | editor | serves the park's rows; 409 without a visible row                       |
| `POST {parkId}/withdraw`           | editor | stops serving them                                                      |
| `POST {parkId}/points/{id}/hide`   | editor | leaves one point out of the public answer                               |
| `POST {parkId}/points/{id}/show`   | editor | serves it again                                                         |

- **State per park** (`park_facility_imports`): `status` is `pending` (no import has gone
  through), `imported` (rows are there, nobody approved them) or `approved`; `lastRun` is `ok`,
  `no_outline` or `failed`, with `lastRunAt` and, when it was not `ok`, `lastRunMessage`. A park
  never tried has no row and is listed with `status: "none"`. A failed run changes neither the
  rows nor `status`: an approved park stays approved and served.
- **The outline.** A park is cut with its own outline in OSM (`tourism=theme_park` or `zoo`,
  `leisure=water_park`): the one that contains or lies within 2 km of the park's coordinate
  and carries its name (an equal name before one that only contains it); of several, the
  smallest. No outline is ever chosen by position alone. When none matches, the run ends as
  `no_outline` and its message lists what was found; an editor names one with
  `?boundary=way/31052062`. The outline is remembered for later imports, and its rings are stored
  for the review plot. An outline that is not a closed ring fails the run.
- **Idempotent.** Rows are matched by OSM element. A second import of the same data changes
  nothing; an element gone from OSM is deleted; `hidden` survives every import. An empty
  answer for a park that has rows changes nothing and fails the run.
- **Review state.** A first import is `imported` and not served. A park without a visible row
  cannot be approved (409): an approved empty list would say "no toilets here". A later import
  that brings a new point, or changes something a client shows, puts an approved park back to
  `imported`; a removal or a bare version bump does not.
- **`insideOutline`** on a row (admin only): false for a building whose centre falls outside the
  outline it touches. Such a park is reviewed by hand.
- **The bulk approval** (`isBulkApprovable`): `status` is `imported`, the last run was `ok`, at
  least three visible points, none of them outside the outline.
- **Wrong point?** Correct it in OpenStreetMap and import again. `hide` is for a point that is
  right there and wrong for a visitor, e.g. a toilet inside the outline and outside the gates.

### The import job

`FacilitiesImportProcessor` (`facilities` queue, job `import`) has one handler for both kinds of
job, so a single park asked for during a bulk run waits its turn and Overpass never sees two of
our queries at once.

- **What Overpass asks for.** The public instance's
  [usage policy](https://dev.overpass-api.de/overpass-doc/en/preface/commons.html): "users are
  expected to send a maximum of about 10000 requests per day" and "keep their download volume
  below about 1 GB per day". A full run is at most two small queries per park, about 420 requests
  and a few megabytes for 209 parks.
- **Serial, with a pause.** One park after the other in id order, ten seconds after each
  (`PAUSE_BETWEEN_PARKS_MS`): a slot's cool-down "grows with the load of the server and
  proportionate to the execution time".
- **Backoff.** A query refused with 429, 502 or 504 is sent again after 15, 30, 60 and 120
  seconds (`OVERPASS_BACKOFF_MS`; the policy: "the client framework shall resubmit the requests
  after the 15 seconds"), then the park's run fails. Three parks in a row that Overpass did not
  answer for stop the bulk run with a failed job.
- **Resumable twice over.** A run stalled by a deploy keeps its place in the job's `progress` and
  goes on behind the last park it finished (the queue allows three stalls). A run started again
  takes only the parks whose last run was not `ok`, unless `all: true`.
- **How long.** With an idle Overpass about 15 seconds a park, under an hour for all. On
  2026-11-25 the instance queued every query for about 35 seconds and refused many first tries;
  at that load a full run takes three to five hours.
- **User-Agent:** `park.fan-facilities/1.0 (+https://park.fan)`. `OVERPASS_URL` points the
  client at another instance.
- A failed bulk job shows up in the nightly failed-jobs sweep like any other
  ([job queues](../architecture/job-queues.md)).

### What the import keeps

The rules are `parseFacilities` in `src/facilities/overpass.parser.ts`, and the query is
`buildFacilitiesQuery` beside it:

1. Inside the outline: `amenity=toilets` and `amenity=drinking_water`. Nodes, ways and
   relations; a building is placed at its centre. First aid, lockers, baby-care rooms, ATMs and
   defibrillators are not read: OSM has 0 to 4 of each per park, too few for "the nearest one" to
   be true. A changing table is the `changing_table` tag on a toilet.
2. Not what a visitor cannot use: `access` = `private`, `no`, `staff`, `employees`, `delivery`.
   `customers` stays.
3. No drinking water tagged `drinking_water=no` or `drinking_water:legal=no`.
4. Of two of one type within 15 m, one: the one with more of the tags we show, a node before
   an outline. Its tags are not merged with the dropped twin's.

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
- **`hidden`, `insideOutline` and the review state** are park.fan's own columns. They leave rows
  out and change none; what is served is always a subset of OSM.

## Code

`src/facilities/`: `overpass.parser.ts` (queries, rules), `outline.geometry.ts` (rings, point in
outline), `facility-merge.ts` (what an import changes), `overpass.client.ts` (backoff),
`facilities.service.ts` (import, run state, list, review, what is served), the two controllers;
`src/queues/processors/facilities-import.processor.ts` (the job). The admin page is
`/admin/facilities` in the `park.fan` repository. Tables `park_facilities_osm` and
`park_facility_imports` are created by schema sync on deploy
([the entities are the schema](../rules/the-entities-are-the-schema.md)).
