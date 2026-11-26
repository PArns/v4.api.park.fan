# Parky's tools

Every tool Parky, park.fan's chat assistant, can call: what it answers and when it came. Open this
page before adding a tool, changing what one returns or routing a question to another one. How the
chat runs around them (the answer loop, the limits, the cards' shapes, the prompt) is
[assistant](./assistant.md); this page owns the list.

## How a tool is built

- **One list.** `ASSISTANT_TOOLS` in `src/assistant/tools/assistant-tools.ts`; the trip tools come
  from `tools/assistant-trip.ts`, the plan reading from `tools/assistant-plans.ts`. The order there
  is the order the model reads them in, and `assistant.spec.ts` pins it.
- **Description and schema** are the model's only guide to the tool. A rule about _when_ to call a
  tool lives in the prompt (`system-prompt.ts`, "# Tools" and the action rule), never in the
  description, so it stands in one place.
- **`status`** names the chip the widget shows while the tool runs (`parks`, `rides`, `forecast`,
  `posts`, `planner`, `location`, `weather`).
- **An action** (a card, a planner change, a page to open) comes back as `ActionOutcome`: the
  widget gets the action, the model a short result. Which tools act on the page is the prompt's
  action rule, mirrored by `ACTION_TOOLS` in `assistant-eval.ts`.
- **Tool results are not stored**, only the calls with their input
  ([what is stored](./assistant.md#the-key-and-what-is-stored)).
- **A new tool** gets an entry in the tool list test, at least one eval case
  (`assistant-eval.fixtures.ts`) and a row below. `assistant_tools_report` in the admin MCP reads
  the tool names from `ASSISTANT_TOOLS` and needs nothing.

## The tools

"Added" is the day the tool first appeared on `main`, with the issue or the commit that brought it.

### Parks and days

| Tool               | Answers                                                                                     | Added                |
| ------------------ | ------------------------------------------------------------------------------------------- | -------------------- |
| `search_parks`     | parks by name, city or region                                                               | 2026-10-31, PAR-1187 |
| `get_park_live`    | one park now: status, crowd level, today's hours and expectation, longest waits, fun facts  | 2026-10-31, PAR-1187 |
| `get_park_weather` | the weather now, the next 12 hours and the warnings in force                                | 2026-10-31, PAR-1187 |
| `get_best_days`    | a park's ten quietest open days ahead                                                       | 2026-10-31, PAR-1187 |
| `get_park_history` | the same day a year back, or one past day                                                   | 2026-11-05, PAR-1235 |
| `get_park_day`     | one day as the planner builds it: hours, crowd, weather, each ride's opening, peak and lull | 2026-11-06, PAR-1243 |
| `get_days`         | particular days at one park, up to a year ahead                                             | 2026-11-10, PAR-1276 |
| `get_hours`        | how a day's waits run hour by hour                                                          | 2026-11-10, PAR-1276 |
| `compare_parks`    | several parks on one day                                                                    | 2026-11-10, PAR-1265 |

### Rides, shows and food

| Tool                | Answers                                                                              | Added                |
| ------------------- | ------------------------------------------------------------------------------------ | -------------------- |
| `find_rides`        | rides by name, typo-tolerant                                                         | 2026-10-31, PAR-1187 |
| `get_ride`          | one ride: status, wait, reopening, profile, today's opening                          | 2026-10-31, PAR-1187 |
| `get_ride_forecast` | one ride's waits hour by hour on a day                                               | 2026-10-31, PAR-1187 |
| `compare_rides`     | rides ranked by a fact (speed, height, rider height …), in a park or a country       | 2026-11-02, PAR-1216 |
| `find_restaurants`  | a park's food, by name or cuisine, or nearest a place or the user (`near`, `fromMe`) | 2026-11-05, PAR-1235 |
| `get_shows`         | a park's shows with their times on a day                                             | 2026-11-05, PAR-1235 |

### Places inside a park and the way there

| Tool                   | Answers                                                                                  | Added                |
| ---------------------- | ---------------------------------------------------------------------------------------- | -------------------- |
| `find_parks_nearby`    | parks near the user, or the rides around them inside one                                 | 2026-10-31, PAR-1187 |
| `request_location`     | asks the user, in the chat, to share their position                                      | 2026-10-31, PAR-1187 |
| `get_directions`       | a Google Maps link to a park's entrance                                                  | 2026-10-31, PAR-1187 |
| `get_walking_distance` | metres and walking minutes from a place or the user to up to eight others in the park    | 2026-11-26, PAR-1437 |
| `find_nearby`          | the nearest rides, shows, restaurants, toilets and drinking water to a place or the user | 2026-11-26, PAR-1437 |

The way to one place is a card, `show_card` kind `navigate` (2026-11-26, PAR-1437), described
[below](#the-way-there).

### Trips

| Tool                    | Answers                                                            | Added                  |
| ----------------------- | ------------------------------------------------------------------ | ---------------------- |
| `plan_vacation`         | a stay of several days in a trip region, park by day               | 2026-11-12, PAR-1290   |
| `get_park_events`       | a region's season and party nights, early entry and evening hours  | 2026-11-12, PAR-1290   |
| `get_travel_time`       | between a region's parks, or from a hotel area                     | 2026-11-12, PAR-1290   |
| `find_trip_dates`       | when to go, for parks and a length                                 | 2026-11-12, PAR-1290   |
| `get_trip_plan`         | a trip the browser remembers, with its ideas                       | 2026-11-14, `fceb97e4` |
| `change_trip`           | changes a trip in the planner, with an undo                        | 2026-11-14, `fceb97e4` |
| `trip_undo`             | takes Parky's last trip change back                                | 2026-11-14, `fceb97e4` |
| `get_trip_region`       | a region's resorts, perks, cities, airports, tickets, storm season | 2026-11-14, `ff9edf99` |
| `get_trip_days`         | which park each resort is quietest on, day by day                  | 2026-11-14, `ff9edf99` |
| `get_forecast_accuracy` | how sure the forecast is, as a typical miss in minutes             | 2026-11-14, `ff9edf99` |

### The day planner

| Tool                | Does                                                 | Added                  |
| ------------------- | ---------------------------------------------------- | ---------------------- |
| `plan_day`          | puts a park day into the planner                     | 2026-10-31, PAR-1187   |
| `plan_add_rides`    | adds rides, by name or id                            | 2026-10-31, PAR-1187   |
| `plan_add_block`    | adds a break, a meal, a meeting point                | 2026-10-31, PAR-1187   |
| `plan_optimize`     | lets the planner sort the day for the shortest waits | 2026-10-31, PAR-1187   |
| `plan_add_show`     | adds a show at one of its times                      | 2026-11-05, `cb5d193e` |
| `plan_remove`       | removes entries                                      | 2026-11-05, `cb5d193e` |
| `plan_move`         | moves an entry to a time or a place in the day       | 2026-11-05, `cb5d193e` |
| `plan_update_block` | renames or moves a block of the user's own           | 2026-11-05, `cb5d193e` |
| `plan_set_party`    | sets the party's size and smallest height            | 2026-11-05, `cb5d193e` |
| `plan_mark_done`    | marks a ride as done                                 | 2026-11-05, `cb5d193e` |
| `plan_clear_day`    | empties a day, after a yes                           | 2026-11-05, `cb5d193e` |
| `plan_trip`         | puts a trip's days into the day planner              | 2026-11-05, `cb5d193e` |
| `plan_undo`         | takes Parky's last planner change back               | 2026-11-05, `cb5d193e` |
| `plan_share`        | the share link of a planned day                      | 2026-11-05, `cb5d193e` |
| `review_plan_day`   | the waits of a planned day, priced                   | 2026-11-14, `fceb97e4` |

### Posts and knowledge

| Tool               | Answers                                       | Added                          |
| ------------------ | --------------------------------------------- | ------------------------------ |
| `search_posts`     | park.fan's news and guides                    | 2026-10-31, PAR-1187           |
| `get_post`         | one post's text, best sections for a question | 2026-10-31, PAR-1187           |
| `look_up_glossary` | what a park word means                        | 2026-10-31, PAR-1188           |
| `read_knowledge`   | park.fan's own background on one topic        | 2026-11-15, PAR-1360, PAR-1361 |

### Cards, pages, alerts and the chat itself

| Tool                | Does                                                                                                              | Added                  |
| ------------------- | ----------------------------------------------------------------------------------------------------------------- | ---------------------- |
| `show_card`         | draws a live card: park, ride, rides, parks, calendar, tiles, shows, restaurants, navigate, posts, glossary, idea | 2026-10-31, PAR-1187   |
| `open_page`         | opens a page of park.fan                                                                                          | 2026-10-31, PAR-1187   |
| `submit_feedback`   | passes the user's feedback to the team                                                                            | 2026-11-04, `a1c1e981` |
| `switch_language`   | opens the site in another of its languages                                                                        | 2026-11-04, `a1c1e981` |
| `set_ride_alert`    | a push when a ride's wait drops or it reopens                                                                     | 2026-11-05, `df76ab85` |
| `set_show_reminder` | a push before a show                                                                                              | 2026-11-05, `df76ab85` |
| `remove_alert`      | removes one                                                                                                       | 2026-11-05, `df76ab85` |
| `get_favorites`     | the user's favourites with their state now                                                                        | 2026-11-05, `a1be2afd` |
| `set_favorite`      | adds or removes a favourite                                                                                       | 2026-11-05, `a1be2afd` |
| `end_chat`          | closes a chat after sustained abuse and one warning                                                               | 2026-11-06, `e4ec649d` |
| `suggest_replies`   | the reply chips under an answer                                                                                   | 2026-11-14, `fceb97e4` |
| `interview`         | leads a trip or a park day one question at a time                                                                 | 2026-11-14, `fceb97e4` |

## Places in a park

`find_restaurants`, the restaurants card, `find_nearby` and `get_walking_distance` measure inside one
park, from one of two anchors:

- **`near`**: a ride, show or restaurant of the same park, by id or name (`findPlace` in
  `tools/assistant-places.ts`: the id, else the exact name with case, accents and punctuation
  folded, else the only name containing it or contained in it; several come back to pick from).
- **`fromMe`**: the position the user shared with this message ([below](#the-users-position)). It
  never reaches the model: a result says `from: { fromTheUser: true }` and gives distances only.
  Without a position the tool answers `needsPosition` with the way to ask; farther than 3 km
  (`AT_PARK_RADIUS_M`) from the park's coordinate it answers `notAtPark`.

Lists run nearest first. `find_restaurants` and its card give at most eight with `near` or `fromMe`
(else 25 and 12, unsorted); a restaurant without a position comes last, without a walk, and the
anchor itself is left out. The card's rows stay as they are: the walk goes to the model only.
`find_nearby` gives, per kind, the `limit` nearest with a position (default 3, at most 5): `rides`
with status and standby wait now, `shows`, `restaurants` with cuisine, and `toilets` and
`drinking_water` from [OpenStreetMap](./facilities.md) with `wheelchair`, `changingTable` and `fee`
where OSM says. `get_walking_distance` measures from one anchor to up to eight places; a name it
cannot place comes back with `asked` and the reason, so the others still answer.

`metres` is the straight line rounded to 10 m. `walkMinutes` is the day planner's ceiling for a
transfer's walk (`planner/day-optimizer/leg.ts`): the straight line times `DETOUR_MAX` (1.6) at
`WALK_PARK_M_PER_MIN` (67 m a minute), at least one. Coordinates are the entities' as the source sent
them; 0/0 counts as no position. The case that asked for it: on 26 November a visitor wanted the
restaurant "under Talocan" at Phantasialand, and `query` found none, since Restaurante Cocorico's
name says nothing about the ride 20 m away.

**Toilets and drinking water** come from `FacilitiesService.publicFor`: nothing while
`FACILITIES_ENABLED` is off or the park's import is not approved, and then each asked type reads
`notKnown: true` ("not mapped for this park yet"), never "there is none". A result with facilities
carries `credit`; the prompt has Parky end an answer that names one with it as a link, once, in
the answer's language. That is the ODbL credit [facilities](./facilities.md#the-credit-a-client-owes)
asks for in a line of text, on the same screen.

### The user's position

The chat's `location` is sent only with the message after the visitor pressed „Standort teilen" in
the share card that `request_location` draws, and the browser asks for its permission on that
press. The API rounds it to four decimals, about ten metres (`chatRequestSchema`), uses it for that
one answer and stores, logs and forwards it nowhere. "This chat" tells the model whether a position
came with the message (`positionShared`), never where. Before 2026-11-26 it was rounded to two
decimals, about a kilometre, which placed a visitor in the wrong part of the park and answered no
"nearest toilet".

The prompt has Parky ask before he needs it: with no position he calls `request_location` with one
line saying what for ("so I can find the nearest toilet from where you stand"), or lets the user
name a ride they are next to, and never guesses. After the share he answers the question asked
before it. The position comes with that one message only, so a later "and the nearest water?"
needs a new share.

### The way there

`show_card` kind `navigate` with `park` and `to` (a ride, show or restaurant by name or id, or a
toilet's or drinking water's id from `find_nearby`) draws a card with the target's own position.
`park` may be left out (PAR-1438): a toilet's or a water point's id names its park, and for any
other target the park the visitor stands in is taken from the shared position. With neither, the
tool answers an error that says to pass `park`. The card:

```jsonc
{
  "kind": "navigate",
  "name": "Taron",
  "place": "ride", // ride | show | restaurant | toilets | drinking_water
  "park": "Phantasialand",
  "latitude": 50.7996591,
  "longitude": 6.8829768,
  "mapsUrl": "https://www.google.com/maps/dir/?api=1&destination=50.7996591,6.8829768&travelmode=walking",
  "url": "/parks/europe/germany/bruehl/phantasialand/taron", // the ride's page, null for others
  "credit": { "text": "© OpenStreetMap contributors", "url": "…" }, // toilets and water only
}
```

The widget reads the visitor's position and the phone's heading itself, after a press on the card,
and points an arrow at the target with the distance; the position never leaves the browser. The
model gets `fromTheUser` (metres and minutes) only when a position came with the message. A target
without a position answers `navigate: null` with the reason, and no card. park.fan's handling of
the card is in park.fan's `docs/features/assistant.md`.
