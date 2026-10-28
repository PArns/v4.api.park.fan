# Park seasons (`/seasons`)

The contract for a park's named seasons and events (Halloween, Christmas, a winter closure, a
maintenance window): the request window, each field, and what an empty list means. Open it when
rendering a season or event on a park page. How editors write seasons: [curation](../admin/curation.md).

```jsonc
// GET /v1/parks/:continent/:country/:city/:park/seasons?from=2026-10-01&to=2026-11-30
{ "parkId": "…", "from": "2026-10-01", "to": "2026-11-30",
  "seasons": [
    { "id": "…", "kind": "halloween", "name": "Fright Nights",
      "startDate": "2026-10-03", "endDate": "2026-11-01",
      "dates": ["2026-10-03", "2026-10-04", "2026-10-10", "…"],
      "status": "confirmed", "separateTicket": true, "priceFrom": "39.50", "priceCurrency": "EUR",
      "opensAt": "18:00", "closesAt": "23:00",
      "url": "https://…", "sourceUrl": "https://…", "confirmedAt": "2026-08-19T10:12:00.000Z" } ] }
```

A request of its own rather than a field on the park payload: that payload is re-polled every five
minutes, and seasons change a few times a year (`ParkSeasonsController`, `src/parks/park-seasons.controller.ts`).

## The window

- `from` and `to` are park-local `YYYY-MM-DD`; a date the calendar does not have is a 400.
- `from` defaults to the park's today. **`to` defaults to a year from today, not from `from`**, so a
  `from` more than a year ahead without a `to` returns nothing.
- A season is returned when it overlaps the window at all (`startDate ≤ to`, `endDate ≥ from`),
  ordered by `startDate`. The response echoes the window it used.
- **An empty `seasons` means nobody has curated one.** Nothing syncs seasons; every row is written by
  hand, so absence says nothing about the park ([absent facts](../rules/absent-facts.md)).

## A season

| Field | Meaning |
| --- | --- |
| `kind` | `halloween`, `christmas`, `summer_nights`, `special_event`, `opening` (the park's own operating season), `closure` (deliberately shut), `maintenance` (a ride or area down inside a normal season). The strings are contract (`PARK_SEASON_KINDS`); translate them |
| `name` | the park's own brand name, never translated; absent → show the label for `kind` |
| `startDate`, `endDate` | the outer bounds, park-local, both inclusive |
| `dates` | **absent: every day between the bounds.** Otherwise the sorted list of the days the season actually runs; never empty. Render a range only when it is absent |
| `status` | `confirmed`, `announced`, `expected` (the park did this last year and has announced nothing yet), `cancelled`. Say which; `expected` dates are not a promise |
| `separateTicket` | `true` when a normal day ticket is not enough |
| `priceFrom`, `priceCurrency` | the cheapest advertised price as a decimal **string** (`"39.50"`) and its ISO 4217 code; a price never comes without a currency |
| `opensAt`, `closesAt` | the event's hours, park-local `HH:MM`. Independent of the schedule rows: an evening event can have no `OPERATING` row at all |
| `attractionIds` | the rides the season is about (the ride under maintenance, the rides an overlay re-themes) |
| `url` | the park's own page for the event |
| `sourceUrl`, `confirmedAt` | where the dates were read and when they were last checked against it; show "as of" from `confirmedAt`, not from any edit date |

The editor's `note` and the admin account that last wrote the row stay in the admin API.

## Caching

The route sets no policy of its own, so the edge gets the global 5-minute one
([edge caching](../deployment/edge-and-throttling.md#cache-headers)); there is no Redis cache. An
editor's write publishes like any curated change (`AdminCurationService.publish`,
[publish outside-in](../rules/publish-outside-in.md)), so the old answer can live at the edge for up to
that window.
