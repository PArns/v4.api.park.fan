# Search results (`/v1/search`)

The contract for the search response: the request parameters, the order and shape of the results, and
which fields read differently from the same field elsewhere. Open it when rendering search results or
a command palette. How a query is matched and ranked: [search](../architecture/search.md).

```
GET /v1/search?q=taron&type=park,attraction&limit=5
→ { query, results[], counts: { park, attraction, show, restaurant, location: { returned, total } } }
```

## The request

- `q`: fewer than 2 characters is a 400; a query that only gets that short once trimmed answers an
  empty result.
- `type`: comma-separated, any of `park`, `attraction`, `show`, `restaurant`, `location` (the last is
  missing from the Swagger enum). Without it, all five.
- `limit`: 1–20 per type, default 5.
- **The cache key leaves `limit` out.** Redis keeps an answer for 5 minutes under type and query
  only, so a request with another `limit` within that time gets the first one's list. The edge caches
  for 1 minute.

## The results

- **Order:** a result whose name is exactly the query first, then results whose park is open, then
  match order; all `location` results come last. Within a type the matcher's ranking decides which
  `limit` survive.
- Entries of one type with the same name in the same park appear once.
- **`counts.<type>.total` is not the number of all matches.** Each type is cut to `limit` before it is
  counted, so `total` is at most `limit`; `returned` is what is left after the duplicate filter.
- Retired parks, rides, shows and restaurants are never found.

| `type` | Carries | Read with care |
| --- | --- | --- |
| `park` | `status`, `load`, `parkHours { open, close, type }`, `resort`, coordinates | `load` and `parkHours` only while the park is open |
| `attraction` | `status`, `load`, `waitTime`, `parentPark { id, name, slug, url }` | see below |
| `show` | `showTimes[]`, `parentPark` | `showTimes` only while the park is open; `url` is always absent; coordinates are the park's |
| `restaurant` | `parentPark` | `url` is always absent; coordinates are the park's |
| `location` | `id` `city:<slug>` or `country:<slug>`, `continent`, `country`, `countryCode` | `url` is a site path (`/discovery/<continent>/<country>[/<city>]`), not an API route |

For an attraction:

- **`slug` and `url` lose a trailing `-<digits>`** (`cleanSlugSuffix`): a ride stored as `apollo-13`
  is returned as `apollo`. Resolve the ride by `id`, or by the slug the park payload serves, never by
  this one.
- **Its `latitude`/`longitude` are its park's**, not the ride's.
- **`status` reads `CLOSED` whenever the park is not open**, and also when an open park has no reading
  for the ride. It is not `effectiveStatus` and never reads `UNKNOWN` ([effective status](./effective-status.md)).
- `waitTime` is the standby wait rounded to 5, only while the park is open. **A 0-minute wait arrives
  absent**, like no wait.
- `load` is absent while the park is closed. Otherwise it is the wait against the ride's P50 (P90 when
  it has none), and `unknown` when the park is not ratable or the ride has no baseline or no wait, a
  0-minute walk-on included.

`url` on parks and attractions is the API path (`/v1/parks/…`), absent when a geo slug is missing.
