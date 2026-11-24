# Too cold to get wet (`tooColdToGetWet`)

The contract for the key that says a ride is no tip right now because it gets its riders wet and
the park is cold (PAR-1420). It sits on the rides of the park payload, on the attraction detail
response and on the rides of the `in_park` answer of [discovery](./discovery.md):

```json
"mayGetWet": true,
"tooColdToGetWet": true
```

## When it is present

Only when both hold, decided by `wetInTheCold` (`src/parks/utils/wet-ride-weather.util.ts`):

- the ride's `mayGetWet` is `true`; `null` (unknown) and `false` never get the key;
- the park feels below **15 °C** now (`WET_RIDE_MIN_FEELS_LIKE_C`): `weather.now.apparentTemperature`
  of the park payload, `weather.now.temperature` where the felt one is missing.

A park without a reading now marks nothing. The attraction detail and the `in_park` answer read the
reading from the park's cached payload (`park:integrated:<id>`), so a cache miss marks nothing
either. Otherwise the key is absent, never `false`.

## What it means, and what it does not

The ride stays what it is: `isHeadliner`, its status and its wait are untouched. The key says why
the ride is no tip right now: its queue is short because nobody wants to sit wet in the cold, and
nobody crosses the park for it. On 2026-11-24, at 2.2 °C (felt -2.1 °C), Phantasialand's Chiapas
showed 5 minutes against a usual 40.

What reads it:

- **`shortWaits`** of discovery leaves such a ride out, by the same rule (`pickShortWaits`).
- **Parky** gets it on `get_ride`, on `get_park_live`'s longest waits and on the rows of
  `show_card` kind rides. The prompt lets him name the ride as a headliner, never as a walk-on, a
  short wait, a rope-drop ride or a tip without saying it is too cold to get soaked
  ([assistant](./assistant.md)).
- **The frontend** leaves it off the compass's headliners, Parky's walk-on stamps and lines, and
  the park page's rope-drop picks and badges.

`ropeDropHeadliners` and each ride's `ropeDrop` are not filtered here: the park page renders them
from a snapshot that can be a day old, while the key comes with its five-minute poll, so the page
drops a cold ride itself and picks it up again when it warms.
