# Where you stay dry (`indoorOutdoor`)

A hand-decided value on every attraction, on the ride endpoint and on every
ride in a park payload:

```json
{ "indoorOutdoor": "covered_queue" }
```

Three values, from `INDOOR_OUTDOOR_VALUES`, which the Swagger enum and the
admin editor's dropdown both read:

| Value           | Queue   | Ride    |
| --------------- | ------- | ------- |
| `indoor`        | covered | covered |
| `covered_queue` | covered | outside |
| `outdoor`       | outside | outside |

`covered_queue` is its own value because a rain plan treats it differently
from both neighbours: you wait dry, and the ride may still stop for weather.

## Null is neither

`null` means nobody has checked. On the day the column landed (PAR-424) that
was every attraction. A reader must behave exactly as it did before the field
existed: a rain plan that reads null as outdoor sends people away from dry
rides, and one that reads it as indoor sends them into the rain. The rule is
[absent facts](../rules/absent-facts.md).

## Where the values come from

No feed. ThemeParks.wiki, Queue-Times and Wartezeiten.app carry nothing like
it, and RCDB and park websites are off-limits by their terms (frontend repo,
`docs/product/attraction-metadata-sources.md`). Every value is written through
the curated-field editor, and the source goes into the `reason` and
`sourceUrl` of the curation's audit row. That row is the record of where a
value came from.

The column appears through `synchronize` on deploy, so a fill cannot run
before the change is deployed.
