# Where you stay dry (`indoorOutdoor`)

The contract for the hand-decided `indoor` / `covered_queue` / `outdoor` value a rain
plan reads, why `null` is neither, and where values come from. On the ride endpoint
and on every ride in a park payload:

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

`null` means nobody has checked (the column started empty, PAR-424). A reader must behave exactly as it did before the field
existed: a rain plan that reads null as outdoor sends people away from dry
rides, and one that reads it as indoor sends them into the rain. The rule is
[absent facts](../rules/absent-facts.md).

## Where the values come from

No feed. ThemeParks.wiki, Queue-Times and Wartezeiten.app carry nothing like
it, and the terms of RCDB and the park websites forbid ingesting or scraping
them (frontend repo, `docs/product/attraction-metadata-sources.md`). An editor
researches the park's own page and writes the value through the curated-field
editor; the source goes into the `reason` and `sourceUrl` of the curation's
audit row, which is the record of where a value came from.
