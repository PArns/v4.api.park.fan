# Smart gaps: opening hours inferred from ride activity

How the calendar fills in a park that publishes no opening hours (Efteling, Hellendoorn, many parks fed
only by wait-time sources): reconstructed hours for past days, the open/closed verdict, the seasonal
off-season rule and the flags that mark all of it as estimated. Open it before changing
`ParksService.getDerivedHistoricalHours` or the status branch of `CalendarService.buildCalendarDay`.
What `UNKNOWN` and `CLOSED` mean on the calendar is [the calendar rules](../architecture/calendar-schedule-and-ml-rules.md).

## Reconstructed hours

`ParksService.getDerivedHistoricalHours` (one window-function query, cached) works in 15-minute slots:

1. **Service points are left out**: rides whose name contains `bar`, `snack`, `restaurant`, `shop`, `cafe`,
   `corner` or `hire` would otherwise make a park look open around the clock.
2. **A slot is active** when at least 10 % of the park's rides, at least 2 and at most 10, show a wait of
   ≥ 5 minutes at the same time. Early test runs and maintenance stay below it.
3. **Opening** is the first active slot, rounded down to the hour (10:15 → 10:00); **closing** the end of the
   last active slot, rounded up (17:15 → 18:00).

## Which days it decides

Only for a park with no `OPERATING` schedule row at all (`hasOperatingSchedule = false`):

- a past day or today with reconstructed hours reads `OPERATING`, `isEstimated: true`;
- a strictly past day with no active slot reads `CLOSED`, `isEstimated: true`, and its crowd figures are
  suppressed;
- today with no activity yet stays `UNKNOWN`, so predictions still show.

Separately, any `UNKNOWN` day is set to `CLOSED` by `isClosedByOperatingRange` when it lies **between** two
operating dates, or **outside** the known operating range of a **seasonal** park (`isParkSeasonal`: a gap of
more than 21 days between `OPERATING` rows). A year-round park keeps `UNKNOWN` outside its range, so the
calendar can still show crowd predictions for months whose hours are not published yet. The yearly
predictions share the same function.

## Flags

| Flag                   | Scope | Meaning                                                                   |
| ---------------------- | ----- | ------------------------------------------------------------------------- |
| `hasOperatingSchedule` | park  | `false` when the park has no published calendar; the frontend shows a best-effort note. |
| `isEstimated`          | day   | The status or hours of this day were reconstructed from activity.          |
| `isInferred`           | hours | The opening and closing times are derived and rounded.                    |

`ParksService.getBatchHasOperatingSchedule` answers the first flag for many parks at once.

## ML

`MLService.buildFeatureContext` sends today's real opening time when the schedule has one, otherwise the
reconstructed opening from today's activity. Before the activity threshold is reached the park has no
opening time in the request, which lets the model predict a closed early morning.
