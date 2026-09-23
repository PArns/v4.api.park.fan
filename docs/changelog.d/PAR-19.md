### Added — `/plan/day` says how well watched a ride's opening time is

Every `opensAt` now travels with an `opensAtConfidence` of `high`, `medium` or
`low`, bucketed from the number of days its median was taken over (40 and 20
days, the boundaries the rope-drop recommendation already uses for the same
count). It is present exactly when `opensAt` is, because it grades that time.

The two lookup keys of one ride were indistinguishable before. An opening is
keyed on the park's own opening that day — the gap between gate and ride is
seasonal — and a park opens at its off-season hour on about twenty mornings a
year. Measured against production on 2026-09-23, Black Mamba's answer for a
09:00 gate rested on 176 observed days and its answer for an 11:00 gate on 23,
and both were served as the same bare `HH:mm`. Across every park open that day,
19.0 % of the 2757 served times rest on fewer than 20 days.

`low` is a thin answer, never a placeholder: five observed openings remain the
floor for an answer to exist, and below it `opensAt` stays absent.
