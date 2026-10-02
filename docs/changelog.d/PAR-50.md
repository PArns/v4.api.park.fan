### Added — show coordinates on `/plan/day`

Each entry in `shows` now carries `latitude` and `longitude`, taken from the
`shows` row the endpoint already loads, so no query was added. Like the ride
coordinates they are numbers, and `null` where the show has no stored position
(an empty column becomes `null`, never 0/0). This prepares the planner to count
the walk to a show (PAR-102). `docs/frontend/plan-day-endpoint.md` §8 says what
a missing value means.
