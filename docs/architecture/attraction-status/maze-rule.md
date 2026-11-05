# The `MAZE` rule: the one verdict a job may write

`attraction_kind` is a hand-written verdict about what a thing is **for**, and nothing can be derived
from a name (`Big Thunder Mountain Railroad` sounds like transport, `Haunted Mansion` like a maze).
One corner is derivable anyway: Halloween mazes. Open this before changing
`src/attractions/utils/maze-rule.ts` or wondering why a row reads `MAZE`. The column itself:
[attraction kind](../../frontend/attraction-kind.md).

## The three marks

A row is filed `MAZE` when all three coincide:

1. an **event-horror name** — `MAZE_NAME_TOKENS` (`maze`, `haunt`, `scare`, `fright`, `slasher`,
   `hexenhaus`, …), anchored at the start of a word;
2. a **season**, resolved curated-before-detected through `resolveCuratedFacts`;
3. a season that **touches autumn** — `MAZE_SEASON_MONTHS` (September–November), or a month a
   non-cancelled `halloween` row in `park_seasons` covers.

None is enough alone: `Haunted Mansion` runs all year, and 258 seasonal rows with an autumn last month
are water slides. All three together are still circumstantial, which is what the rubric is for.

**Season evidence is read in two steps:** the resolved months, else the month of
`season_out_since`. Only 18 of 1,098 seasonal rows carried months (2026-10-06); 633 had neither and
the rule refuses them. `park_seasons` was empty that day, so the Halloween branch contributes
nothing until a season is curated; the fixed window is the floor.

## The park's rubric overrides all three (PAR-835)

Event pages list "Haunted Mazes" and "Scare Zones" separately, and some feeds write that heading into
the name. `mazeNameLabel` reads it as a **delimited segment** — the prefix before the first colon or
the suffix after the last pipe — never anywhere in the name (loosely, `haunted house` would claim
three year-round walk-throughs at LEGOLAND Billund, LEGOLAND Windsor and Lotte World).

| rubric in the name | effect | rows (2026-10-11) |
| --- | --- | --- |
| `… \| Scare Zone`, `Scare Zone: …` | refused, whatever the season says | 8 |
| `… \| Scare Maze`, `HAUNTED HOUSE: …`, `… \| Haunted Maze` | written **without** the season gate | 10 |

The season gate is a proxy for "runs only during the event", and `detect-seasonal` misses exactly the
maze that runs twelve evenings in October. A row labelled both ways is evidence of neither; the
scare-zone label wins.

## It only ever fills a blank

`UPDATE attractions SET attraction_kind = 'MAZE' WHERE id = ANY(...) AND attraction_kind IS NULL`.
An editor's answer is final, including `RIDE` on something whose name screams maze. The one
correction this cannot carry is clearing the field, which hands the row back to the rule; "a human
looked and this is not a maze" therefore lives in a `not_a_maze` [review mark](review-marks.md), and
`selectMazeRows` takes the marked ids as its third argument (with the mark: 0 of 2 Fiesta Texas scare
zones re-selected; without: 2 of 2).

The rule runs as the last attraction step of `detect-seasonal` (step 5), right after the steps that
rewrite `is_seasonal` and `season_months`; on its own cron it would read a day-old season on the
night a maze's season is first recognised. It decides in TypeScript over loaded rows (7,531 unjudged
rows, 2026-10-06) rather than in SQL, so seasonality is not resolved a second time in a second
language.

## Calibration

Against all 7,575 production names (2026-10-06): matched anywhere, `fear` claimed Liseberg's
*AtmosFear* (a drop tower) and `evil` eight `D-evil` coasters; anchored, both fall away. `voodoo` is
not a token — all four rows carrying it are rides. The first run selected 9 rows in 2 parks; two of
them (*Cirkus Bezerkus: Big Top Terror*, *Fear Acres*) are scare zones by Fiesta Texas' own page and
still read `MAZE` because their names carry no rubric. The rubric version changed +2 written and −8
refused once the detector reaches them; of 114 judged rows it would write 17, all already `MAZE`.

## What it does not reach

Of nine mazes filed by hand at Movie Park Germany the rule finds two; seven resolve as not seasonal
because `detect-seasonal` wants ≥ 20 observed OPERATING rows (`MIN_EVER_OPERATING`) and a current
CLOSED, which a maze that runs a dozen evenings and then loses its wiki id never has. On 2026-10-06,
174 unjudged rows matched the name and failed the gate (133 not seasonal at all); that list is hand
work (PAR-727). A pattern that keeps turning up there belongs in the token list.

## The verdict survives a re-issued id

The wiki gives a maze a new entity id each season. The re-issue reuses the row
([reclassification and absence](reclassification-and-absence.md#a-re-issue-is-not-a-rename)) and moves
only `externalId` and the synced fields; on a merge `attractionKind` travels in `INHERITABLE_COLUMNS`.
Both are held by specs.
