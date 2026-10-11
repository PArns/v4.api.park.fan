import {
  MAZE_NAME_LABELS,
  MAZE_NAME_TOKENS,
  MAZE_SEASON_MONTHS,
  SCARE_ZONE_NAME_LABELS,
  isMazeByRule,
  isMazeName,
  mazeNameLabel,
  mazeNamePatternSql,
  mazeSeasonEvidenceMonths,
  type MazeRuleRow,
  selectMazeRows,
} from "./maze-rule";

/**
 * The rule that files a Halloween maze without anybody typing it.
 *
 * Every fixture below is a real production row as of 2026-10-06, because the
 * two mistakes this rule can make are both name-shaped and neither is
 * imaginable from the token list alone: `AtmosFear` is a 116 m drop tower and
 * `Jersey Devil Coaster` is a coaster, and a pattern matched anywhere in the
 * name files both as walk-through horror attractions.
 */
describe("isMazeName", () => {
  it.each([
    ["Fear Acres", "fear at the start of a word"],
    ["Evil Dead Burn - Express", "evil at the start of a word"],
    ["Killer Klowns - Express", "kill, suffix free"],
    ["Hexenhaus presented by SNICKERS®", "a German event name"],
    ["Cinema Slasher presented by M&M'S®", "slasher"],
    ["Carnival of Terrors", "terror mid-name but word-initial"],
    ["Twisted: Theater of Torment presented by SKITTLES®", "torment"],
    ["The Tenement | Scare Maze", "scare and maze"],
    ["Haunted Hollow", "haunt reaching Haunted"],
    ["Scare Zone: Kill Cute Party", "scare after punctuation"],
    ["Streets of the Undead", "undead"],
    ["Il Labirinto Di Halloween", "halloween"],
  ])("matches %s (%s)", (name) => {
    expect(isMazeName(name)).toBe(true);
  });

  /**
   * The anchor, measured. Matched anywhere in the name, `fear` took Liseberg's
   * AtmosFear and `evil` took all eight `D-evil` rows in the catalogue.
   */
  it.each([
    ["AtmosFear", "fear is not at the start of a word"],
    ["Jersey Devil Coaster", "evil is inside Devil"],
    ["Dare Devil Dive", "same"],
    ["Daredevil Falls", "same"],
    ["Lil' Devil Coaster", "same"],
    ["All-New Tasmanian Devil: Behing the Scenes Experience", "same"],
    ["Amazing Adventures", "maze is inside Amazing"],
    [
      "Voodoo Express",
      "voodoo is not a token — it names a theme, not an event",
    ],
    ["Voodoo Bayou", "same"],
    ["Spooky Fun House", "spook is not a token either"],
    ["Big Thunder Mountain Railroad", "nothing horror about it"],
  ])("does not match %s (%s)", (name) => {
    expect(isMazeName(name)).toBe(false);
  });

  it("keeps every token a bare lowercase word", () => {
    // The tokens are pasted into one alternation. A token carrying `|`, `(`
    // or `.` would change the pattern rather than extend it, and the damage
    // would read as a mysteriously broad rule rather than as a typo.
    for (const token of MAZE_NAME_TOKENS) {
      expect(token).toMatch(/^[a-z]+$/);
    }
    expect(new Set(MAZE_NAME_TOKENS).size).toBe(MAZE_NAME_TOKENS.length);
  });

  it("builds the SQL pattern from the same tokens", () => {
    const pattern = mazeNamePatternSql();
    expect(pattern).toBe(`(^|[^[:alnum:]])(${MAZE_NAME_TOKENS.join("|")})`);
  });
});

/**
 * The rubric a feed writes into the name. Every fixture is a production row as
 * of 2026-10-11, and the three LEGOLAND/Lotte World rows at the bottom are the
 * reason the match is anchored to a delimited segment: a loose search for
 * `haunted house` files three year-round walk-throughs as Halloween mazes.
 */
describe("mazeNameLabel", () => {
  it.each([
    ["Purgatory Town | Scare Zone", "Thorpe Park, rubric after the pipe"],
    ["Creature Campus | Scare Zone", "same"],
    ["Lucifer's Lair | Scare Zone", "same"],
    ["Scare Zone: M3GAN", "Universal Studios Beijing, rubric before the colon"],
    ["Scare Zone: Mel's Die In", "same"],
    ["SCARE ZONE: shouting it", "the comparison drops case"],
    ["Zone | Scare-Zone", "and punctuation"],
  ])("reads %s as a scare zone (%s)", (name) => {
    expect(mazeNameLabel(name)).toBe("SCARE_ZONE");
  });

  it.each([
    ["DeadBeat | Scare Maze", "Thorpe Park"],
    ["The Tenement | Scare Maze", "same"],
    ["HAUNTED HOUSE: Big Top Terror", "Six Flags Great America"],
    [
      "HAUNTED HOUSE: SAW: Legacy of Terror",
      "the prefix stops at the first colon",
    ],
    [
      "Tartaros | Haunted Maze",
      "the event pages' own heading, not yet in a feed",
    ],
  ])("reads %s as a maze (%s)", (name) => {
    expect(mazeNameLabel(name)).toBe("MAZE");
  });

  it.each([
    [
      "Ghost – The Haunted House",
      "LEGOLAND Billund, a year-round walk-through",
    ],
    ["Haunted House Monster Party", "LEGOLAND Windsor, same"],
    ["Haunted House", "Lotte World Adventure, same"],
    [
      "The Catacombs Haunted House",
      "Parc Asterix, the rubric is not a segment",
    ],
    ["Cirkus Bezerkus: Big Top Terror", "a prefix that is not a rubric"],
    ["Creature Campus | Reunion Reckoning", "a suffix that is not a rubric"],
    ["Fire Show | Lucifer's Lair", "the zone's name, not the rubric"],
    ["IT: The 4D Experience® | Ages 15+", "an age rating"],
    ["Fear Acres", "no delimiter at all"],
  ])("reads no rubric in %s (%s)", (name) => {
    expect(mazeNameLabel(name)).toBeNull();
  });

  /**
   * A row labelled both ways is not evidence of either. `NULL` means "not
   * decided yet" and costs a reader nothing; a wrong `MAZE` is a claim.
   */
  it("lets the scare zone win when a name carries both rubrics", () => {
    expect(mazeNameLabel("Scare Zone: Something | Scare Maze")).toBe(
      "SCARE_ZONE",
    );
  });

  it("keeps both vocabularies normalised the way the match expects", () => {
    // The segment is compared by equality after its punctuation and case come
    // off, so an entry carrying either would never match anything.
    for (const label of [...MAZE_NAME_LABELS, ...SCARE_ZONE_NAME_LABELS]) {
      expect(label).toMatch(/^[a-z0-9]+$/);
    }
  });
});

describe("mazeSeasonEvidenceMonths", () => {
  it("takes the resolved month list when there is one", () => {
    expect(
      mazeSeasonEvidenceMonths({
        isSeasonal: true,
        seasonMonths: [9, 10, 11],
        seasonOutSince: null,
      }),
    ).toEqual([9, 10, 11]);
  });

  it("falls back to the month of seasonOutSince", () => {
    // Universal Studios Hollywood's houses: flagged seasonal, no months
    // derivable, last seen running on 2026-09-05.
    expect(
      mazeSeasonEvidenceMonths({
        isSeasonal: true,
        seasonMonths: null,
        seasonOutSince: "2026-09-05",
      }),
    ).toEqual([9]);
  });

  it("prefers the month list over seasonOutSince", () => {
    expect(
      mazeSeasonEvidenceMonths({
        isSeasonal: true,
        seasonMonths: [10],
        seasonOutSince: "2026-03-01",
      }),
    ).toEqual([10]);
  });

  /**
   * 633 of 1,098 seasonal production rows are in this state. "Seasonal, and we
   * do not know when" is not evidence of Halloween, and treating it as such
   * would file every winter-closed water slide as a maze.
   */
  it("is null for a seasonal row with no evidence at all", () => {
    expect(
      mazeSeasonEvidenceMonths({
        isSeasonal: true,
        seasonMonths: null,
        seasonOutSince: null,
      }),
    ).toBeNull();
    expect(
      mazeSeasonEvidenceMonths({
        isSeasonal: true,
        seasonMonths: [],
        seasonOutSince: null,
      }),
    ).toBeNull();
  });

  it("is null for a row that is not seasonal", () => {
    expect(
      mazeSeasonEvidenceMonths({
        isSeasonal: false,
        seasonMonths: [10],
        seasonOutSince: "2026-10-01",
      }),
    ).toBeNull();
  });
});

describe("isMazeByRule", () => {
  const maze = {
    name: "Fear Acres",
    isSeasonal: true,
    seasonMonths: [9, 10, 11],
    seasonOutSince: null,
  };

  it("takes a seasonal autumn attraction with an event-horror name", () => {
    expect(isMazeByRule(maze)).toBe(true);
  });

  it("refuses it once the name no longer matches", () => {
    expect(isMazeByRule({ ...maze, name: "Thunder Rapids" })).toBe(false);
  });

  it("refuses it once the seasonality is gone", () => {
    expect(isMazeByRule({ ...maze, isSeasonal: false })).toBe(false);
  });

  /**
   * The condition that carries the whole rule. `Haunted Mansion` is a
   * year-round dark ride at a year-round park: the name matches and the season
   * does not, which is exactly the case the type docstring says must stay a
   * RIDE.
   */
  it("refuses Haunted Mansion, which runs all year", () => {
    expect(
      isMazeByRule({
        name: "Haunted Mansion",
        isSeasonal: false,
        seasonMonths: null,
        seasonOutSince: null,
      }),
    ).toBe(false);
  });

  it("refuses a seasonal horror name whose season is the summer", () => {
    expect(
      isMazeByRule({
        ...maze,
        seasonMonths: null,
        seasonOutSince: "2026-06-30",
      }),
    ).toBe(false);
  });

  const august = () => ({
    ...maze,
    seasonMonths: null,
    seasonOutSince: "2026-08-20",
  });

  it("takes a season outside autumn when the park's Halloween season covers it", () => {
    expect(isMazeByRule(august())).toBe(false);
    expect(isMazeByRule(august(), [8, 9])).toBe(true);
  });

  it("does not let a park season widen the rule past the name or the season", () => {
    expect(isMazeByRule({ ...august(), name: "Tidal Wave Bay" }, [8, 9])).toBe(
      false,
    );
    expect(isMazeByRule({ ...august(), isSeasonal: false }, [8, 9])).toBe(
      false,
    );
  });

  it("holds the autumn window at September to November", () => {
    expect([...MAZE_SEASON_MONTHS]).toEqual([9, 10, 11]);
  });

  /**
   * The error PAR-835 is about. `Purgatory Town | Scare Zone` is an open-air
   * area with scare actors at Thorpe Park's Fright Nights: the name matches
   * `scare`, the season is the same Halloween season as the mazes beside it,
   * and the row is still not a walk-through horror attraction.
   */
  it("refuses a scare zone even though name, season and window all hold", () => {
    const scareZone = { ...maze, name: "Purgatory Town | Scare Zone" };
    expect(isMazeName(scareZone.name)).toBe(true);
    expect(mazeSeasonEvidenceMonths(scareZone)).toEqual([9, 10, 11]);
    expect(isMazeByRule(scareZone)).toBe(false);
  });

  it("refuses a scare zone the park's own Halloween months would have taken", () => {
    expect(
      isMazeByRule(
        {
          ...maze,
          name: "Scare Zone: M3GAN",
          seasonMonths: null,
          seasonOutSince: "2026-08-20",
        },
        [8, 9],
      ),
    ).toBe(false);
  });

  /**
   * The other half of the change: a park that files a row under its own
   * haunted-house heading has said what the season gate was guessing at. Both
   * of these are production rows that sat unjudged because `detect-seasonal`
   * never flagged them.
   */
  it("takes a maze label without any season evidence", () => {
    const unflagged = {
      name: "HAUNTED HOUSE: Big Top Terror",
      isSeasonal: false,
      seasonMonths: null,
      seasonOutSince: null,
    };
    expect(mazeSeasonEvidenceMonths(unflagged)).toBeNull();
    expect(isMazeByRule(unflagged)).toBe(true);
    expect(isMazeByRule({ ...unflagged, name: "DeadBeat | Scare Maze" })).toBe(
      true,
    );
  });

  /**
   * The label drops the season gate, not the whole rule — without a label the
   * same row stays out, which is what makes the test above a statement about
   * the label rather than about the fixture.
   */
  it("keeps an unflagged row out when it carries no label", () => {
    expect(
      isMazeByRule({
        name: "Big Top Terror",
        isSeasonal: false,
        seasonMonths: null,
        seasonOutSince: null,
      }),
    ).toBe(false);
  });

  it("does not let a maze label reach past a summer season it never needed", () => {
    // The label answers the season question outright, so a summer month is
    // not a counter-argument any more. Stated here so the next reader does
    // not take the summer case above as a general guarantee.
    expect(
      isMazeByRule({
        name: "Tartaros | Haunted Maze",
        isSeasonal: true,
        seasonMonths: [6, 7],
        seasonOutSince: null,
      }),
    ).toBe(true);
  });
});

describe("selectMazeRows", () => {
  const row = (over: Partial<MazeRuleRow> = {}): MazeRuleRow => ({
    id: "row-1",
    parkId: "park-1",
    attractionKind: null,
    name: "Fear Acres",
    curatedName: null,
    isSeasonal: true,
    curatedIsSeasonal: null,
    seasonMonths: [9, 10, 11],
    curatedSeasonMonths: null,
    seasonOutSince: null,
    ...over,
  });

  const select = (
    rows: MazeRuleRow[],
    halloween: Array<[string, number[]]> = [],
  ) => selectMazeRows(rows, new Map(halloween));

  it("selects the row the rule matches", () => {
    expect(select([row()])).toEqual(["row-1"]);
  });

  /**
   * The guarantee of the whole rule: an editor's verdict is final. Both halves
   * matter — a row already filed as MAZE is left alone so the rule is
   * idempotent, and a row filed as something else is left alone even when its
   * name screams maze.
   */
  it("never touches a row that already carries a kind", () => {
    expect(select([row({ attractionKind: "MAZE" })])).toEqual([]);
    expect(select([row({ attractionKind: "RIDE" })])).toEqual([]);
    expect(select([row({ attractionKind: "WALKTHROUGH" })])).toEqual([]);
  });

  it("resolves curated seasonality over the detector's", () => {
    // The detector says not seasonal; an editor says it is, with months. The
    // curated statement takes the whole question over.
    expect(
      select([
        row({
          isSeasonal: false,
          curatedIsSeasonal: true,
          seasonMonths: null,
          curatedSeasonMonths: [9, 10, 11],
        }),
      ]),
    ).toEqual(["row-1"]);

    // And the other way round: an editor says "the detector is wrong, this is
    // not seasonal", which has to take the detector's months down with it.
    expect(
      select([
        row({
          isSeasonal: true,
          curatedIsSeasonal: false,
          seasonMonths: [10],
        }),
      ]),
    ).toEqual([]);
  });

  it("reads the curated name, which is the one a visitor sees", () => {
    expect(select([row({ name: "Walibi Fright Nights House 3" })])).toEqual([
      "row-1",
    ]);
    // A curated name that is not a maze name wins over a synced one that is.
    expect(
      select([
        row({ name: "Walibi Fright Nights House 3", curatedName: "Excalibur" }),
      ]),
    ).toEqual([]);
  });

  it("applies each park's own Halloween months to that park only", () => {
    const rows = [
      row({
        id: "a",
        parkId: "park-1",
        seasonMonths: null,
        seasonOutSince: "2026-08-20",
      }),
      row({
        id: "b",
        parkId: "park-2",
        seasonMonths: null,
        seasonOutSince: "2026-08-20",
      }),
    ];
    expect(select(rows, [["park-1", [8, 9]]])).toEqual(["a"]);
  });

  /**
   * The guarantee of the whole change, at the level the processor calls: a
   * labelled scare zone stays out and a labelled maze comes in, and the
   * editor's verdict still wins over both.
   */
  it("reads the rubric a feed wrote into the name", () => {
    expect(
      select([
        row({ id: "zone", name: "Purgatory Town | Scare Zone" }),
        row({
          id: "house",
          name: "HAUNTED HOUSE: Big Top Terror",
          isSeasonal: false,
          seasonMonths: null,
        }),
      ]),
    ).toEqual(["house"]);
  });

  it("leaves a labelled row alone once somebody has judged it", () => {
    expect(
      select([
        row({
          name: "HAUNTED HOUSE: Big Top Terror",
          attractionKind: "WALKTHROUGH",
          isSeasonal: false,
          seasonMonths: null,
        }),
      ]),
    ).toEqual([]);
  });

  it("reads the rubric off the curated name when there is one", () => {
    // The curated name is the one a visitor sees, so it is also the one whose
    // rubric counts — in both directions.
    expect(
      select([
        row({
          name: "Purgatory Town | Scare Zone",
          curatedName: "Purgatory Town | Scare Maze",
        }),
      ]),
    ).toEqual(["row-1"]);
    expect(
      select([
        row({
          name: "DeadBeat | Scare Maze",
          curatedName: "DeadBeat | Scare Zone",
        }),
      ]),
    ).toEqual([]);
  });

  it("keeps the water park out", () => {
    // 258 production rows look like this: seasonal, last ran in September,
    // and not a maze. The name is the only thing standing between them and a
    // wrong verdict, which is why the token list is calibrated rather than
    // generous.
    const slides = [
      "Thunder Rapids Water Coaster",
      "Tidal Wave Bay",
      "Riptide Racer",
      "White Water Canyon",
      "Water Maze",
    ].map((name, i) =>
      row({
        id: `slide-${i}`,
        name,
        seasonMonths: null,
        seasonOutSince: "2026-09-21",
      }),
    );
    // `Water Maze` is the one that gets through, and it is the token list's
    // known cost: a water-park maze is still a maze by name. Nothing else in
    // the catalogue reaches the rule through `maze` alone (measured
    // 2026-10-06: zero rows).
    expect(select(slides)).toEqual(["slide-4"]);
  });
});
