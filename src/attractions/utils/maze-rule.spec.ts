import {
  MAZE_NAME_TOKENS,
  MAZE_SEASON_MONTHS,
  isMazeByRule,
  isMazeName,
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
