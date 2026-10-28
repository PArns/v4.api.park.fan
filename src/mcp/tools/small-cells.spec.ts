import {
  MCP_MIN_CELL,
  OTHER_ROW,
  foldCrosstab,
  foldSmallCounts,
  foldSmallRows,
  hideSmallInSeries,
  isNarrowed,
} from "./small-cells";

const BY_VALUE = {
  dims: ["value"],
  count: "views",
  sum: ["views", "visits", "share"],
};

/**
 * What the tools fold. What is pinned: a row counting fewer than five goes
 * into one `(other)` row at the end, its additive measures summed and
 * everything else empty; rows that are not small and lists without small rows
 * come back as they were; visitors decide when a row has them; a row that is
 * already `(other)` is one with the folded rows; a crosstab folds its sides,
 * then its cells, and its numbers still add up to the total; a series hides
 * the values from one to four and what is computed from them.
 */
describe("small-cells", () => {
  it("counts a row small below five, not at five", () => {
    expect(MCP_MIN_CELL).toBe(5);
    expect(
      foldSmallRows(
        [
          { value: "a", views: 5 },
          { value: "b", views: 4 },
        ],
        { dims: ["value"], count: "views", sum: ["views"] },
      ),
    ).toEqual([
      { value: "a", views: 5 },
      { value: "(other)", views: 4 },
    ]);
  });

  describe("foldSmallRows", () => {
    it("leaves a list without a small row as it is, an empty list too", () => {
      const rows = [
        { value: "a", views: 50, bounceRate: 0.2 },
        { value: "b", views: 5, bounceRate: 0.5 },
      ];
      expect(foldSmallRows(rows, BY_VALUE)).toEqual(rows);
      expect(foldSmallRows([], BY_VALUE)).toEqual([]);
    });

    it("folds the small rows into one, last, with the sums of the additive measures and nothing else", () => {
      const rows = [
        {
          value: "google",
          views: 50,
          visits: 40,
          share: 0.5,
          bounceRate: 0.2,
          avgVisitSeconds: 90,
          exitRate: 0.1,
        },
        {
          value: "bing",
          views: 3,
          visits: 2,
          share: 0.03,
          bounceRate: 0.5,
          avgVisitSeconds: 10,
          exitRate: 0.3,
        },
        {
          value: "ecosia",
          views: 49,
          visits: 30,
          share: 0.49,
          bounceRate: 0.4,
          avgVisitSeconds: 70,
          exitRate: 0.2,
        },
        {
          value: "qwant",
          views: 1,
          visits: 1,
          share: 0.01,
          bounceRate: 1,
          avgVisitSeconds: 5,
          exitRate: 1,
        },
      ];
      expect(foldSmallRows(rows, BY_VALUE)).toEqual([
        rows[0],
        rows[2],
        {
          value: "(other)",
          views: 4,
          visits: 3,
          share: 0.04,
          bounceRate: null,
          avgVisitSeconds: null,
          exitRate: null,
        },
      ]);
    });

    it("folds a list of nothing but small rows into the one row, and a single small row too", () => {
      expect(
        foldSmallRows(
          [
            { value: "a", views: 1, avg: 3 },
            { value: "b", views: 2, avg: 4 },
          ],
          { dims: ["value"], count: "views", sum: ["views"] },
        ),
      ).toEqual([{ value: "(other)", views: 3, avg: null }]);
      expect(
        foldSmallRows([{ value: "a", views: 2, avg: 3 }], {
          dims: ["value"],
          count: "views",
          sum: ["views"],
        }),
      ).toEqual([{ value: "(other)", views: 2, avg: null }]);
    });

    it("adds a key only that a folded row has, and leaves a key no folded row has off", () => {
      const rows = [
        { value: "a", views: 50, exits: 5 },
        { value: "b", views: 2 },
      ];
      expect(
        foldSmallRows(rows, { ...BY_VALUE, sum: ["views", "exits"] }),
      ).toEqual([
        { value: "a", views: 50, exits: 5 },
        { value: "(other)", views: 2 },
      ]);
    });

    it("leaves a sum empty when a folded row has none of it", () => {
      expect(
        foldSmallRows(
          [
            { value: "a", views: 2, previous: 1 },
            { value: "b", views: 1, previous: null },
          ],
          { dims: ["value"], count: "views", sum: ["views", "previous"] },
        ),
      ).toEqual([{ value: "(other)", views: 3, previous: null }]);
    });

    it("names every dimension of a row by two values (other)", () => {
      expect(
        foldSmallRows(
          [
            { surface: "hero", pageType: "home", opened: 30 },
            { surface: "hero", pageType: "park", opened: 2 },
            { surface: "header", pageType: "news", opened: 1 },
          ],
          {
            dims: ["surface", "pageType"],
            count: "opened",
            sum: ["opened"],
          },
        ),
      ).toEqual([
        { surface: "hero", pageType: "home", opened: 30 },
        { surface: "(other)", pageType: "(other)", opened: 3 },
      ]);
    });

    it("counts a row by its visitors when it has them, by its count when they are null", () => {
      const spec = {
        dims: ["value"],
        count: "views",
        sum: ["views", "visitors"],
      };
      expect(
        foldSmallRows(
          [
            { value: "few readers, many views", views: 100, visitors: 3 },
            { value: "many readers, few views", views: 2, visitors: 50 },
            { value: "no visitors counted", views: 2, visitors: null },
            {
              value: "no visitors counted, many views",
              views: 80,
              visitors: null,
            },
          ],
          spec,
        ),
      ).toEqual([
        { value: "many readers, few views", views: 2, visitors: 50 },
        { value: "no visitors counted, many views", views: 80, visitors: null },
        { value: "(other)", views: 102, visitors: null },
      ]);
    });

    describe("a row that is already (other)", () => {
      const guard = {
        value: "(other)",
        views: 900,
        share: 0.9,
        bounceRate: 0.3,
      };

      it("is one row with the folded ones, its own measures summed in and its rates emptied", () => {
        expect(
          foldSmallRows(
            [
              { value: "a", views: 50, share: 0.05, bounceRate: 0.1 },
              guard,
              { value: "b", views: 3, share: 0.003, bounceRate: 0.4 },
            ],
            BY_VALUE,
          ),
        ).toEqual([
          { value: "a", views: 50, share: 0.05, bounceRate: 0.1 },
          { value: "(other)", views: 903, share: 0.903, bounceRate: null },
        ]);
      });

      it("stays as it is when nothing else is small, whatever its size", () => {
        const rows = [{ value: "a", views: 50, share: 0.05 }, guard];
        expect(foldSmallRows(rows, BY_VALUE)).toEqual(rows);
        const tiny = [
          { value: "a", views: 50 },
          { ...guard, views: 2 },
        ];
        expect(foldSmallRows(tiny, BY_VALUE)).toEqual(tiny);
      });
    });

    it("does not touch the rows it was given", () => {
      const rows = [
        { value: "a", views: 50 },
        { value: "b", views: 2 },
      ];
      const copy = structuredClone(rows);
      foldSmallRows(rows, BY_VALUE);
      expect(rows).toEqual(copy);
    });
  });

  describe("foldSmallCounts", () => {
    it("folds a map of counts, a page's reads per language, like a list", () => {
      expect(foldSmallCounts({ de: 40, en: 3, nl: 1 })).toEqual({
        de: 40,
        "(other)": 4,
      });
      expect(foldSmallCounts({ de: 40, en: 9 })).toEqual({ de: 40, en: 9 });
    });
  });

  describe("foldCrosstab", () => {
    const both = { row: true, col: true };

    it("answers the matrix as it is when no value and no cell is small", () => {
      expect(
        foldCrosstab(
          ["hero", "header"],
          ["home", "park"],
          [
            { row: "hero", col: "home", count: 40 },
            { row: "header", col: "park", count: 25 },
            { row: "header", col: "home", count: 5 },
          ],
          both,
        ),
      ).toEqual({
        rows: ["hero", "header"],
        cols: ["home", "park"],
        counts: [
          [40, 0],
          [5, 25],
        ],
        other: null,
      });
    });

    it("folds a side value whose cells add up to under five, and merges its cells", () => {
      const folded = foldCrosstab(
        ["hero", "header", "keyboard"],
        ["home", "park"],
        [
          { row: "hero", col: "home", count: 40 },
          { row: "hero", col: "park", count: 20 },
          { row: "header", col: "home", count: 2 },
          { row: "keyboard", col: "home", count: 1 },
          { row: "keyboard", col: "park", count: 1 },
        ],
        both,
      );
      expect(folded.rows).toEqual(["hero", "(other)"]);
      expect(folded.cols).toEqual(["home", "park"]);
      // header and keyboard are 2 + 1 + 1 = 4 together: under (other), where
      // the cells of the two are one cell each, small, hidden.
      expect(folded.counts).toEqual([
        [40, 20],
        [null, null],
      ]);
      expect(folded.other).toEqual({ cells: 2, count: 4 });
    });

    it("hides a small cell between two values that are not small, and says how many and how much", () => {
      const folded = foldCrosstab(
        ["hero", "header"],
        ["home", "park"],
        [
          { row: "hero", col: "home", count: 40 },
          { row: "hero", col: "park", count: 3 },
          { row: "header", col: "home", count: 4 },
          { row: "header", col: "park", count: 30 },
        ],
        both,
      );
      expect(folded).toEqual({
        rows: ["hero", "header"],
        cols: ["home", "park"],
        counts: [
          [40, null],
          [null, 30],
        ],
        other: { cells: 2, count: 7 },
      });
    });

    it("keeps the numbers of the matrix and other adding up to the total", () => {
      const cells = [
        { row: "a", col: "x", count: 60 },
        { row: "a", col: "y", count: 2 },
        { row: "b", col: "x", count: 3 },
        { row: "b", col: "y", count: 30 },
        { row: "c", col: "x", count: 1 },
      ];
      const folded = foldCrosstab(["a", "b", "c"], ["x", "y"], cells, both);
      const sum =
        folded.counts.flat().reduce<number>((s, n) => s + (n ?? 0), 0) +
        (folded.other?.count ?? 0);
      expect(sum).toBe(96);
    });

    it("does not fold a day side: its periods are not values", () => {
      const folded = foldCrosstab(
        ["hero"],
        ["2026-10-27", "2026-10-28"],
        [
          { row: "hero", col: "2026-10-27", count: 2 },
          { row: "hero", col: "2026-10-28", count: 30 },
        ],
        { row: true, col: false },
      );
      expect(folded.cols).toEqual(["2026-10-27", "2026-10-28"]);
      expect(folded.counts).toEqual([[null, 30]]);
      expect(folded.other).toEqual({ cells: 1, count: 2 });
    });

    it("takes a side value that is literally (other), or past the top thirty, as a value, and one (other) row of the two", () => {
      const folded = foldCrosstab(
        ["a", "(other)", "__rest__", "b"],
        ["x"],
        [
          { row: "a", col: "x", count: 50 },
          { row: "(other)", col: "x", count: 100 },
          { row: "__rest__", col: "x", count: 40 },
          { row: "b", col: "x", count: 1 },
        ],
        both,
      );
      // The (other) row, whatever its origin, comes last.
      expect(folded.rows).toEqual(["a", "__rest__", "(other)"]);
      expect(folded.counts).toEqual([[50], [40], [101]]);
      expect(folded.other).toBeNull();
    });

    it("leaves a cell that is not there at zero, which no reader is behind", () => {
      const folded = foldCrosstab(
        ["a", "b"],
        ["x", "y"],
        [
          { row: "a", col: "x", count: 50 },
          { row: "b", col: "y", count: 50 },
        ],
        both,
      );
      expect(folded.counts).toEqual([
        [50, 0],
        [0, 50],
      ]);
    });
  });

  describe("isNarrowed", () => {
    it("is true under a filter or a language only", () => {
      expect(isNarrowed({})).toBe(false);
      expect(isNarrowed({ filters: [] })).toBe(false);
      expect(isNarrowed({ filters: [{}] })).toBe(true);
      expect(isNarrowed({ locale: "de" })).toBe(true);
      expect(isNarrowed({ locale: undefined, filters: undefined })).toBe(false);
    });
  });

  describe("hideSmallInSeries", () => {
    const points = [
      { day: "a", pageviews: 0, visits: 0, bounceRate: 0 },
      { day: "b", pageviews: 4, visits: 1, bounceRate: 0.5 },
      { day: "c", pageviews: 5, visits: 20, bounceRate: 0.3 },
    ];

    it("hides the counts from one to four, keeps zero and five, and says it hid", () => {
      const hidden = hideSmallInSeries(points, ["pageviews", "visits"]);
      expect(hidden.series).toEqual([
        { day: "a", pageviews: 0, visits: 0, bounceRate: 0 },
        { day: "b", pageviews: null, visits: null, bounceRate: 0.5 },
        { day: "c", pageviews: 5, visits: 20, bounceRate: 0.3 },
      ]);
      expect(hidden.hidden).toBe(true);
    });

    it("hides what is computed from a hidden count", () => {
      const hidden = hideSmallInSeries(points, ["pageviews", "visits"], {
        bounceRate: ["visits"],
      });
      expect(hidden.series[1]).toEqual({
        day: "b",
        pageviews: null,
        visits: null,
        bounceRate: null,
      });
      expect(hidden.series[2].bounceRate).toBe(0.3);
    });

    it("says it hid nothing when no value was small, and keeps the series as it was", () => {
      const big = [{ day: "a", pageviews: 50 }];
      const hidden = hideSmallInSeries(big, ["pageviews"]);
      expect(hidden).toEqual({ series: big, hidden: false });
    });

    it("does not touch the points it was given", () => {
      const copy = structuredClone(points);
      hideSmallInSeries(points, ["pageviews"]);
      expect(points).toEqual(copy);
    });

    it("leaves a null count and a derived null as they are", () => {
      expect(
        hideSmallInSeries(
          [{ day: "a", visitors: null, avgVisitSeconds: null, visits: 2 }],
          ["visitors"],
          { avgVisitSeconds: ["visits"] },
        ),
      ).toEqual({
        series: [
          { day: "a", visitors: null, avgVisitSeconds: null, visits: 2 },
        ],
        hidden: false,
      });
    });
  });

  it("names the folded row as the guard names the values past its limit", () => {
    expect(OTHER_ROW).toBe("(other)");
  });
});
