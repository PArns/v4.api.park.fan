import {
  clickShare,
  keywordMove,
  keywordMovers,
  keywordSummary,
  keywordWeeks,
  type KeywordRow,
  type KeywordWeek,
} from "./search-console-keywords";

const week = (position: number | null, impressions = 100): KeywordWeek => ({
  clicks: 0,
  impressions: position === null ? 0 : impressions,
  position,
});

const row = (
  query: string,
  now: number | null,
  before: number | null,
): KeywordRow => {
  const current = week(now);
  const previous = week(before);
  return {
    query,
    page: null,
    current,
    previous,
    ...keywordMove(current, previous),
  };
};

describe("keyword report", () => {
  it("takes the last complete Monday-to-Sunday week and the one before", () => {
    // 2026-11-14 is a Saturday: that week has not ended, so the one before it.
    expect(keywordWeeks("2026-11-14")).toEqual({
      current: { from: "2026-11-02", to: "2026-11-08" },
      previous: { from: "2026-10-26", to: "2026-11-01" },
    });
    // A Sunday ends its own week.
    expect(keywordWeeks("2026-11-15").current).toEqual({
      from: "2026-11-09",
      to: "2026-11-15",
    });
    // An earlier week by any day in it; a day after the data falls back to the last week.
    expect(keywordWeeks("2026-11-15", "2026-10-21").current.from).toBe(
      "2026-10-19",
    );
    expect(keywordWeeks("2026-11-15", "2026-12-01").current.to).toBe(
      "2026-11-15",
    );
  });

  it("calls a move of a place or more up or down, and marks new and lost keywords", () => {
    expect(keywordMove(week(3), week(5.5))).toEqual({
      change: 2.5,
      trend: "up",
    });
    expect(keywordMove(week(8), week(4))).toEqual({
      change: -4,
      trend: "down",
    });
    expect(keywordMove(week(4.2), week(4.8))).toEqual({
      change: 0.6,
      trend: "same",
    });
    expect(keywordMove(week(7), week(null)).trend).toBe("new");
    expect(keywordMove(week(null), week(7)).trend).toBe("lost");
  });

  it("follows the click curve: most at 1, nothing beyond page two", () => {
    expect(clickShare(1)).toBe(0.28);
    expect(clickShare(1.4)).toBe(0.28);
    expect(clickShare(15)).toBe(0.01);
    expect(clickShare(21)).toBe(0);
    expect(clickShare(null)).toBe(0);
  });

  it("sums visibility, top 3, top 10, risers, fallers, new and lost", () => {
    const rows = [
      row("a", 1, 3),
      row("b", 9, 2),
      row("c", 4, null),
      row("d", null, 12),
    ];
    const summary = keywordSummary(rows);
    expect(summary.top3).toEqual({ current: 1, previous: 2 });
    expect(summary.top10).toEqual({ current: 3, previous: 2 });
    expect(summary.risers).toEqual({ keywords: 1, places: 2 });
    expect(summary.fallers).toEqual({ keywords: 1, places: 7 });
    expect(summary.new).toBe(1);
    expect(summary.lost).toBe(1);
    expect(summary.visibility.current).toBeGreaterThan(0);
    expect(summary.visibility.change).toBeCloseTo(
      summary.visibility.current - summary.visibility.previous,
      2,
    );
    expect(keywordSummary([row("top", 1, 1)]).visibility.current).toBe(100);
  });

  it("lists the biggest moves each way", () => {
    const movers = keywordMovers([
      row("a", 2, 5),
      row("b", 1, 9),
      row("c", 9, 4),
    ]);
    expect(movers.risers.map((item) => item.query)).toEqual(["b", "a"]);
    expect(movers.fallers.map((item) => item.query)).toEqual(["c"]);
  });
});
