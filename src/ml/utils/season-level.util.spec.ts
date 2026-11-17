import {
  applySeasonLevel,
  isWeekend,
  seasonOn,
  type RunningSeason,
  type SeasonReference,
} from "./season-level.util";

describe("season level (PAR-1343)", () => {
  const wintertraum: RunningSeason = {
    id: "w",
    startDate: "2026-11-14",
    endDate: "2027-01-24",
    // Closed on 24 and 25 December: a listed season counts its own days only.
    dates: ["2026-11-28", "2026-12-01", "2026-12-23", "2026-12-26"],
  };
  const range: RunningSeason = {
    id: "h",
    startDate: "2026-10-03",
    endDate: "2026-11-01",
    dates: null,
  };
  const row = (attractionId: string, day: string, wait: number) => ({
    attractionId,
    predictedTime: `${day}T12:00:00`,
    predictedWaitTime: wait,
    modelVersion: "tft",
    crowdLevel: "very_low" as string,
  });
  const dayOf = (p: { predictedTime: string }) => p.predictedTime.slice(0, 10);
  const ref = (
    weekend: boolean,
    level: number,
    days = 3,
    attractionId = "taron",
  ): SeasonReference => ({
    seasonId: "w",
    attractionId,
    weekend,
    days,
    level,
  });

  it("reads a listed season by its days and a plain one by its range", () => {
    expect(seasonOn([wintertraum], "2026-12-23")?.id).toBe("w");
    expect(seasonOn([wintertraum], "2026-12-24")).toBeUndefined();
    expect(seasonOn([range], "2026-10-13")?.id).toBe("h");
    expect(seasonOn([range], "2026-11-02")).toBeUndefined();
  });

  it("knows a Saturday from a Tuesday", () => {
    expect(isWeekend("2026-11-28")).toBe(true);
    expect(isWeekend("2026-12-01")).toBe(false);
  });

  it("serves what the ride measured in the season, by day type", () => {
    const out = applySeasonLevel(
      [row("taron", "2026-11-28", 15), row("taron", "2026-12-01", 10)],
      [wintertraum],
      [ref(true, 62), ref(false, 33)],
      dayOf,
    );
    // Half-up to 5, like every served wait.
    expect(out.map((p) => p.predictedWaitTime)).toEqual([60, 35]);
    expect(out[0].modelVersion).toBe("tft+season");
    expect(out[0].crowdLevel).toBe("unknown");
  });

  it("never serves one day type what the other measured", () => {
    // The opening weekend and a Monday: three days in all, none of them
    // enough for its own day type.
    const rows = [
      row("taron", "2026-11-28", 15),
      row("taron", "2026-12-01", 10),
    ];
    expect(
      applySeasonLevel(
        rows,
        [wintertraum],
        [ref(true, 55, 2), ref(false, 25, 1)],
        dayOf,
      ),
    ).toEqual(rows);

    // A full weekday reference leaves the Saturday to the model.
    const out = applySeasonLevel(
      rows,
      [wintertraum],
      [ref(true, 55, 2), ref(false, 25)],
      dayOf,
    );
    expect(out.map((p) => p.predictedWaitTime)).toEqual([15, 25]);
  });

  it("leaves the model's number where nothing was measured yet", () => {
    const rows = [
      row("taron", "2026-11-28", 15), // two days only
      row("fly", "2026-11-28", 20), // no reference for this ride
      row("taron", "2026-12-24", 15), // not a day of the season
    ];
    const out = applySeasonLevel(
      rows,
      [wintertraum],
      [ref(true, 62, 2), ref(false, 41)],
      dayOf,
    );
    expect(out).toEqual(rows);
  });

  it("returns the list untouched without a running season", () => {
    const rows = [row("taron", "2026-11-28", 15)];
    expect(applySeasonLevel(rows, [], [ref(false, 41)], dayOf)).toBe(rows);
  });
});
