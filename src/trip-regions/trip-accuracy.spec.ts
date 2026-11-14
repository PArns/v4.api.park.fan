import { addIsoDays } from "../common/utils/date.util";
import { PredictionLeadSnapshotService } from "../ml/services/prediction-lead-snapshot.service";
import type { ParkDayError } from "../ml/services/prediction-lead-snapshot.service";
import {
  ACCURACY_LEAD_DAYS,
  ACCURACY_MIN_PARK_ROWS,
  ACCURACY_MIN_SAMPLE_DAYS,
  ACCURACY_WINDOW_DAYS,
  regionForecastAccuracy,
} from "./trip-accuracy";

/**
 * A trip region's forecast accuracy from its parks' daily errors: the window
 * of the 30 measured days, the mean of the parks' own errors, and null where
 * too little is measured.
 */

const YESTERDAY = "2026-11-13";
const MIN = PredictionLeadSnapshotService.MIN_SCORED;

/** `days` days ending `last`, `scored` rows a day, each off by `error` minutes. */
function rowsOf(
  parkId: string,
  last: string,
  days: number,
  scored: number,
  error: number,
): ParkDayError[] {
  return Array.from({ length: days }, (_, k) => ({
    parkId,
    targetDate: addIsoDays(last, -k),
    scored,
    errorSum: scored * error,
  }));
}

const parks = [
  { id: "a", yesterday: YESTERDAY },
  { id: "b", yesterday: YESTERDAY },
];

describe("regionForecastAccuracy", () => {
  it("measures the 30 days up to the region's last measured day, the parks' errors averaged", () => {
    // Park a measured on 40 days to the 12th, park b on 30; yesterday not yet scored.
    const rows = [
      ...rowsOf("a", "2026-11-12", 40, 6, 12),
      ...rowsOf("b", "2026-11-12", 30, 2, 20),
    ];
    expect(regionForecastAccuracy({ parks, rows, minScored: MIN })).toEqual({
      days: ACCURACY_WINDOW_DAYS,
      from: "2026-10-14",
      to: "2026-11-12",
      leadDays: ACCURACY_LEAD_DAYS,
      // Each park counts once, however many rides it has: (12 + 20) / 2.
      maeMinutes: 16,
      parks: 2,
      sampleDays: 30,
    });
  });

  it("never counts a day the park has not finished", () => {
    const rows = [
      ...rowsOf("a", "2026-11-12", 30, 6, 12),
      { parkId: "a", targetDate: "2026-11-14", scored: 6, errorSum: 600 },
    ];
    const figure = regionForecastAccuracy({
      parks: [{ id: "a", yesterday: YESTERDAY }],
      rows,
      minScored: MIN,
    });
    expect(figure).toMatchObject({ to: "2026-11-12", maeMinutes: 12 });
  });

  it("leaves out a park with too few rows, and answers null where too little is measured", () => {
    const thin = rowsOf("b", "2026-11-12", 3, 2, 90);
    expect(
      ACCURACY_MIN_PARK_ROWS > thin.reduce((s, r) => s + r.scored, 0),
    ).toBe(true);
    expect(
      regionForecastAccuracy({
        parks,
        rows: [...rowsOf("a", "2026-11-12", 30, 6, 12), ...thin],
        minScored: MIN,
      }),
    ).toMatchObject({ parks: 1, maeMinutes: 12 });
    // Under the planner's floor of scored rows.
    expect(
      regionForecastAccuracy({
        parks,
        rows: rowsOf("a", "2026-11-12", 30, 3, 12),
        minScored: MIN,
      }),
    ).toBeNull();
    // Enough rows, too few days.
    expect(
      regionForecastAccuracy({
        parks,
        rows: rowsOf("a", "2026-11-12", ACCURACY_MIN_SAMPLE_DAYS - 1, 20, 12),
        minScored: MIN,
      }),
    ).toBeNull();
    // Nothing measured, or only parks of another region.
    expect(
      regionForecastAccuracy({ parks, rows: [], minScored: MIN }),
    ).toBeNull();
    expect(
      regionForecastAccuracy({
        parks,
        rows: rowsOf("z", "2026-11-12", 30, 6, 12),
        minScored: MIN,
      }),
    ).toBeNull();
  });

  it("answers from before a closure: the window ends on the last measured day", () => {
    const figure = regionForecastAccuracy({
      parks: [{ id: "a", yesterday: YESTERDAY }],
      rows: rowsOf("a", "2026-11-01", 30, 6, 9.25),
      minScored: MIN,
    });
    expect(figure).toMatchObject({
      from: "2026-10-03",
      to: "2026-11-01",
      maeMinutes: 9.3,
      sampleDays: 30,
    });
  });
});
