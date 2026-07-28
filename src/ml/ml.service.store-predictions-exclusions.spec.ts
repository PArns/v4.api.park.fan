import { isQueueTimesExcluded } from "../external-apis/queue-times/queue-times.exclusions";

/**
 * Excluding a row from ingestion stops its wait times, but it stays in
 * `attractions` until it can be deleted — and the daily generator predicts for
 * every attraction row it finds. Energylandia's fifteen turnstile counters were
 * therefore still accumulating 7,770 predictions and 86,822 accuracy rows for
 * things that are not rides.
 *
 * `storePredictions` is the one place on this side that sees both the incoming
 * predictions and the attraction rows behind them, so the filter belongs there.
 * This pins the decision rule; the wiring itself is covered by the service's
 * own specs.
 */
describe("prediction storage skips non-attraction rows", () => {
  it("recognises a counter by the id the source is keyed on", () => {
    expect(isQueueTimesExcluded("qt-ride-14585")).toBe(true);
  });

  it("leaves real rides alone, including ones merged from two sources", () => {
    // The four Universal Studios Japan rides that existed twice were merged,
    // and the survivor inherited the Queue-Times id. Excluding those ids would
    // cut a live ride off from its feed — they must never end up in the list.
    for (const id of [
      "qt-ride-14918",
      "qt-ride-14919",
      "qt-ride-15427",
      "qt-ride-15428",
    ]) {
      expect(isQueueTimesExcluded(id)).toBe(false);
    }
  });

  it("only matches exact ids, never a prefix", () => {
    expect(isQueueTimesExcluded("qt-ride-1125")).toBe(false); // 11250 exists
    expect(isQueueTimesExcluded("qt-ride-112500")).toBe(false);
    expect(isQueueTimesExcluded("qt-ride-11250")).toBe(true);
  });
});
