import { diffRideAlerts, type AlertRow } from "./ride-alert-transitions";

/**
 * `armed` is the whole of the dedup here — no Redis marker, no lead window —
 * so the property worth pinning is the state machine itself: fire once per
 * crossing, stay silent while still under threshold, rearm once the wait
 * genuinely recovers, and never guess about a ride that did not answer.
 */
describe("diffRideAlerts", () => {
  const alert = (over: Partial<AlertRow> = {}): AlertRow => ({
    id: "alert-1",
    subscriptionId: "sub-1",
    attractionId: "taron",
    thresholdMinutes: 20,
    armed: true,
    ...over,
  });

  it("fires when an armed alert's ride drops below the threshold", () => {
    const { triggers, armedUpdates } = diffRideAlerts(
      [{ attractionId: "taron", waitTime: 15 }],
      [alert()],
    );
    expect(triggers).toEqual([
      {
        alertId: "alert-1",
        subscriptionId: "sub-1",
        attractionId: "taron",
        waitTime: 15,
        thresholdMinutes: 20,
      },
    ]);
    // Disarmed in the same pass — the caller persists this with the send.
    expect(armedUpdates).toEqual([{ id: "alert-1", armed: false }]);
  });

  it("does not fire again while still under threshold and already disarmed", () => {
    const { triggers, armedUpdates } = diffRideAlerts(
      [{ attractionId: "taron", waitTime: 10 }],
      [alert({ armed: false })],
    );
    expect(triggers).toEqual([]);
    expect(armedUpdates).toEqual([]);
  });

  it("rearms once the wait rises back to the threshold", () => {
    const { triggers, armedUpdates } = diffRideAlerts(
      [{ attractionId: "taron", waitTime: 20 }],
      [alert({ armed: false })],
    );
    expect(triggers).toEqual([]);
    expect(armedUpdates).toEqual([{ id: "alert-1", armed: true }]);
  });

  it("rearms on a wait strictly above the threshold too", () => {
    const { armedUpdates } = diffRideAlerts(
      [{ attractionId: "taron", waitTime: 45 }],
      [alert({ armed: false })],
    );
    expect(armedUpdates).toEqual([{ id: "alert-1", armed: true }]);
  });

  it("does nothing for an armed alert whose ride is still above threshold", () => {
    const { triggers, armedUpdates } = diffRideAlerts(
      [{ attractionId: "taron", waitTime: 30 }],
      [alert({ armed: true })],
    );
    expect(triggers).toEqual([]);
    expect(armedUpdates).toEqual([]);
  });

  it("treats an exact threshold match as NOT below — no trigger", () => {
    // 20 is not "below 20". An alert set to fire under 20 minutes must not
    // fire the moment the wait reads exactly 20.
    const { triggers, armedUpdates } = diffRideAlerts(
      [{ attractionId: "taron", waitTime: 20 }],
      [alert({ armed: true, thresholdMinutes: 20 })],
    );
    expect(triggers).toEqual([]);
    expect(armedUpdates).toEqual([]);
  });

  it("leaves an alert untouched when its ride did not answer this cycle", () => {
    const { triggers, armedUpdates } = diffRideAlerts(
      [{ attractionId: "some-other-ride", waitTime: 5 }],
      [alert()],
    );
    expect(triggers).toEqual([]);
    expect(armedUpdates).toEqual([]);
  });

  it("evaluates two subscribers watching the same ride independently", () => {
    const strict = alert({
      id: "a",
      subscriptionId: "s1",
      thresholdMinutes: 15,
    });
    const lenient = alert({
      id: "b",
      subscriptionId: "s2",
      thresholdMinutes: 30,
    });
    const { triggers, armedUpdates } = diffRideAlerts(
      [{ attractionId: "taron", waitTime: 20 }],
      [strict, lenient],
    );
    // 20 is below the lenient alert's 30 but not below the strict alert's 15.
    expect(triggers).toEqual([
      {
        alertId: "b",
        subscriptionId: "s2",
        attractionId: "taron",
        waitTime: 20,
        thresholdMinutes: 30,
      },
    ]);
    expect(armedUpdates).toEqual([{ id: "b", armed: false }]);
  });

  it("fires independently for alerts on different rides in one cycle", () => {
    const taron = alert({
      id: "a",
      attractionId: "taron",
      thresholdMinutes: 20,
    });
    const fLy = alert({ id: "b", attractionId: "f-l-y", thresholdMinutes: 20 });
    const { triggers } = diffRideAlerts(
      [
        { attractionId: "taron", waitTime: 10 },
        { attractionId: "f-l-y", waitTime: 10 },
      ],
      [taron, fLy],
    );
    expect(triggers.map((t) => t.attractionId).sort()).toEqual([
      "f-l-y",
      "taron",
    ]);
  });

  it("answers empty diffs for empty input", () => {
    expect(diffRideAlerts([], [])).toEqual({ triggers: [], armedUpdates: [] });
  });
});
