/**
 * Deciding which ride alerts just crossed their threshold.
 *
 * Pure — no database, no Redis, no clock — because a threshold crossing has
 * no calendar dimension at all (unlike `dueNotifications`, which has to reckon
 * in the park's timezone). It only ever compares this cycle's fresh reading
 * against the alert's own stored state, so the whole question is answerable
 * from two small arrays and a unit test.
 *
 * `armed` IS the dedup: no Redis marker like `push-notification.processor.ts`
 * uses for `next-up`, because a threshold crossing has no lead window to
 * overlap on consecutive ticks — it is evaluated once, on the one cycle that
 * produced the reading, and the caller persists `armed` in the same
 * transaction as the send, so a crash between the two loses at most one
 * notification rather than sending it twice.
 */

export interface RideReading {
  attractionId: string;
  /** The STANDBY wait, in minutes. Already filtered to OPERATING readings by the caller. */
  waitTime: number;
}

export interface AlertRow {
  id: string;
  subscriptionId: string;
  attractionId: string;
  thresholdMinutes: number;
  armed: boolean;
}

export interface AlertTrigger {
  alertId: string;
  subscriptionId: string;
  attractionId: string;
  waitTime: number;
  thresholdMinutes: number;
}

export interface RideAlertDiff {
  /** Alerts to send a notification for, right now. */
  triggers: AlertTrigger[];
  /** `armed` changes to persist — a subset of `alerts`, keyed by id. */
  armedUpdates: { id: string; armed: boolean }[];
}

/**
 * `readings` need not cover every alert — a ride that did not answer this
 * cycle, or answered CLOSED/DOWN, is simply absent, and its alert is left
 * exactly as it was rather than guessed at either way.
 */
export function diffRideAlerts(
  readings: RideReading[],
  alerts: AlertRow[],
): RideAlertDiff {
  const waitByAttraction = new Map(
    readings.map((r) => [r.attractionId, r.waitTime]),
  );

  const triggers: AlertTrigger[] = [];
  const armedUpdates: { id: string; armed: boolean }[] = [];

  for (const alert of alerts) {
    const waitTime = waitByAttraction.get(alert.attractionId);
    if (waitTime === undefined) continue;

    const below = waitTime < alert.thresholdMinutes;
    if (below && alert.armed) {
      // The crossing this alert exists for. Disarm in the same pass — the
      // caller persists this alongside the send, so the next cycle (still
      // below threshold) does not re-fire.
      triggers.push({
        alertId: alert.id,
        subscriptionId: alert.subscriptionId,
        attractionId: alert.attractionId,
        waitTime,
        thresholdMinutes: alert.thresholdMinutes,
      });
      armedUpdates.push({ id: alert.id, armed: false });
    } else if (!below && !alert.armed) {
      // Rearm once the wait has genuinely recovered — not a timer, so the
      // SAME alert can fire again later the same day if the queue builds
      // back up and drops again.
      armedUpdates.push({ id: alert.id, armed: true });
    }
    // below && !alert.armed: still under threshold, already fired — nothing
    // to do until it rises back above. !below && alert.armed: armed and
    // above threshold, the ordinary waiting state — nothing to do either.
  }

  return { triggers, armedUpdates };
}
