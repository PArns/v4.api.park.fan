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

/**
 * What one cycle saw of a ride a `reopen` alert watches. `operating` is true
 * only for a fresh, non-heartbeat OPERATING reading of a ride in season; a
 * ride that is closed, down, refurbishing or out of season reads `false`. A
 * ride with no usable reading is simply absent.
 */
export interface ReopenReading {
  attractionId: string;
  operating: boolean;
}

export interface ReopenAlertRow {
  id: string;
  subscriptionId: string;
  attractionId: string;
  armed: boolean;
  /** Set once the alert has fired — a reopen alert is spent then and never re-arms. */
  lastTriggeredAt: Date | null;
}

export interface ReopenTrigger {
  alertId: string;
  subscriptionId: string;
  attractionId: string;
}

export interface ReopenAlertDiff {
  triggers: ReopenTrigger[];
  /** `armed` changes to persist, keyed by id. */
  armedUpdates: { id: string; armed: boolean }[];
}

/**
 * An edge trigger like `diffRideAlerts`, on a status instead of a wait, and
 * one-shot: `armed` is "last seen not operating", the first operating reading
 * fires and disarms for good. A later non-operating reading arms an alert only
 * if it has never fired. Reading the
 * status rather than the wait is what makes a ride that merely stays open
 * quiet across any number of cycles.
 */
export function diffReopenAlerts(
  readings: ReopenReading[],
  alerts: ReopenAlertRow[],
): ReopenAlertDiff {
  const operatingByAttraction = new Map(
    readings.map((r) => [r.attractionId, r.operating]),
  );
  const triggers: ReopenTrigger[] = [];
  const armedUpdates: { id: string; armed: boolean }[] = [];

  for (const alert of alerts) {
    const operating = operatingByAttraction.get(alert.attractionId);
    if (operating === undefined) continue;
    if (operating && alert.armed) {
      triggers.push({
        alertId: alert.id,
        subscriptionId: alert.subscriptionId,
        attractionId: alert.attractionId,
      });
      armedUpdates.push({ id: alert.id, armed: false });
    } else if (!operating && !alert.armed && !alert.lastTriggeredAt) {
      // Only an alert that has not fired yet: it was created against an open
      // ride and is now seeing it close. A fired one stays spent, or the
      // evening closing would arm it and tomorrow's opening would send the
      // same "open again" every day.
      armedUpdates.push({ id: alert.id, armed: true });
    }
  }

  return { triggers, armedUpdates };
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
