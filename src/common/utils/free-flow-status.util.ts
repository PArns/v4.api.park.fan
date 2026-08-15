import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import {
  QueueType,
  LiveStatus,
} from "../../external-apis/themeparks/themeparks.types";
import { getCurrentDateInTimezone } from "./date.util";

/**
 * Free-flow attractions: playgrounds, splash pads, climbing nets.
 *
 * They have no queue, so the upstream feed reports them CLOSED for the whole
 * day — which is wrong in the only way that matters to a visitor, because you
 * can walk onto them whenever the park is open. `attractions.open_with_park`
 * marks them, and this decides what that flag means.
 *
 * It lives here because the rule was written twice and the two copies drifted:
 * the attraction detail and the favorites list honoured the flag, the park's
 * ride list did not — and the ride list is the surface people actually look
 * at. One function, three callers, no third copy to forget.
 */

export interface FreeFlowContext {
  /** `attractions.open_with_park`. */
  openWithPark: boolean | null | undefined;
  /** The park's current status; a playground in a closed park is behind a gate. */
  parkStatus: string | null | undefined;
  /** False for parks whose wait times we cannot read at all (Hansa-Park). */
  waitTimesReadable?: boolean;
  /**
   * `attractions.season_months`, 1-based. Null or empty means "runs all year",
   * which is the case for most free-flow areas and must keep working.
   */
  seasonMonths?: number[] | null;
  /** IANA timezone of the park. "Which month is it" is a park-local question. */
  parkTimezone?: string | null;
}

/**
 * Whether a set of season months covers today, in the park's own timezone.
 *
 * Deliberately keyed on the months rather than on `is_seasonal`: the flag says
 * "this closes for part of the year" without saying which part, and a gate
 * cannot act on that. An attraction whose season we do not know stays open —
 * the same direction the rest of this file takes, where a missing fact never
 * invents a restriction.
 *
 * Months are 1-based, as `detect-seasonal` writes them (`EXTRACT(MONTH …)`).
 */
export function isInSeason(
  seasonMonths: number[] | null | undefined,
  parkTimezone?: string | null,
): boolean {
  if (!seasonMonths || seasonMonths.length === 0) return true;

  // A UTC fallback is tolerable here and nowhere else in this codebase: the
  // question is only which month it is, and the two disagree for at most a few
  // hours a year. Throwing instead would close a playground over a missing
  // column.
  const today = getCurrentDateInTimezone(parkTimezone || "UTC");
  const month = Number(today.slice(5, 7));

  return seasonMonths.includes(month);
}

/**
 * Whether a free-flow attraction should be shown as running.
 *
 * Deliberately false when the park is closed or unreadable. A playground is
 * open "whenever the park is", so a closed park closes it too — and in a park
 * whose wait times we cannot read at all, nothing below the park may claim to
 * be running (that is the Hansa-Park rule, and this must not undercut it).
 *
 * The season check exists because "whenever the park is" is not true of every
 * free-flow area: Europa-Park's water playgrounds run in summer while the park
 * itself is open all winter, and a snow playground is the same story inverted.
 * Without this gate those areas could not carry the flag at all.
 *
 * An options object rather than five positional arguments, two of them
 * optional booleans — that shape is how the original rule drifted.
 */
export function isFreeFlowOpen(context: FreeFlowContext): boolean {
  const {
    openWithPark,
    parkStatus,
    waitTimesReadable = true,
    seasonMonths,
    parkTimezone,
  } = context;

  return (
    Boolean(openWithPark) &&
    parkStatus === "OPERATING" &&
    waitTimesReadable &&
    isInSeason(seasonMonths, parkTimezone)
  );
}

/**
 * The synthetic queue row a free-flow attraction serves.
 *
 * A real 0-minute STANDBY entry rather than an empty list: clients read the
 * wait off `queues`, and "no queue rows" is how this codebase says "no data".
 * Zero minutes on a playground is the truth — you walk on.
 */
export function freeFlowQueues(): QueueDataItemDto[] {
  return [
    {
      queueType: QueueType.STANDBY,
      status: LiveStatus.OPERATING,
      waitTime: 0,
      state: null,
      returnStart: null,
      returnEnd: null,
      price: null,
      allocationStatus: null,
      currentGroupStart: null,
      currentGroupEnd: null,
      estimatedWait: null,
      lastUpdated: new Date().toISOString(),
    },
  ];
}
