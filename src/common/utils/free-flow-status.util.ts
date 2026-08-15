import { QueueDataItemDto } from "../../queue-data/dto/queue-data-item.dto";
import {
  QueueType,
  LiveStatus,
} from "../../external-apis/themeparks/themeparks.types";

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

/**
 * Whether a free-flow attraction should be shown as running.
 *
 * Deliberately false when the park is closed or unreadable. A playground is
 * open "whenever the park is", so a closed park closes it too — and in a park
 * whose wait times we cannot read at all, nothing below the park may claim to
 * be running (that is the Hansa-Park rule, and this must not undercut it).
 */
export function isFreeFlowOpen(
  openWithPark: boolean | null | undefined,
  parkStatus: string | null | undefined,
  waitTimesReadable = true,
): boolean {
  return (
    Boolean(openWithPark) && parkStatus === "OPERATING" && waitTimesReadable
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
