import type { OptimizeStop } from "./optimize";
import { MAX_PLANNED_MINUTE, type PlannerEntry } from "./types";

/**
 * An entry of the day as a plan leaves it. A ride the plan adds has no id yet: the caller's store
 * mints ids (park.fan's `applyPlan`), and an id made up here would collide with one it holds.
 */
export type PlannedEntry = Omit<PlannerEntry, "id"> & {
  id: string | null;
  /** Set on a ride the plan adds. */
  added?: true;
};

function clampMinute(minute: number, ceiling: number): number {
  if (!Number.isFinite(minute)) return 0;
  return Math.max(0, Math.min(ceiling, Math.round(minute)));
}

/**
 * The day with a plan laid over it, in start order: what park.fan's `applyPlan` writes, minus the
 * ids. It moves and adds and never removes, so an entry the plan does not name keeps its minute,
 * and a show keeps its performance's minute whatever a stop says.
 */
export function planEntries(
  entries: readonly PlannerEntry[],
  stops: readonly Pick<
    OptimizeStop,
    "entryId" | "attractionSlug" | "attractionName" | "startMinute"
  >[],
): PlannedEntry[] {
  const moved = new Map<string, number>();
  const added: PlannedEntry[] = [];
  for (const stop of stops) {
    const minute = clampMinute(stop.startMinute, MAX_PLANNED_MINUTE);
    if (stop.entryId) {
      moved.set(stop.entryId, minute);
      continue;
    }
    added.push({
      id: null,
      added: true,
      attractionSlug: stop.attractionSlug,
      attractionName: stop.attractionName,
      startMinute: minute,
    });
  }

  const next: PlannedEntry[] = [
    ...entries.map((entry) => {
      const minute = moved.get(entry.id);
      return minute === undefined ||
        minute === entry.startMinute ||
        entry.showSlug
        ? entry
        : { ...entry, startMinute: minute };
    }),
    ...added,
  ];
  return next
    .map((entry, index) => ({ entry, index }))
    .sort(
      (a, b) => a.entry.startMinute - b.entry.startMinute || a.index - b.index,
    )
    .map(({ entry }) => entry);
}
