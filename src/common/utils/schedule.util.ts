interface ScheduleLike {
  openingTime?: Date | null;
  closingTime?: Date | null;
  scheduleType: string;
}

type FormattedSchedule = {
  openingTime: string;
  closingTime: string;
  scheduleType: string;
};

/**
 * Maps the first entry of a today-schedule array to a plain DTO.
 * Shared between LocationService and FavoritesService.
 */
export function formatTodaySchedule(
  schedule: ScheduleLike[] | null | undefined,
): FormattedSchedule | undefined {
  if (!schedule || schedule.length === 0) return undefined;
  return {
    openingTime: schedule[0].openingTime?.toISOString() ?? "",
    closingTime: schedule[0].closingTime?.toISOString() ?? "",
    scheduleType: schedule[0].scheduleType,
  };
}

/**
 * Maps a single next-schedule entry to a plain DTO.
 * Shared between LocationService and FavoritesService.
 */
export function formatNextSchedule(
  schedule: ScheduleLike | null | undefined,
): FormattedSchedule | undefined {
  if (!schedule) return undefined;
  return {
    openingTime: schedule.openingTime?.toISOString() ?? "",
    closingTime: schedule.closingTime?.toISOString() ?? "",
    scheduleType: schedule.scheduleType,
  };
}
