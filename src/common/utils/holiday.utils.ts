import { addIsoDays, formatInParkTimezone } from "./date.util";
import { formatInTimeZone, fromZonedTime } from "date-fns-tz";
import { addDays } from "date-fns";
import type { HolidayRow } from "../../holidays/holidays.service";
import { normalizeRegionCode } from "./region.util";

/**
 * Holiday Calculation Utilities
 *
 * Centralized logic for determining if a date is a holiday, bridge day,
 * or weekend extension of a holiday. This logic is used in both TypeScript
 * services and can be replicated in the ML service (Python).
 *
 * Rules:
 * 1. Direct holiday: Date is in the holiday map
 * 2. Bridge day: Friday after Thursday holiday OR Monday before Tuesday holiday
 * 3. Weekend extension: Saturday/Sunday after Friday holiday
 */

export interface HolidayInfo {
  isHoliday: boolean;
  holidayName: string | null;
  isBridgeDay: boolean;
  holidayType: HolidayType | null; // Type of holiday: "public", "school", "bank", "observance", or null
  isPublicHoliday: boolean; // Convenience: true if holidayType is "public" or "bank"
  isSchoolHoliday: boolean; // Convenience: true if holidayType is "school"
}

/**
 * Holiday type: "public" for public holidays, "school" for school holidays
 * Bridge days only apply to public holidays, not school holidays
 */
export type HolidayType =
  "public" | "school" | "observance" | "bank" | "bridge";

/**
 * Holiday entry with type information
 */
export interface HolidayEntry {
  name: string;
  type: HolidayType;
  // Support for multiple types on the same day (e.g., Public + School)
  allTypes?: HolidayType[];
}

/**
 * Calculate holiday information for a given date
 *
 * @param date - The date to check
 * @param holidayMap - Map of date strings (YYYY-MM-DD) to holiday names or HolidayEntry objects
 * @param timezone - IANA timezone for date calculations (e.g., "Europe/Berlin")
 * @returns Holiday information including isHoliday, holidayName, and isBridgeDay
 *
 * @example
 * const holidayMap = new Map([
 *   ["2025-12-25", "Christmas Day"],
 *   ["2025-12-26", "Boxing Day"]
 * ]);
 * const info = calculateHolidayInfo(new Date("2025-12-27"), holidayMap, "Europe/Berlin");
 * // Returns: { isHoliday: true, holidayName: "Christmas Day", isBridgeDay: false }
 */
export function calculateHolidayInfo(
  date: Date,
  holidayMap: Map<string, string | HolidayEntry>,
  timezone: string,
): HolidayInfo {
  const dateStr = formatInParkTimezone(date, timezone);
  const holidayEntry = holidayMap.get(dateStr);
  let holidayName: string | null = null;
  let holidayType: HolidayType | null = null;

  // Use formatInTimeZone to get day of week in target timezone (1=Mon, ..., 7=Sun)
  // We convert to 0-6 (0=Sun, 1=Mon, ..., 6=Sat) to match JS getDay()
  const dayOfWeekIso = Number(formatInTimeZone(date, timezone, "i"));
  const dayOfWeek = dayOfWeekIso === 7 ? 0 : dayOfWeekIso;

  // Helper to check if a day in the map is a school holiday
  const isSchoolHolidayInMap = (dateString: string): boolean => {
    const entry = holidayMap.get(dateString);
    if (!entry) return false;
    if (typeof entry === "string") return false; // String default is public
    return (
      entry.type === "school" || (entry.allTypes?.includes("school") ?? false)
    );
  };

  const isPublicHolidayInMap = (dateString: string): boolean => {
    const entry = holidayMap.get(dateString);
    if (!entry) return false;
    if (typeof entry === "string") return true;
    return (
      entry.type === "public" ||
      entry.type === "bank" ||
      (entry.allTypes?.some((t) => t === "public" || t === "bank") ?? false)
    );
  };

  const currentIsPublic = isPublicHolidayInMap(dateStr);
  const currentIsSchool = isSchoolHolidayInMap(dateStr);

  // Initial determination
  holidayName = holidayEntry
    ? typeof holidayEntry === "string"
      ? holidayEntry
      : holidayEntry.name
    : null;

  const currentIsBridge =
    holidayEntry &&
    typeof holidayEntry !== "string" &&
    (holidayEntry.type === "bridge" ||
      holidayEntry.allTypes?.includes("bridge"));

  // Weekend Extension Logic (Bidirectional & Deep)
  // School holidays extend to weekends if:
  // - The PRECEDING Friday is a school holiday OR a Public Holiday adjacent to a School Holiday
  // - The FOLLOWING Monday is a school holiday OR a Public Holiday adjacent to a School Holiday
  let isWeekendBonus = false;
  if (!currentIsSchool && (dayOfWeek === 0 || dayOfWeek === 6)) {
    // 1. Check Friday backward (from Sat or Sun)
    const daysBackToFriday = dayOfWeek === 6 ? 1 : 2;
    const fridayDate = addDays(date, -daysBackToFriday);
    const fridayStr = formatInParkTimezone(fridayDate, timezone);

    // 2. Check Monday forward (from Sat or Sun)
    const daysForwardToMonday = dayOfWeek === 6 ? 2 : 1;
    const mondayDate = addDays(date, daysForwardToMonday);
    const mondayStr = formatInParkTimezone(mondayDate, timezone);

    const isFridaySchool = isSchoolHolidayInMap(fridayStr);
    const isFridayPublic = isPublicHolidayInMap(fridayStr);

    const isMondaySchool = isSchoolHolidayInMap(mondayStr);
    const isMondayPublic = isPublicHolidayInMap(mondayStr);

    // Deep check: If Friday/Monday is Public, does IT attach to a School holiday?
    let isFridayEffectiveSchool = isFridaySchool;
    if (isFridayPublic && !isFridaySchool) {
      // Check Thursday
      const thursdayDate = addDays(fridayDate, -1);
      const thursdayStr = formatInParkTimezone(thursdayDate, timezone);
      if (isSchoolHolidayInMap(thursdayStr)) {
        isFridayEffectiveSchool = true;
      }
    }

    let isMondayEffectiveSchool = isMondaySchool;
    if (isMondayPublic && !isMondaySchool) {
      // Check Tuesday
      const tuesdayDate = addDays(mondayDate, 1);
      const tuesdayStr = formatInParkTimezone(tuesdayDate, timezone);
      if (isSchoolHolidayInMap(tuesdayStr)) {
        isMondayEffectiveSchool = true;
      }
    }

    if (isFridayEffectiveSchool || isMondayEffectiveSchool) {
      isWeekendBonus = true;
      // Improved Naming Logic:
      // Try to find the name from the "Effective School Holiday" source
      if (!holidayName) {
        if (isFridayEffectiveSchool) {
          const friEntry = holidayMap.get(fridayStr);
          if (isFridaySchool && friEntry) {
            holidayName =
              typeof friEntry === "string" ? friEntry : friEntry.name;
          } else if (isFridayPublic) {
            // It was public, but effective school because of Thursday
            const thuEntry = holidayMap.get(
              formatInParkTimezone(addDays(fridayDate, -1), timezone),
            );
            if (thuEntry)
              holidayName =
                typeof thuEntry === "string" ? thuEntry : thuEntry.name;
          }
        } else if (isMondayEffectiveSchool) {
          const monEntry = holidayMap.get(mondayStr);
          if (isMondaySchool && monEntry) {
            holidayName =
              typeof monEntry === "string" ? monEntry : monEntry.name;
          } else if (isMondayPublic) {
            // It was public, but effective school because of Tuesday
            const tueEntry = holidayMap.get(
              formatInParkTimezone(addDays(mondayDate, 1), timezone),
            );
            if (tueEntry)
              holidayName =
                typeof tueEntry === "string" ? tueEntry : tueEntry.name;
          }
        }
      }
    }
  }

  // Public Holiday Continuity Logic
  // If today is a Public Holiday, check if it's adjacent to a School Holiday
  // If so, treat it as School Holiday as well (important for ML and continuity)
  let isEffectiveSchoolFromAdjancency = false;
  if (currentIsPublic && !currentIsSchool) {
    const yesterday = addDays(date, -1);
    const tomorrow = addDays(date, 1);
    const yesterdayStr = formatInParkTimezone(yesterday, timezone);
    const tomorrowStr = formatInParkTimezone(tomorrow, timezone);

    if (
      isSchoolHolidayInMap(yesterdayStr) ||
      isSchoolHolidayInMap(tomorrowStr)
    ) {
      isEffectiveSchoolFromAdjancency = true;
    }
  }

  const isPublicHoliday = currentIsPublic;
  const isSchoolHoliday =
    currentIsSchool || isWeekendBonus || isEffectiveSchoolFromAdjancency;
  const isHoliday = isPublicHoliday || isSchoolHoliday;

  // For backward compatibility: single holidayType
  // Prioritize public over school if both exist
  if (isPublicHoliday) {
    holidayType = "public";
  } else if (isSchoolHoliday) {
    holidayType = "school";
  }

  // Check Bridge Day Logic
  // 1. Friday (5) after Thursday Holiday
  // 2. Monday (1) before Tuesday Holiday
  // 3. Tuesday (2) between Monday and Wednesday holidays
  // 4. Wednesday (3) between Tuesday and Thursday holidays
  // 5. Thursday (4) between Wednesday and Friday holidays
  // Note: Only applies to public holidays, not school holidays
  let isBridgeDay = false;

  const checkIsPublicHoliday = (
    entry: string | HolidayEntry | undefined,
  ): boolean => {
    if (!entry) return false;
    if (typeof entry === "string") return true;
    return (
      entry.type === "public" ||
      entry.type === "bank" ||
      (entry.allTypes?.some((t) => t === "public" || t === "bank") ?? false)
    );
  };

  if (dayOfWeek === 5) {
    // Friday - check if Thursday is a public holiday
    const prevDate = addDays(date, -1);
    const prevDateStr = formatInParkTimezone(prevDate, timezone);
    if (checkIsPublicHoliday(holidayMap.get(prevDateStr))) isBridgeDay = true;
  } else if (dayOfWeek === 1) {
    // Monday - check if Tuesday is a public holiday
    const nextDate = addDays(date, 1);
    const nextDateStr = formatInParkTimezone(nextDate, timezone);
    if (checkIsPublicHoliday(holidayMap.get(nextDateStr))) isBridgeDay = true;
  } else if (dayOfWeek === 2) {
    // Tuesday - check if both Monday and Wednesday are public holidays
    const prevDate = addDays(date, -1);
    const prevDateStr = formatInParkTimezone(prevDate, timezone);
    const nextDate = addDays(date, 1);
    const nextDateStr = formatInParkTimezone(nextDate, timezone);
    if (
      checkIsPublicHoliday(holidayMap.get(prevDateStr)) &&
      checkIsPublicHoliday(holidayMap.get(nextDateStr))
    ) {
      isBridgeDay = true;
    }
  } else if (dayOfWeek === 3) {
    // Wednesday - check if both Tuesday and Thursday are public holidays
    const prevDate = addDays(date, -1);
    const prevDateStr = formatInParkTimezone(prevDate, timezone);
    const nextDate = addDays(date, 1);
    const nextDateStr = formatInParkTimezone(nextDate, timezone);
    if (
      checkIsPublicHoliday(holidayMap.get(prevDateStr)) &&
      checkIsPublicHoliday(holidayMap.get(nextDateStr))
    ) {
      isBridgeDay = true;
    }
  } else if (dayOfWeek === 4) {
    // Thursday - check if both Wednesday and Friday are public holidays
    const prevDate = addDays(date, -1);
    const prevDateStr = formatInParkTimezone(prevDate, timezone);
    const nextDate = addDays(date, 1);
    const nextDateStr = formatInParkTimezone(nextDate, timezone);
    if (
      checkIsPublicHoliday(holidayMap.get(prevDateStr)) &&
      checkIsPublicHoliday(holidayMap.get(nextDateStr))
    ) {
      isBridgeDay = true;
    }
  }

  // Bridge day cannot be a holiday
  const finalIsBridgeDay = isHoliday ? false : currentIsBridge || isBridgeDay;

  // Convenience flags
  // Note: "observance" holidays are typically not official public holidays,
  // but some observances (like Christmas Eve in some regions) should be treated as public holidays
  // For now, we only mark "public" and "bank" as public holidays
  return {
    isHoliday,
    holidayName,
    isBridgeDay: finalIsBridgeDay,
    holidayType,
    isPublicHoliday,
    isSchoolHoliday,
  };
}

/**
 * Calculate holiday information for a date string (YYYY-MM-DD)
 *
 * Convenience function that parses a date string and calls calculateHolidayInfo.
 *
 * @param dateStr - Date string in YYYY-MM-DD format
 * @param holidayMap - Map of date strings (YYYY-MM-DD) to holiday names or Holiday objects
 * @param timezone - IANA timezone for date calculations
 * @returns Holiday information
 */
export function calculateHolidayInfoFromString<
  T extends string | { name: string } | HolidayEntry,
>(dateStr: string, holidayMap: Map<string, T>, timezone: string): HolidayInfo {
  // Convert to HolidayEntry map if needed
  const entryMap = new Map<string, string | HolidayEntry>();
  for (const [date, value] of holidayMap.entries()) {
    if (typeof value === "string") {
      entryMap.set(date, value);
    } else if (
      typeof value === "object" &&
      value !== null &&
      "name" in value &&
      "type" in value
    ) {
      // Ensure type is a valid HolidayType
      const entry = value as HolidayEntry;
      entryMap.set(date, {
        name: entry.name,
        type: entry.type as HolidayType,
      });
    } else if (typeof value === "object" && value !== null && "name" in value) {
      // Legacy: object with just name property
      entryMap.set(date, (value as { name: string }).name);
    } else {
      // Fallback: convert to string
      entryMap.set(date, String(value));
    }
  }

  // Parse date string (YYYY-MM-DD) - create date at midnight in UTC
  // This ensures consistent date formatting regardless of timezone
  const [year, month, day] = dateStr.split("-").map(Number);
  // Create date at noon UTC to avoid issues with date shifting across timezones.
  // Midnight UTC (00:00:00) can shift to the previous day in Western timezones (e.g., US),
  // whereas Noon UTC is safely within the same calendar day for all timezones between UTC-12 and UTC+12.
  const date = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
  return calculateHolidayInfo(date, entryMap, timezone);
}

/** The park fields the local-holiday rule reads. */
export interface HolidayLocation {
  countryCode: string;
  regionCode: string | null;
  timezone: string;
}

/**
 * Whether a holiday row applies at the park itself: the park's country, and
 * nationwide, without a region, or in the park's own region. Every other row is
 * a neighbour's holiday. The calendar and the yearly forecast both ask this, so
 * a day cannot be a holiday on one and a plain day on the other.
 */
export function isLocalHoliday(
  h: Pick<HolidayRow, "country" | "region" | "isNationwide">,
  park: Pick<HolidayLocation, "countryCode" | "regionCode">,
): boolean {
  return localHolidayTest(park)(h);
}

/**
 * {@link isLocalHoliday} for one park, as a test to run over many rows: the
 * park's region is normalised once, and each distinct row region once. The
 * Netherlands stores school breaks per municipality, about 250,000 rows over
 * the two years a yearly window touches, and normalising both sides of every
 * row cost 210 ms.
 */
function localHolidayTest(
  park: Pick<HolidayLocation, "countryCode" | "regionCode">,
): (h: Pick<HolidayRow, "country" | "region" | "isNationwide">) => boolean {
  const parkRegion = normalizeRegionCode(park.regionCode);
  const byRegion = new Map<string, boolean>();
  return (h) => {
    if (h.country !== park.countryCode) return false;
    if (h.isNationwide || !h.region) return true;
    let local = byRegion.get(h.region);
    if (local === undefined) {
      local = normalizeRegionCode(h.region) === parkRegion;
      byRegion.set(h.region, local);
    }
    return local;
  };
}

/**
 * The calendar day a holiday row names. The row already is one, `YYYY-MM-DD`
 * in the country's own calendar. Reading it through a `Date` at noon UTC and
 * the park's zone, as the calendar did, put every holiday a day late for a park
 * at UTC+12 or beyond: noon UTC on Christmas Day is already the 26th in
 * Auckland.
 */
export function holidayDay(h: Pick<HolidayRow, "date">): string {
  return h.date.slice(0, 10);
}

/**
 * The park's local holidays keyed by park-local day, in the shape
 * {@link calculateHolidayInfo} reads. A day with several rows keeps every type
 * in `allTypes` and takes its name from the public holiday, which is the entry
 * the bridge-day rule looks at.
 */
export function buildLocalHolidayMap(
  holidays: HolidayRow[],
  park: HolidayLocation,
): Map<string, HolidayEntry> {
  const map = new Map<string, HolidayEntry>();
  const isLocal = localHolidayTest(park);

  for (const h of holidays) {
    if (!isLocal(h)) continue;

    const day = holidayDay(h);
    const name = h.localName || h.name || "";
    const type = h.holidayType;

    const existing = map.get(day);
    if (!existing) {
      map.set(day, { name, type, allTypes: [type] });
      continue;
    }
    if (!existing.allTypes) existing.allTypes = [existing.type];
    if (!existing.allTypes.includes(type)) existing.allTypes.push(type);
    if (type === "public" || type === "bank") {
      existing.name = name;
      existing.type = type;
    }
  }

  return map;
}

/** The calendar's holiday flags for one day, plus the English names behind them. */
export interface DayHolidayFlags {
  isHoliday: boolean;
  isSchoolVacation: boolean;
  isBridgeDay: boolean;
  /** Only on an `isHoliday` day. English, as the calendar's `events[].name`. */
  holidayName?: string;
  /**
   * Only on a day a school-break row covers. The weekends and public holidays
   * the break absorbs read `isSchoolVacation` without one.
   */
  schoolVacationName?: string;
}

/**
 * The calendar's three holiday flags for each of `dates` (park-local
 * `YYYY-MM-DD`), by the calendar's own rule: {@link buildLocalHolidayMap} and
 * {@link calculateHolidayInfo}. `holidays` must reach three days past both ends
 * of `dates`, because a school break absorbs a weekend by looking across it to
 * the Thursday or the Tuesday.
 *
 * Neighbouring regions are left out: they are the calendar's `neighborHolidays`,
 * a separate signal.
 */
export function dayHolidayFlags(
  dates: string[],
  holidays: HolidayRow[],
  park: HolidayLocation,
): Map<string, DayHolidayFlags> {
  const local = holidays.filter(localHolidayTest(park));
  const map = buildLocalHolidayMap(local, park);

  // The names come from `name`, not the map's `localName`: the calendar sends
  // the English name, and the frontend translates from it.
  const publicNames = new Map<string, string>();
  const schoolNames = new Map<string, string>();
  for (const h of local) {
    const day = holidayDay(h);
    if (h.holidayType === "school") {
      if (!schoolNames.has(day)) schoolNames.set(day, h.name);
    } else if (h.holidayType === "public" || h.holidayType === "bank") {
      if (!publicNames.has(day)) publicNames.set(day, h.name);
    }
  }

  // `calculateHolidayInfo` looks no further than three days from the day it
  // rates (a weekend reaches across to the Thursday or the Tuesday), so a day
  // with no map entry that close cannot be flagged, and only the rest pay for
  // its timezone formatting: 21 ms for a year of days.
  const reachable = new Set<string>();
  for (const day of map.keys()) {
    for (let offset = -3; offset <= 3; offset++) {
      reachable.add(addIsoDays(day, offset));
    }
  }

  const result = new Map<string, DayHolidayFlags>();
  for (const date of dates) {
    if (!reachable.has(date)) {
      result.set(date, {
        isHoliday: false,
        isSchoolVacation: false,
        isBridgeDay: false,
      });
      continue;
    }
    // Park-local noon, the instant the calendar rates a day at: noon UTC is
    // already the next day east of UTC+12.
    const info = calculateHolidayInfo(
      fromZonedTime(`${date}T12:00:00`, park.timezone),
      map,
      park.timezone,
    );
    const flags: DayHolidayFlags = {
      isHoliday: info.isPublicHoliday,
      isSchoolVacation: info.isSchoolHoliday,
      isBridgeDay: info.isBridgeDay,
    };
    const holidayName = info.isPublicHoliday ? publicNames.get(date) : null;
    if (holidayName) flags.holidayName = holidayName;
    const schoolVacationName = info.isSchoolHoliday
      ? schoolNames.get(date)
      : null;
    if (schoolVacationName) flags.schoolVacationName = schoolVacationName;
    result.set(date, flags);
  }
  return result;
}
