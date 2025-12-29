/**
 * Peak Seasons / School Vacation Periods
 *
 * Hardcoded vacation periods for countries where no reliable API exists:
 * - USA: School districts vary, but major breaks are consistent
 * - UK: Local authorities set dates, but general patterns exist
 * - Japan: National breaks but school-specific variations
 * - China: National breaks tied to festivals
 * - Canada: Provincial variations
 * - South Korea: Semester-based breaks
 * - Australia: State-based but predictable
 *
 * These are stored as "school" holiday type with region = null (nationwide)
 * to influence ML predictions.
 *
 * Data covers 2024-2035 (10+ years)
 */

export interface PeakSeasonPeriod {
  name: string;
  startMonth: number; // 1-12
  startDay: number;
  endMonth: number;
  endDay: number;
}

// US Peak Seasons (approximate, covers most school districts)
export const US_PEAK_SEASONS: PeakSeasonPeriod[] = [
  // Winter Break (Christmas/New Year)
  {
    name: "Winter Break",
    startMonth: 12,
    startDay: 20,
    endMonth: 1,
    endDay: 3,
  },
  // MLK Weekend (3rd Monday of January) - approximated
  { name: "MLK Weekend", startMonth: 1, startDay: 13, endMonth: 1, endDay: 21 },
  // Presidents' Day Weekend (3rd Monday of February) - approximated
  {
    name: "Presidents Day",
    startMonth: 2,
    startDay: 14,
    endMonth: 2,
    endDay: 22,
  },
  // Spring Break (varies by region, typically mid-March to mid-April)
  {
    name: "Spring Break",
    startMonth: 3,
    startDay: 10,
    endMonth: 4,
    endDay: 20,
  },
  // Memorial Day Weekend
  {
    name: "Memorial Day",
    startMonth: 5,
    startDay: 23,
    endMonth: 5,
    endDay: 31,
  },
  // Summer Break
  { name: "Summer Break", startMonth: 6, startDay: 1, endMonth: 8, endDay: 20 },
  // Labor Day Weekend
  { name: "Labor Day", startMonth: 8, startDay: 30, endMonth: 9, endDay: 7 },
  // Columbus Day Weekend (2nd Monday of October)
  {
    name: "Columbus Day",
    startMonth: 10,
    startDay: 7,
    endMonth: 10,
    endDay: 14,
  },
  // Thanksgiving Break
  {
    name: "Thanksgiving",
    startMonth: 11,
    startDay: 22,
    endMonth: 11,
    endDay: 30,
  },
];

// UK Peak Seasons (England pattern, Scotland/Wales/NI similar)
export const UK_PEAK_SEASONS: PeakSeasonPeriod[] = [
  // February Half Term
  {
    name: "February Half Term",
    startMonth: 2,
    startDay: 10,
    endMonth: 2,
    endDay: 23,
  },
  // Easter Holidays
  {
    name: "Easter Holidays",
    startMonth: 3,
    startDay: 28,
    endMonth: 4,
    endDay: 14,
  },
  // May Half Term
  {
    name: "May Half Term",
    startMonth: 5,
    startDay: 24,
    endMonth: 6,
    endDay: 2,
  },
  // Summer Holidays
  {
    name: "Summer Holidays",
    startMonth: 7,
    startDay: 20,
    endMonth: 9,
    endDay: 3,
  },
  // October Half Term
  {
    name: "October Half Term",
    startMonth: 10,
    startDay: 21,
    endMonth: 11,
    endDay: 3,
  },
  // Christmas Holidays
  {
    name: "Christmas Holidays",
    startMonth: 12,
    startDay: 20,
    endMonth: 1,
    endDay: 5,
  },
];

// Japan Peak Seasons
export const JP_PEAK_SEASONS: PeakSeasonPeriod[] = [
  // New Year (Shogatsu)
  { name: "New Year", startMonth: 12, startDay: 28, endMonth: 1, endDay: 6 },
  // Golden Week
  { name: "Golden Week", startMonth: 4, startDay: 29, endMonth: 5, endDay: 6 },
  // Obon
  { name: "Obon", startMonth: 8, startDay: 11, endMonth: 8, endDay: 16 },
  // Summer Break
  {
    name: "Summer Break",
    startMonth: 7,
    startDay: 20,
    endMonth: 8,
    endDay: 31,
  },
  // Spring Break
  { name: "Spring Break", startMonth: 3, startDay: 20, endMonth: 4, endDay: 7 },
  // Silver Week (around Autumnal Equinox)
  { name: "Silver Week", startMonth: 9, startDay: 14, endMonth: 9, endDay: 24 },
];

// China Peak Seasons
export const CN_PEAK_SEASONS: PeakSeasonPeriod[] = [
  // Chinese New Year / Spring Festival (dates vary, this is approximate window)
  {
    name: "Spring Festival",
    startMonth: 1,
    startDay: 20,
    endMonth: 2,
    endDay: 15,
  },
  // Qingming Festival
  {
    name: "Qingming Festival",
    startMonth: 4,
    startDay: 3,
    endMonth: 4,
    endDay: 7,
  },
  // Labor Day
  { name: "Labor Day", startMonth: 5, startDay: 1, endMonth: 5, endDay: 5 },
  // Dragon Boat Festival
  {
    name: "Dragon Boat Festival",
    startMonth: 6,
    startDay: 7,
    endMonth: 6,
    endDay: 12,
  },
  // Summer Break
  { name: "Summer Break", startMonth: 7, startDay: 1, endMonth: 8, endDay: 31 },
  // National Day / Golden Week
  {
    name: "National Day",
    startMonth: 10,
    startDay: 1,
    endMonth: 10,
    endDay: 7,
  },
  // Winter Break
  {
    name: "Winter Break",
    startMonth: 1,
    startDay: 10,
    endMonth: 2,
    endDay: 20,
  },
];

// Canada Peak Seasons (Ontario pattern, varies by province)
export const CA_PEAK_SEASONS: PeakSeasonPeriod[] = [
  // Winter Break
  {
    name: "Winter Break",
    startMonth: 12,
    startDay: 20,
    endMonth: 1,
    endDay: 5,
  },
  // March Break
  { name: "March Break", startMonth: 3, startDay: 9, endMonth: 3, endDay: 17 },
  // Victoria Day Weekend
  {
    name: "Victoria Day",
    startMonth: 5,
    startDay: 17,
    endMonth: 5,
    endDay: 25,
  },
  // Summer Break
  { name: "Summer Break", startMonth: 6, startDay: 25, endMonth: 9, endDay: 2 },
  // Thanksgiving Weekend (2nd Monday of October)
  {
    name: "Thanksgiving",
    startMonth: 10,
    startDay: 7,
    endMonth: 10,
    endDay: 14,
  },
];

// South Korea Peak Seasons
export const KR_PEAK_SEASONS: PeakSeasonPeriod[] = [
  // Lunar New Year (Seollal)
  {
    name: "Lunar New Year",
    startMonth: 1,
    startDay: 20,
    endMonth: 2,
    endDay: 5,
  },
  // Spring Break
  { name: "Spring Break", startMonth: 2, startDay: 10, endMonth: 3, endDay: 1 },
  // Children's Day / Buddha's Birthday period
  { name: "Golden Week", startMonth: 5, startDay: 1, endMonth: 5, endDay: 8 },
  // Summer Break
  {
    name: "Summer Break",
    startMonth: 7,
    startDay: 20,
    endMonth: 8,
    endDay: 25,
  },
  // Chuseok (Korean Thanksgiving)
  { name: "Chuseok", startMonth: 9, startDay: 10, endMonth: 9, endDay: 20 },
  // Winter Break
  {
    name: "Winter Break",
    startMonth: 12,
    startDay: 20,
    endMonth: 2,
    endDay: 28,
  },
];

// Australia Peak Seasons (varies by state, this is approximate national)
export const AU_PEAK_SEASONS: PeakSeasonPeriod[] = [
  // Summer Holidays (Dec-Jan, crosses year boundary)
  {
    name: "Summer Holidays",
    startMonth: 12,
    startDay: 15,
    endMonth: 1,
    endDay: 31,
  },
  // Easter / Autumn Break
  { name: "Easter Break", startMonth: 4, startDay: 5, endMonth: 4, endDay: 22 },
  // Winter Break (July)
  { name: "Winter Break", startMonth: 7, startDay: 1, endMonth: 7, endDay: 15 },
  // Spring Break (September/October)
  {
    name: "Spring Break",
    startMonth: 9,
    startDay: 20,
    endMonth: 10,
    endDay: 7,
  },
];

// Denmark Peak Seasons
export const DK_PEAK_SEASONS: PeakSeasonPeriod[] = [
  // Winter Break
  {
    name: "Winter Break",
    startMonth: 2,
    startDay: 10,
    endMonth: 2,
    endDay: 18,
  },
  // Easter
  { name: "Easter", startMonth: 3, startDay: 28, endMonth: 4, endDay: 8 },
  // Summer Holidays
  {
    name: "Summer Holidays",
    startMonth: 6,
    startDay: 25,
    endMonth: 8,
    endDay: 10,
  },
  // Autumn Break
  {
    name: "Autumn Break",
    startMonth: 10,
    startDay: 12,
    endMonth: 10,
    endDay: 20,
  },
  // Christmas
  { name: "Christmas", startMonth: 12, startDay: 20, endMonth: 1, endDay: 3 },
];

// Brazil Peak Seasons (Southern Hemisphere)
export const BR_PEAK_SEASONS: PeakSeasonPeriod[] = [
  // Summer Break / End of Year
  {
    name: "Summer Break",
    startMonth: 12,
    startDay: 15,
    endMonth: 1,
    endDay: 31,
  },
  // Carnival (Varies, but usually in Feb/Mar window)
  { name: "Carnival", startMonth: 2, startDay: 15, endMonth: 3, endDay: 10 },
  // Winter Break (Mid-year)
  { name: "Winter Break", startMonth: 7, startDay: 1, endMonth: 7, endDay: 31 },
  // October Break (Children's Day week)
  {
    name: "October Break",
    startMonth: 10,
    startDay: 10,
    endMonth: 10,
    endDay: 15,
  },
];

// Hong Kong (Similar to China but with some specific colonial/western influence)
export const HK_PEAK_SEASONS: PeakSeasonPeriod[] = [...CN_PEAK_SEASONS];

// Map country codes to peak seasons
export const PEAK_SEASONS_BY_COUNTRY: Record<string, PeakSeasonPeriod[]> = {
  US: US_PEAK_SEASONS,
  GB: UK_PEAK_SEASONS, // UK uses GB as ISO code
  JP: JP_PEAK_SEASONS,
  CN: CN_PEAK_SEASONS,
  CA: CA_PEAK_SEASONS,
  KR: KR_PEAK_SEASONS,
  AU: AU_PEAK_SEASONS,
  DK: DK_PEAK_SEASONS,
  BR: BR_PEAK_SEASONS,
  HK: HK_PEAK_SEASONS,
};

/**
 * Generate all dates within a peak season period for a specific year
 */
export function* generatePeakSeasonDates(
  period: PeakSeasonPeriod,
  year: number,
): Generator<Date> {
  let startYear = year;
  let endYear = year;

  // Handle periods that cross year boundary (e.g., Dec 20 - Jan 5)
  if (period.endMonth < period.startMonth) {
    endYear = year + 1;
  }

  const start = new Date(startYear, period.startMonth - 1, period.startDay);
  const end = new Date(endYear, period.endMonth - 1, period.endDay);

  const current = new Date(start);
  while (current <= end) {
    yield new Date(current);
    current.setDate(current.getDate() + 1);
  }
}

/**
 * Get all peak season holidays for a country for a range of years
 */
export function getPeakSeasonHolidays(
  countryCode: string,
  startYear: number,
  endYear: number,
): { date: Date; name: string; country: string }[] {
  const periods = PEAK_SEASONS_BY_COUNTRY[countryCode];
  if (!periods) return [];

  const holidays: { date: Date; name: string; country: string }[] = [];

  for (let year = startYear; year <= endYear; year++) {
    for (const period of periods) {
      for (const date of generatePeakSeasonDates(period, year)) {
        holidays.push({
          date,
          name: period.name,
          country: countryCode,
        });
      }
    }
  }

  return holidays;
}
