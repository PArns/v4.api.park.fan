/**
 * Weekend Rules by Country
 *
 * Maps ISO 3166-1 alpha-2 country codes to weekend day numbers (0 = Sunday, 6 = Saturday)
 * Rules are based on official government working week definitions.
 *
 * Default: Saturday (6) + Sunday (0) for Western countries
 */

export interface WeekendRule {
  /** ISO 3166-1 alpha-2 country code */
  countryCode: string;
  /** Country name for documentation */
  countryName: string;
  /** Weekend days (0 = Sunday, 1 = Monday, ..., 6 = Saturday) */
  weekendDays: number[];
  /** Additional notes or historical context */
  notes?: string;
}

/**
 * Country-specific weekend rules
 * Only non-standard weekends are defined here (not Sat+Sun)
 */
export const CUSTOM_WEEKEND_RULES: WeekendRule[] = [
  // Middle East - Friday + Saturday
  {
    countryCode: "SA",
    countryName: "Saudi Arabia",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "AE",
    countryName: "United Arab Emirates",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "KW",
    countryName: "Kuwait",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "QA",
    countryName: "Qatar",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "BH",
    countryName: "Bahrain",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "OM",
    countryName: "Oman",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "EG",
    countryName: "Egypt",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "JO",
    countryName: "Jordan",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "IQ",
    countryName: "Iraq",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "SY",
    countryName: "Syria",
    weekendDays: [5, 6], // Friday + Saturday
  },
  {
    countryCode: "YE",
    countryName: "Yemen",
    weekendDays: [5, 6], // Friday + Saturday
  },

  // Israel - Friday + Saturday
  {
    countryCode: "IL",
    countryName: "Israel",
    weekendDays: [5, 6], // Friday (half day) + Saturday
    notes: "Friday is often half day, Saturday is Shabbat",
  },

  // Iran - Friday only (historically, but often Fri+Sat in practice)
  {
    countryCode: "IR",
    countryName: "Iran",
    weekendDays: [5], // Friday only officially
    notes: "Officially Friday only, but many businesses also close Saturday",
  },

  // Afghanistan - Thursday + Friday (historically)
  {
    countryCode: "AF",
    countryName: "Afghanistan",
    weekendDays: [4, 5], // Thursday + Friday
    notes: "Historically Thursday + Friday, though this has varied",
  },

  // Brunei - Friday + Sunday
  {
    countryCode: "BN",
    countryName: "Brunei",
    weekendDays: [5, 0], // Friday + Sunday
  },

  // Nepal - Saturday only
  {
    countryCode: "NP",
    countryName: "Nepal",
    weekendDays: [6], // Saturday only
    notes: "Saturday is the only official weekend day",
  },
];

/**
 * Default weekend days for Western countries (Saturday + Sunday)
 */
export const DEFAULT_WEEKEND_DAYS = [0, 6]; // Sunday (0) + Saturday (6)

/**
 * Get weekend days for a specific country
 * @param countryCode ISO 3166-1 alpha-2 country code
 * @returns Array of weekend day numbers (0 = Sunday, 6 = Saturday)
 */
export function getWeekendDaysForCountry(countryCode: string): number[] {
  const customRule = CUSTOM_WEEKEND_RULES.find(
    (rule) => rule.countryCode === countryCode.toUpperCase(),
  );

  return customRule ? customRule.weekendDays : DEFAULT_WEEKEND_DAYS;
}
