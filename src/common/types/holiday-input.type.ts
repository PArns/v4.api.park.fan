/**
 * Holiday Input Types
 *
 * Used for creating/updating holiday records
 */

export interface HolidayInput {
  externalId: string;
  date: Date;
  name: string;
  localName?: string;
  country: string;
  region?: string;
  holidayType: "public" | "observance" | "school" | "bank";
  isNationwide: boolean;
}
