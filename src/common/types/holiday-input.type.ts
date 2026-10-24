/**
 * Holiday Input Types
 *
 * Used for creating/updating holiday records
 */
import type { HolidayType } from "../../holidays/entities/holiday.entity";

export interface HolidayInput {
  externalId: string;
  date: Date;
  name: string;
  localName?: string;
  country: string;
  region?: string;
  holidayType: HolidayType;
  isNationwide: boolean;
}
