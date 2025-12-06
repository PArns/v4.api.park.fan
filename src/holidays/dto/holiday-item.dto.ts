/**
 * Holiday Item DTO
 *
 * Represents a single holiday
 */
export class HolidayItemDto {
  date: string; // ISO 8601 date (YYYY-MM-DD)
  name: string;
  localName: string | null;
  country: string; // ISO 3166-1 alpha-2 code (e.g., "US")
  region: string | null;
  holidayType: "public" | "observance" | "school" | "bank";
  isNationwide: boolean;

  static fromEntity(holiday: {
    date: Date | string;
    name: string;
    localName: string | null;
    country: string;
    region: string | null;
    holidayType: "public" | "observance" | "school" | "bank";
    isNationwide: boolean;
  }): HolidayItemDto {
    const dto = new HolidayItemDto();
    // Handle both Date objects and string dates
    dto.date =
      typeof holiday.date === "string"
        ? holiday.date.split("T")[0]
        : holiday.date.toISOString().split("T")[0]; // YYYY-MM-DD format
    dto.name = holiday.name;
    dto.localName = holiday.localName;
    dto.country = holiday.country;
    dto.region = holiday.region;
    dto.holidayType = holiday.holidayType;
    dto.isNationwide = holiday.isNationwide;
    return dto;
  }
}
