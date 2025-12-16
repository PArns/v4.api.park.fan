import { ApiProperty } from "@nestjs/swagger";

/**
 * Holiday Item DTO
 *
 * Represents a single holiday
 */
export class HolidayItemDto {
  @ApiProperty({ description: "Date of the holiday (ISO 8601 YYYY-MM-DD)" })
  date: string;

  @ApiProperty({ description: "Name of the holiday (English)" })
  name: string;

  @ApiProperty({ description: "Local name of the holiday", nullable: true })
  localName: string | null;

  @ApiProperty({ description: "ISO 3166-1 alpha-2 country code" })
  country: string;

  @ApiProperty({
    description: "Region code (e.g., US-FL) if applicable",
    nullable: true,
  })
  region: string | null;

  @ApiProperty({
    description: "Type of holiday",
    enum: ["public", "observance", "school", "bank"],
  })
  holidayType: "public" | "observance" | "school" | "bank";

  @ApiProperty({ description: "Whether the holiday applies nationwide" })
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
