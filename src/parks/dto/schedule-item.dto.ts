import { ApiProperty } from "@nestjs/swagger";
import { ScheduleType } from "../entities/schedule-entry.entity";

/**
 * Schedule Item DTO
 *
 * Represents a single schedule entry for a park
 */
export class ScheduleItemDto {
  @ApiProperty({
    description: "Date of the schedule (YYYY-MM-DD)",
    example: "2025-12-17",
  })
  date: string; // ISO 8601 date (YYYY-MM-DD)

  @ApiProperty({
    description: "Type of schedule: OPERATING or CLOSED",
    enum: ["OPERATING", "CLOSED"],
  })
  scheduleType: ScheduleType;

  @ApiProperty({
    description: "Opening time (ISO 8601)",
    required: false,
    nullable: true,
  })
  openingTime: string | null; // ISO 8601 timestamp

  @ApiProperty({
    description: "Closing time (ISO 8601)",
    required: false,
    nullable: true,
  })
  closingTime: string | null; // ISO 8601 timestamp

  @ApiProperty({
    description: "Description or notes",
    required: false,
    nullable: true,
  })
  description: string | null;

  @ApiProperty({
    description: "Ticket/Upgrade purchase options",
    required: false,
    nullable: true,
  })
  purchases:
    | {
        startDate: string;
        endDate: string;
        price: { amount: number; currency: string };
      }[]
    | null; // Lightning Lane pricing, etc.

  @ApiProperty({
    description: "Indicates if this date is a regional or national holiday",
    required: false,
  })
  isHoliday: boolean;

  @ApiProperty({
    description: "Name of the holiday (if any)",
    required: false,
    nullable: true,
  })
  holidayName: string | null;

  @ApiProperty({
    description: "Indicates if this date is a bridge day",
    required: false,
  })
  isBridgeDay: boolean;

  static fromEntity(schedule: {
    date: Date | string;
    scheduleType: ScheduleType;
    openingTime: Date | string | null;
    closingTime: Date | string | null;
    description: string | null;
    purchases: any | null;
    isHoliday?: boolean;
    holidayName?: string | null;
    isBridgeDay?: boolean;
  }): ScheduleItemDto {
    const dto = new ScheduleItemDto();
    // Handle both Date objects and string dates
    dto.date =
      typeof schedule.date === "string"
        ? schedule.date.split("T")[0]
        : schedule.date.toISOString().split("T")[0]; // YYYY-MM-DD format
    dto.scheduleType = schedule.scheduleType;
    dto.openingTime = schedule.openingTime
      ? typeof schedule.openingTime === "string"
        ? schedule.openingTime
        : schedule.openingTime.toISOString()
      : null;
    dto.closingTime = schedule.closingTime
      ? typeof schedule.closingTime === "string"
        ? schedule.closingTime
        : schedule.closingTime.toISOString()
      : null;
    dto.description = schedule.description;
    dto.purchases = schedule.purchases;
    dto.isHoliday = schedule.isHoliday || false;
    dto.holidayName = schedule.holidayName || null;
    dto.isBridgeDay = schedule.isBridgeDay || false;
    return dto;
  }
}
