import { ScheduleType } from "../entities/schedule-entry.entity";

/**
 * Schedule Item DTO
 *
 * Represents a single schedule entry for a park
 */
export class ScheduleItemDto {
  date: string; // ISO 8601 date (YYYY-MM-DD)
  scheduleType: ScheduleType;
  openingTime: string | null; // ISO 8601 timestamp
  closingTime: string | null; // ISO 8601 timestamp
  description: string | null;
  purchases:
    | {
        startDate: string;
        endDate: string;
        price: { amount: number; currency: string };
      }[]
    | null; // Lightning Lane pricing, etc.

  static fromEntity(schedule: {
    date: Date | string;
    scheduleType: ScheduleType;
    openingTime: Date | string | null;
    closingTime: Date | string | null;
    description: string | null;
    purchases: any | null;
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
    return dto;
  }
}
