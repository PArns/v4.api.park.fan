import { Injectable, Logger } from "@nestjs/common";
import { Park } from "../entities/park.entity";
import { formatInParkTimezone } from "../../common/utils/date.util";

export interface ParkEvent {
  name: string;
  type:
    | "holiday"
    | "parade"
    | "firework"
    | "character_meet"
    | "seasonal_event"
    | "special_show"
    | "festival";
  startTime?: string;
  endTime?: string;
  location?: string;
  description?: string;
}

/**
 * Events Service
 *
 * Manages special park events like parades, fireworks, and seasonal events.
 * Currently uses hardcoded seasonal events - can be extended with API integrations.
 */
@Injectable()
export class EventsService {
  private readonly logger = new Logger(EventsService.name);

  /**
   * Get today's special events for a park
   */
  async getTodayEvents(park: Park): Promise<ParkEvent[]> {
    const events: ParkEvent[] = [];

    // Get current date in park's timezone
    const today = new Date();
    const month = today.getMonth() + 1; // 1-12
    const day = today.getDate();

    // Christmas Season (Dec 1 - Jan 6)
    if (month === 12 || (month === 1 && day <= 6)) {
      events.push({
        name: "Christmas Parade",
        type: "parade",
        startTime: "15:00",
        endTime: "15:30",
        location: "Main Street",
        description:
          "Magical Christmas parade with festive characters and floats",
      });

      events.push({
        name: "Holiday Fireworks",
        type: "firework",
        startTime: "20:00",
        endTime: "20:15",
        location: "Central Plaza",
        description: "Spectacular fireworks display with holiday music",
      });

      events.push({
        name: "Winter Wonderland",
        type: "seasonal_event",
        description:
          "Experience the magic of winter with special decorations and shows",
      });
    }

    // Halloween Season (October)
    if (month === 10) {
      events.push({
        name: "Halloween Horror Nights",
        type: "seasonal_event",
        startTime: "19:00",
        endTime: "01:00",
        description:
          "Frightening after-hours Halloween event with haunted mazes",
      });

      events.push({
        name: "Spooky Parade",
        type: "parade",
        startTime: "18:00",
        endTime: "18:30",
        location: "Main Street",
        description: "Halloween-themed parade with spooky characters",
      });
    }

    // Summer Season (June-August)
    if (month >= 6 && month <= 8) {
      events.push({
        name: "Summer Festival",
        type: "festival",
        description:
          "Celebrate summer with special entertainment and activities",
      });

      events.push({
        name: "Evening Spectacular",
        type: "firework",
        startTime: "21:30",
        endTime: "21:45",
        location: "Central Plaza",
        description: "Nightly summer fireworks show",
      });
    }

    // Easter (March-April, simplified to dates)
    if (month === 3 || month === 4) {
      events.push({
        name: "Easter Celebration",
        type: "seasonal_event",
        description: "Special Easter activities and character meet-and-greets",
      });

      events.push({
        name: "Bunny Meet & Greet",
        type: "character_meet",
        startTime: "11:00",
        endTime: "16:00",
        location: "Character Square",
        description: "Meet the Easter Bunny and friends",
      });
    }

    // New Year's Eve (Dec 31)
    if (month === 12 && day === 31) {
      events.push({
        name: "New Year's Eve Countdown",
        type: "special_show",
        startTime: "23:45",
        endTime: "00:15",
        location: "Main Entrance",
        description:
          "Ring in the new year with a spectacular countdown celebration",
      });

      events.push({
        name: "Midnight Fireworks",
        type: "firework",
        startTime: "00:00",
        endTime: "00:10",
        location: "Central Plaza",
        description: "Grand fireworks display to celebrate the new year",
      });
    }

    this.logger.debug(
      `Found ${events.length} seasonal events for ${park.name} on ${formatInParkTimezone(today, park.timezone)}`,
    );

    return events;
  }

  /**
   * Get events for a specific date in park's timezone
   */
  async getEventsForDate(park: Park, date: Date): Promise<ParkEvent[]> {
    const month = date.getMonth() + 1;
    const day = date.getDate();
    const events: ParkEvent[] = [];

    // Same logic as getTodayEvents but for specific date
    // Christmas Season
    if (month === 12 || (month === 1 && day <= 6)) {
      events.push(
        {
          name: "Christmas Parade",
          type: "parade",
          startTime: "15:00",
          endTime: "15:30",
          location: "Main Street",
        },
        {
          name: "Holiday Fireworks",
          type: "firework",
          startTime: "20:00",
          endTime: "20:15",
          location: "Central Plaza",
        },
      );
    }

    // Halloween
    if (month === 10) {
      events.push({
        name: "Halloween Horror Nights",
        type: "seasonal_event",
        startTime: "19:00",
        endTime: "01:00",
      });
    }

    // Summer
    if (month >= 6 && month <= 8) {
      events.push({
        name: "Evening Spectacular",
        type: "firework",
        startTime: "21:30",
        endTime: "21:45",
        location: "Central Plaza",
      });
    }

    return events;
  }
}
