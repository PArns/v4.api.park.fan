import {
  Entity,
  PrimaryColumn,
  Column,
  ManyToOne,
  UpdateDateColumn,
  Index,
  JoinColumn,
} from "typeorm";
import { Park } from "./park.entity";

/**
 * Weather Data Entity
 *
 * Stores daily weather data for parks to correlate with wait times.
 * Used for ML predictions to understand weather impact on park attendance.
 *
 * Data Source: Open-Meteo API (https://open-meteo.com/)
 * - Free, no API key required
 * - Historical data available (365 days back)
 * - 16-day forecast
 *
 * Strategy:
 * - Historical: Fetch once (last 365 days)
 * - Current: Update daily (until day is complete)
 * - Forecast: 16 days ahead, updated every 12 hours
 */
@Entity("weather_data")
@Index(["park", "date"])
export class WeatherData {
  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @PrimaryColumn()
  parkId: string;

  @PrimaryColumn({ type: "date" })
  date: Date;

  /**
   * Data type: historical, current, or forecast
   * - historical: Past data (ML training)
   * - current: Today's data (updated throughout day)
   * - forecast: Future predictions (16 days ahead)
   */
  @Column({
    type: "enum",
    enum: ["historical", "current", "forecast"],
  })
  dataType: "historical" | "current" | "forecast";

  // Temperature (°C)
  @Column({ type: "decimal", precision: 5, scale: 2, nullable: true })
  temperatureMax: number | null;

  @Column({ type: "decimal", precision: 5, scale: 2, nullable: true })
  temperatureMin: number | null;

  // Precipitation (mm)
  @Column({ type: "decimal", precision: 6, scale: 2, nullable: true })
  precipitationSum: number | null;

  @Column({ type: "decimal", precision: 6, scale: 2, nullable: true })
  rainSum: number | null;

  @Column({ type: "decimal", precision: 6, scale: 2, nullable: true })
  snowfallSum: number | null;

  /**
   * WMO Weather Code
   * 0: Clear sky
   * 1-3: Mainly clear, partly cloudy, overcast
   * 45,48: Fog
   * 51-55: Drizzle
   * 61-65: Rain
   * 71-75: Snow fall
   * 80-82: Rain showers
   * 95-99: Thunderstorm
   */
  @Column({ type: "int", nullable: true })
  weatherCode: number | null;

  // Wind (km/h)
  @Column({ type: "decimal", precision: 5, scale: 2, nullable: true })
  windSpeedMax: number | null;

  @Column({ type: "timestamp", default: () => "CURRENT_TIMESTAMP" })
  createdAt: Date; // When this data was fetched

  @UpdateDateColumn()
  updatedAt: Date; // When this data was last updated
}
