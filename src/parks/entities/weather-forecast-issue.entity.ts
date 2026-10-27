import { Column, Entity, PrimaryColumn } from "typeorm";

/**
 * One daily Open-Meteo forecast as it was issued (PAR-1061, design PR 1).
 *
 * `weather_data` overwrites a forecast row every 12 hours and later turns it into
 * the observed row of the same key, so nobody can ask what the forecast said at
 * an origin. This table keeps every issue the 12-hourly `weather-full-cron`
 * already fetches, append-only. Nothing reads it yet.
 *
 * Plain table, no FK and no hypertable: ~230 MB/yr
 * (docs/architecture/covariate-source.md §3.6), below the size where a
 * TimescaleDB job queueing for a lock pays off (docs/troubleshooting/timescaledb-lock-queues.md). The PK
 * is the only index the design names.
 */
@Entity("weather_forecast_issues")
export class WeatherForecastIssue {
  @PrimaryColumn({ type: "uuid", name: "park_id" })
  parkId: string;

  /** Park-local date the forecast is for. */
  @PrimaryColumn({ type: "date", name: "target_date" })
  targetDate: string;

  /** When we fetched the response (shared by every park on one coordinate). */
  @PrimaryColumn({ type: "timestamptz", name: "issued_at" })
  issuedAt: Date;

  /** `target_date` minus the park-local date of `issued_at`. */
  @Column({ type: "smallint", name: "lead_days" })
  leadDays: number;

  @Column({ type: "real", name: "temp_max", nullable: true })
  tempMax: number | null;

  @Column({ type: "real", name: "temp_min", nullable: true })
  tempMin: number | null;

  @Column({ type: "real", name: "precip_sum", nullable: true })
  precipSum: number | null;

  @Column({ type: "real", name: "rain_sum", nullable: true })
  rainSum: number | null;

  @Column({ type: "real", name: "snow_sum", nullable: true })
  snowSum: number | null;

  @Column({ type: "real", name: "wind_max", nullable: true })
  windMax: number | null;

  @Column({ type: "smallint", name: "weather_code", nullable: true })
  weatherCode: number | null;
}
