import {
  Entity,
  PrimaryColumn,
  Column,
  ManyToOne,
  JoinColumn,
  Index,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { Park } from "./park.entity";

/**
 * Severe-weather warning affecting a park.
 *
 * Stored per (park, alert): the sync (see WeatherWarningsService) fetches a
 * country's active warnings, matches each park to the affected area(s), and
 * upserts one row per park × alert. Read by `parkId` + `expires > now()` to
 * surface the currently-active warnings on the weather response.
 *
 * Source: MeteoGate (EUMETNET → MeteoAlarm / national services; DWD for DE).
 */
@Entity("weather_warnings")
@Index("idx_weather_warnings_park_expires", ["parkId", "expires"])
export class WeatherWarning {
  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "parkId" })
  park: Park;

  @PrimaryColumn({ type: "text" })
  parkId: string;

  /** Stable CAP alert id (one warning may cover many parks). */
  @PrimaryColumn({ type: "text" })
  alertId: string;

  /** Source identifier, e.g. "meteogate". */
  @Column({ type: "text" })
  source: string;

  @Column({ type: "text" })
  countryCode: string;

  // --- CAP content (German preferred; English where available) ---
  @Column({ type: "text" })
  event: string;

  @Column({ type: "text", nullable: true })
  eventEn: string | null;

  @Column({ type: "text", nullable: true })
  category: string | null;

  /** Minor | Moderate | Severe | Extreme. */
  @Column({ type: "text", nullable: true })
  severity: string | null;

  @Column({ type: "text", nullable: true })
  urgency: string | null;

  @Column({ type: "text", nullable: true })
  certainty: string | null;

  @Column({ type: "timestamptz", nullable: true })
  onset: Date | null;

  @Column({ type: "timestamptz", nullable: true })
  expires: Date | null;

  @Column({ type: "timestamptz", nullable: true })
  sent: Date | null;

  @Column({ type: "text", nullable: true })
  headline: string | null;

  @Column({ type: "text", nullable: true })
  headlineEn: string | null;

  @Column({ type: "text", nullable: true })
  description: string | null;

  @Column({ type: "text", nullable: true })
  descriptionEn: string | null;

  @Column({ type: "text", nullable: true })
  instruction: string | null;

  @Column({ type: "text", nullable: true })
  instructionEn: string | null;

  /** The matched area name (CAP areaDesc), e.g. "Kreis Freyung-Grafenau". */
  @Column({ type: "text", nullable: true })
  area: string | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
