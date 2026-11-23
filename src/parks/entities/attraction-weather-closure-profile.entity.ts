import { Column, Entity, Index, PrimaryColumn } from "typeorm";
import type { WeatherClosureProfile } from "../utils/weather-closure.util";

/**
 * How a ride's closures follow cold and snow, learned weekly from its own
 * park-open hours (`WeatherClosureService.rebuildAll`).
 *
 * One row per ride that has any profile, whatever its probability: the 90 %
 * bar is applied where a verdict is made (`decideWeatherClosure`), so a ride
 * approaching it stays visible to whoever reads this table.
 */
@Entity("attraction_weather_closure_profiles")
@Index(["parkId"])
export class AttractionWeatherClosureProfile {
  @PrimaryColumn("uuid")
  attractionId: string;

  @Column("uuid")
  parkId: string;

  /** Learned on dry hours; null when too few cold hours stand behind it. */
  @Column({ type: "jsonb", nullable: true })
  cold: WeatherClosureProfile | null;

  /** Learned on the hours with snow in the last 24; null when too few. */
  @Column({ type: "jsonb", nullable: true })
  snow: WeatherClosureProfile | null;

  @Column({ type: "timestamptz" })
  generatedAt: Date;
}
