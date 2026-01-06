import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";

/**
 * Holiday Entity
 *
 * Stores holiday data for ML predictions.
 * Parks are significantly more crowded on holidays and weekends.
 *
 * Data Source: Nager.Date API (https://date.nager.at/)
 * - Free, no API key required
 * - Supports 100+ countries
 * - Includes public holidays and observances
 *
 * Use Cases:
 * - ML predictions: Correlate holidays with wait times
 * - Analytics: Identify peak attendance days
 * - User features: Show upcoming park events
 *
 * Weekend Detection:
 * - Stored separately via `DateFeaturesService`
 * - Region-specific (e.g., Middle East: Fri+Sat, Western: Sat+Sun)
 */
@Entity("holidays")
@Index(["country", "date"])
@Index(["country", "region", "date"])
export class Holiday {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  /**
   * Holiday date (YYYY-MM-DD)
   */
  @Column({ type: "date" })
  date: Date;

  /**
   * Holiday name
   * Examples: "Christmas Day", "Independence Day", "Spring Break"
   */
  @Column()
  name: string;

  /**
   * Holiday name in local language (if different from English)
   * Examples: "Tag der Deutschen Einheit", "Día de la Independencia"
   */
  @Column({ type: "text", nullable: true })
  localName: string | null;

  /**
   * ISO 3166-1 alpha-2 country code
   * Examples: "US", "DE", "FR", "JP", "GB"
   */
  @Column({ length: 2 })
  country: string;

  /**
   * ISO 3166-2 region/state code (optional)
   * Examples: "US-FL" (Florida), "DE-BY" (Bavaria)
   * Used for region-specific holidays (e.g., school holidays)
   */
  @Column({ type: "text", nullable: true })
  region: string | null;

  /**
   * Holiday type
   * - public: Official public holiday (government/banks closed)
   * - observance: Observed but not official (e.g., Father's Day)
   * - school: School vacation period
   * - bank: Bank holiday only
   */
  @Column({
    type: "enum",
    enum: ["public", "observance", "school", "bank"],
  })
  holidayType: "public" | "observance" | "school" | "bank";

  /**
   * Whether this is a nationwide holiday
   * False for regional holidays (e.g., state-specific)
   */
  @Column({ default: true })
  isNationwide: boolean;

  /**
   * External API reference (for updates/deduplication)
   * Format: "nager:{countryCode}:{date}:{name}"
   */
  @Column({ unique: true })
  externalId: string;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
