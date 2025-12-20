import {
  Entity,
  PrimaryColumn,
  Column,
  ManyToOne,
  Index,
  JoinColumn,
  BeforeInsert,
} from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";

/**
 * Forecast Data Entity
 *
 * Stores hourly wait time predictions from ThemeParks.wiki API.
 * Can be extended later to include our own ML model predictions.
 *
 * API Source: GET /entity/{id}/live -> liveData[].forecast[]
 */
@Entity("forecast_data")
@Index(["attraction", "predictedTime"])
export class ForecastData {
  @PrimaryColumn({ type: "uuid" })
  id: string;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column()
  attractionId: string;

  @Column({ type: "timestamptz" })
  predictedTime: Date;

  @Column({ type: "int" })
  predictedWaitTime: number;

  @Column({ type: "int", nullable: true })
  confidencePercentage: number | null;

  @Column({ type: "text", default: "themeparks_wiki" })
  source: string; // 'themeparks_wiki' or 'our_ml_model' (future)

  @PrimaryColumn({ type: "timestamptz" })
  createdAt: Date; // When this forecast was created/fetched

  @BeforeInsert()
  generateId() {
    if (!this.id) {
      this.id = crypto.randomUUID();
    }
    if (!this.createdAt) {
      this.createdAt = new Date();
    }
  }
}
