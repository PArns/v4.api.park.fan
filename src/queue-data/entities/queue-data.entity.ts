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
import {
  QueueType,
  LiveStatus,
} from "../../external-apis/themeparks/themeparks.types";

/**
 * Queue Data Entity
 *
 * Stores wait time data for attractions.
 * Supports all 6 queue types from ThemeParks.wiki:
 * - STANDBY, SINGLE_RIDER, RETURN_TIME, PAID_RETURN_TIME, BOARDING_GROUP, PAID_STANDBY
 *
 * Polymorphic design: Different queue types use different fields (nullable).
 *
 * API Mapping (from GET /v1/entity/{id}/live):
 * - queue.STANDBY.waitTime → waitTime
 * - queue.RETURN_TIME.state → state
 * - queue.RETURN_TIME.returnStart → returnStart
 * - queue.PAID_RETURN_TIME.price → price
 * - etc.
 *
 * NOTE: This entity will be populated in Phase 3 (Live Data integration).
 * For Phase 2, we only fetch metadata (parks/attractions), not live data.
 */
@Entity("queue_data")
@Index(["attractionId", "timestamp"])
@Index(["queueType", "timestamp"])
@Index(["attractionId", "queueType", "timestamp"])
@Index("idx_queue_data_operating", ["attractionId", "timestamp"], {
  where: "\"status\" = 'OPERATING'",
})
@Index(["queueType", "status", "timestamp"])
export class QueueData {
  @PrimaryColumn({ type: "uuid" })
  id: string;

  @ManyToOne(() => Attraction)
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column({ type: "text" })
  attractionId: string;

  @Column({
    type: "enum",
    enum: QueueType,
  })
  queueType: QueueType;

  @Column({
    type: "enum",
    enum: LiveStatus,
  })
  status: LiveStatus;

  // Standby/Single Rider/Paid Standby fields
  @Column({ nullable: true })
  waitTime: number; // minutes

  // Virtual Queue / Return Time fields
  @Column({ type: "text", nullable: true })
  state: string;

  @Column({ type: "timestamptz", nullable: true })
  returnStart: Date;

  @Column({ type: "timestamptz", nullable: true })
  returnEnd: Date;

  // Paid options (Lightning Lane, etc.)
  @Column({ type: "jsonb", nullable: true })
  price: {
    amount: number;
    currency: string;
    formatted?: string;
  };

  // Boarding Groups fields
  @Column({ type: "text", nullable: true })
  allocationStatus: string;

  @Column({ nullable: true })
  currentGroupStart: number;

  @Column({ nullable: true })
  currentGroupEnd: number;

  @Column({ nullable: true })
  estimatedWait: number;

  // API timestamp: when ThemeParks.wiki last updated this data
  @Column({ type: "timestamptz", nullable: true })
  lastUpdated: Date | null;

  // Multi-source tracking
  @Column({ type: "text", name: "data_source", default: "themeparks-wiki" })
  dataSource: string; // 'themeparks-wiki', 'queue-times'

  @PrimaryColumn({ type: "timestamptz" })
  timestamp: Date;

  @BeforeInsert()
  generateId() {
    if (!this.id) {
      this.id = crypto.randomUUID();
    }
    if (!this.timestamp) {
      this.timestamp = new Date();
    }
  }
}
