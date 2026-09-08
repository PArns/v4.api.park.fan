import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  JoinColumn,
  ManyToOne,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from "typeorm";
import { PushSubscription } from "../../push/entities/push-subscription.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";

/**
 * One browser's wish to be told when a ride's wait time drops.
 *
 * `onDelete: "CASCADE"` on both foreign keys, unlike `push_subscriptions.tripId`
 * (a loose string match, because a trip has its own independent lifecycle and
 * TTL). An alert has no purpose once its subscription is gone, and none once
 * the ride it watches is — cascading at the database means no sweep job has to
 * exist for either case.
 */
@Entity("ride_alerts")
@Index(["subscriptionId", "attractionId"], { unique: true })
@Index("idx_ride_alerts_attraction", ["attractionId"])
export class RideAlert {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @ManyToOne(() => PushSubscription, { onDelete: "CASCADE" })
  @JoinColumn({ name: "subscriptionId" })
  subscription: PushSubscription;

  @Column()
  subscriptionId: string;

  @ManyToOne(() => Attraction, { onDelete: "CASCADE" })
  @JoinColumn({ name: "attractionId" })
  attraction: Attraction;

  @Column()
  attractionId: string;

  /** Notify once the STANDBY wait drops below this many minutes. */
  @Column({ type: "int" })
  thresholdMinutes: number;

  /**
   * Edge-trigger state, not a duplicate of the threshold comparison.
   *
   * `true` — the next reading under the threshold fires. It flips to `false`
   * the moment it does, so a ride sitting at 12 minutes for three straight
   * polls sends one notification, not three. It flips back to `true` only once
   * the wait rises to the threshold or above — a real rearm, not a timer —
   * which is what lets the SAME alert fire again later the same day if the
   * queue builds back up and drops again.
   */
  @Column({ default: true })
  armed: boolean;

  @Column({ type: "timestamptz", nullable: true })
  lastTriggeredAt: Date | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
