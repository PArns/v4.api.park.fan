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

/** `RideAlert.kind` of an alert that fires when the ride opens, not on a wait time. */
export const RIDE_ALERT_KIND_REOPEN = "reopen";

/**
 * One browser's wish to be told when a ride's wait time drops, or (kind
 * `reopen`) when a ride that is closed opens again.
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

  /**
   * What this alert waits for. `null` is the original wait-time alert (fires
   * below `thresholdMinutes`); `"reopen"` fires on the first OPERATING reading
   * after the ride was seen not operating. One row per ride and subscription,
   * so changing the kind replaces the alert.
   */
  @Column({ type: "varchar", length: 16, nullable: true })
  kind: string | null;

  /**
   * Notify once the STANDBY wait drops below this many minutes. `null` for a
   * `reopen` alert, which has no wait-time promise to keep.
   */
  @Column({ type: "int", nullable: true })
  thresholdMinutes: number | null;

  /**
   * Edge-trigger state, not a duplicate of the threshold comparison.
   *
   * `true` — the next reading under the threshold fires. It flips to `false`
   * the moment it does, so a ride sitting at 12 minutes for three straight
   * polls sends one notification, not three. It flips back to `true` only once
   * the wait rises to the threshold or above — a real rearm, not a timer —
   * which is what lets the SAME alert fire again later the same day if the
   * queue builds back up and drops again.
   *
   * For a `reopen` alert `true` means "the ride was last seen not operating":
   * the next OPERATING reading fires, and only a later non-operating reading
   * arms it again.
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
