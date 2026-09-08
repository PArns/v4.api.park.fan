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
import { Show } from "../../shows/entities/show.entity";

/**
 * One browser's wish to be told 30 minutes before a show starts.
 *
 * No threshold, no edge-trigger state — a show has no parameter, and "which
 * showtime already notified" is deduplicated the same way `next-up` already
 * is: a Redis marker keyed by a pure `dedupeKey`, in
 * `push-notification.processor.ts`. `onDelete: "CASCADE"` on both foreign
 * keys for the same reason as `RideAlert` — a follow has no purpose once its
 * subscription or its show is gone.
 */
@Entity("show_follows")
@Index(["subscriptionId", "showId"], { unique: true })
@Index("idx_show_follows_show", ["showId"])
export class ShowFollow {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @ManyToOne(() => PushSubscription, { onDelete: "CASCADE" })
  @JoinColumn({ name: "subscriptionId" })
  subscription: PushSubscription;

  @Column()
  subscriptionId: string;

  @ManyToOne(() => Show, { onDelete: "CASCADE" })
  @JoinColumn({ name: "showId" })
  show: Show;

  @Column()
  showId: string;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
