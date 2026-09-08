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
 * One browser's wish to be told before a show starts.
 *
 * "Which showtime already notified" is deduplicated the same way `next-up`
 * already is: a Redis marker keyed by a pure `dedupeKey`, in
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

  /**
   * The performance this follow is about, or null for "whichever is next".
   *
   * A show runs several times a day and the one somebody wants is not always
   * the next one — a visitor standing in the park at 17:00 may be booked
   * until 18:30 and want the 19:10 performance, which is the whole reason
   * this column exists. Null keeps the original open-ended behaviour, which
   * is what every row written before this column did and what a follow made
   * from a card's bell (rather than from one of its showtime badges) still
   * means.
   *
   * An absolute instant, not a wall-clock time: it names one performance on
   * one day, so it stops matching by itself once that performance is over,
   * and the reader never has to guess which day a bare "19:10" belonged to.
   */
  @Column({ type: "timestamptz", nullable: true })
  startTime: Date | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
