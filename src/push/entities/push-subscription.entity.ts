import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
  UpdateDateColumn,
} from "typeorm";

/**
 * One browser that asked to be told about a trip.
 *
 * **Postgres, not Redis.** This instance runs `maxmemory 512mb` with
 * `allkeys-lru` (`docker-compose.yml`), so Redis is free to evict any key at any
 * moment — a subscription store there would silently lose subscribers, and the
 * symptom would be notifications quietly not arriving, which nobody reports and
 * nothing measures. The rate limiter next door IS in Redis, and that asymmetry
 * is the point: an evicted counter resets a window, an evicted subscription is
 * gone.
 *
 * The `endpoint` is the identity, not the id column. A push service issues one
 * URL per browser and hands the SAME one back when a page re-subscribes, so the
 * write is an upsert on it — an insert would accumulate a row per page load and
 * then send the same notification four times.
 */
@Entity("push_subscriptions")
@Index("idx_push_subscriptions_trip", ["tripId"])
export class PushSubscription {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  /**
   * The push service's URL for this browser. Unique: it is the browser's
   * identity as far as anyone here can tell.
   *
   * Long by specification and long in practice — FCM endpoints run past 200
   * characters — so `text` rather than a guessed `varchar` length, with the
   * uniqueness carried by an index rather than by a column constraint that
   * would need a length.
   */
  @Index("idx_push_subscriptions_endpoint", { unique: true })
  @Column({ type: "text" })
  endpoint: string;

  /** The browser's public key, base64url. Half of the payload encryption. */
  @Column({ type: "text" })
  p256dh: string;

  /** The browser's auth secret, base64url. The other half. */
  @Column({ type: "text" })
  auth: string;

  /** Which trip this browser wants to hear about. */
  @Column({ type: "varchar", length: 32 })
  tripId: string;

  /**
   * The language to write in, and the zone to reckon in.
   *
   * Both are the SUBSCRIBER's rather than the park's, and they have to be stored
   * rather than derived at send time: the job runs on a server with no request
   * to read them from, and a notification in the wrong language is worse than
   * none — it is an unreadable interruption on a lock screen.
   */
  @Column({ type: "varchar", length: 12, default: "en" })
  locale: string;

  @Column({ type: "varchar", length: 64, nullable: true })
  timezone: string | null;

  /**
   * What this browser wants to be told about.
   *
   * Stored as a list rather than as columns so a new kind of notification does
   * not need a schema change on a table with a live subscriber base.
   */
  @Column({ type: "jsonb", default: () => "'[]'::jsonb" })
  topics: string[];

  /**
   * Consecutive delivery failures.
   *
   * A push service answering 404 or 410 means the subscription is dead — the
   * browser was uninstalled, the permission revoked — and the row should go.
   * Anything else (a 500, a timeout) is the service having a bad minute and must
   * not cost somebody their notifications, so it counts rather than deletes, and
   * the count resets on the next success.
   */
  @Column({ type: "int", default: 0 })
  failureCount: number;

  /** The last instant something was sent to this endpoint, for the dedup window. */
  @Column({ type: "timestamptz", nullable: true })
  lastNotifiedAt: Date | null;

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
