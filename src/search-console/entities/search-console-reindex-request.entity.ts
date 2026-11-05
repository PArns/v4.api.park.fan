import {
  Column,
  CreateDateColumn,
  Entity,
  Index,
  PrimaryGeneratedColumn,
} from "typeorm";

/**
 * One "request indexing" an admin sent from the reindex queue: which page,
 * when and by whom. Google offers no API for the request itself, so the admin
 * opens Search Console and this row is the record that it happened; it also
 * counts the day's requests against Google's roughly ten a day.
 */
@Entity("search_console_reindex_requests")
@Index("search_console_reindex_requests_at_idx", ["requestedAt"])
export class SearchConsoleReindexRequest {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ type: "varchar", length: 820 })
  path: string;

  @CreateDateColumn({ name: "requested_at", type: "timestamptz" })
  requestedAt: Date;

  /** The admin's display name, or the token's client. */
  @Column({ type: "text" })
  actor: string;
}
