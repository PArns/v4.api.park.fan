import { Column, Entity, PrimaryColumn, UpdateDateColumn } from "typeorm";

/**
 * One value of a member's own storage: whatever a members-only feature keeps for its account,
 * under a key the feature chooses (`notes`, `planner.presets`). The value is JSON and opaque to
 * the API, which only bounds its size and the number of keys; the account's deletion removes
 * every row (`AccountService`).
 */
@Entity("account_storage")
export class AccountStorageEntry {
  @PrimaryColumn({ name: "user_id", type: "uuid" })
  userId: string;

  @PrimaryColumn({ type: "varchar", length: 64 })
  key: string;

  @Column({ type: "jsonb" })
  value: unknown;

  @UpdateDateColumn({ name: "updated_at", type: "timestamptz" })
  updatedAt: Date;
}
