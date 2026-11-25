import { Column, Entity, PrimaryColumn, UpdateDateColumn } from "typeorm";
import type { ApnsEnvironment } from "../apns";
import type { ApnsAnswerState } from "../apns-key";

/** What Apple answered the last time the key was checked in one environment. */
export interface ApnsCheckResult {
  at: string;
  status: number;
  reason: string;
  state: ApnsAnswerState;
  text: string;
  /** The key the answer is about. */
  fingerprint: string;
}

/** The one row of the APNs key uploaded in the admin; `id` is always `1`. */
@Entity("apns_settings")
export class ApnsSettings {
  @PrimaryColumn({ type: "smallint", default: 1 })
  id: number;

  /** `sealSecret()` output of the `.p8` file's PEM; null when none is uploaded. */
  @Column({ type: "text", nullable: true })
  keySealed: string | null;

  @Column({ type: "varchar", length: 10, nullable: true })
  keyId: string | null;

  @Column({ type: "varchar", length: 10, nullable: true })
  teamId: string | null;

  @Column({ type: "varchar", length: 155, nullable: true })
  bundleId: string | null;

  /** The first 8 hex characters of the SHA-256 over the public key. */
  @Column({ type: "varchar", length: 16, nullable: true })
  fingerprint: string | null;

  @Column({ type: "varchar", length: 200, nullable: true })
  uploadedBy: string | null;

  @Column({ type: "timestamptz", nullable: true })
  uploadedAt: Date | null;

  /** The last check per environment, of the key in use, uploaded or not. */
  @Column({ type: "jsonb", nullable: true })
  checks: Partial<Record<ApnsEnvironment, ApnsCheckResult>> | null;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
