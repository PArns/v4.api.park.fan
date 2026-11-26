import { Column, Entity, PrimaryColumn, UpdateDateColumn } from "typeorm";

/** What one platform's app must at least be, and where it updates; all null blocks nothing. */
export interface PlatformConfig {
  /** Apps below this version may not start. */
  minVersion: string | null;
  /** With the same version, apps below this build (iOS) / versionCode (Android) may not start. */
  minBuild: number | null;
  updateUrl: string | null;
  /** A note per language (`de`, `en`, …) for the block screen. */
  message: Partial<Record<string, string>> | null;
}

/** The one row of what the iOS app asks the server at start; `id` is always `1`. */
@Entity("app_config")
export class AppConfig {
  @PrimaryColumn({ type: "smallint", default: 1 })
  id: number;

  /** Per platform (`ios`, `android`, …) its minimum version block; a platform not in here blocks nothing. */
  @Column({ type: "jsonb", nullable: true })
  platforms: Record<string, PlatformConfig> | null;

  @Column({ type: "varchar", length: 200, nullable: true })
  updatedBy: string | null;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}
