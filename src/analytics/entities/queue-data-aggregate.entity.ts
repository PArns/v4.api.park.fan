import {
  Entity,
  PrimaryColumn,
  Column,
  Index,
  CreateDateColumn,
  UpdateDateColumn,
  BeforeInsert,
} from "typeorm";
import { v4 as uuidv4 } from "uuid";

/**
 * Queue Data Aggregate Entity
 *
 * Stores pre-computed hourly percentiles for queue data.
 * Used for:
 * - Fast ML feature lookups (temporal percentiles)
 * - Analytics API (distribution metrics)
 * - Performance optimization (avoid expensive on-the-fly calculations)
 *
 * Computation Strategy:
 * - Calculated daily via batch job (queue-percentile.processor)
 * - Uses PostgreSQL percentile_cont() for efficiency
 * - TimescaleDB hypertable for time-series optimization
 *
 * Primary Percentile: P90 (90th percentile) - Universal standard
 * - Baseline for occupancy scores
 * - Load rating calculations
 * - ML feature engineering
 */
@Entity("queue_data_aggregates")
@Index(["attractionId", "hour"])
@Index(["parkId", "hour"])
@Index("queue_data_aggregates_hour_idx", ["hour"])
export class QueueDataAggregate {
  // Composite Primary Key (required for TimescaleDB)
  @PrimaryColumn("uuid")
  id: string;

  @PrimaryColumn({ type: "timestamptz" })
  hour: Date; // Hourly bucket (e.g., 2025-01-15 14:00:00)

  @Column({ type: "text" })
  @Index()
  attractionId: string;

  @Column({ type: "text" })
  @Index()
  parkId: string;

  // Percentile Distribution (comprehensive coverage)
  @Column({ type: "float" })
  p25: number; // 25th percentile (lower quartile)

  @Column({ type: "float" })
  p50: number; // 50th percentile (median)

  @Column({ type: "float" })
  p75: number; // 75th percentile (upper quartile)

  @Column({ type: "float" })
  p90: number; // 90th percentile ← PRIMARY for all calculations

  @Column({ type: "float" })
  p95: number; // 95th percentile (available for comparison)

  @Column({ type: "float" })
  p99: number; // 99th percentile (extremes)

  // Spread/Volatility Metrics
  @Column({ type: "float" })
  iqr: number; // Interquartile Range (P75 - P25)

  @Column({ type: "float" })
  stdDev: number; // Standard Deviation

  @Column({ type: "float" })
  mean: number; // Average wait time

  // Data Quality
  @Column({ type: "int" })
  sampleCount: number; // Number of data points in this hour

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;

  @BeforeInsert()
  generateId(): void {
    if (!this.id) {
      this.id = uuidv4();
    }
  }
}
