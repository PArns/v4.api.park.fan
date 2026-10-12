import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";
import { HYPERTABLES } from "./hypertables";

/**
 * TimescaleDB Initialization Service
 *
 * Converts time-series tables to TimescaleDB hypertables on startup.
 * Hypertables provide optimized storage and queries for time-series data.
 *
 * **Which tables, on which column, with which chunk interval and compression
 * settings, is `HYPERTABLES` in `./hypertables.ts`** — not a list in this
 * docblock and not a sequence of calls below. The E2E schema reads the same
 * constant, which is what keeps the test schema from drifting into plain tables
 * behind a passing suite (PAR-172, PAR-234).
 *
 * Compression policies preserve the source resolution: a chunk is compressed,
 * never downsampled, so an hourly table stays hourly.
 *
 * Strategy:
 * 1. Check if tables are already hypertables
 * 2. If not, rebuild the primary key around the time column and convert
 * 3. Set up compression policies
 */
@Injectable()
export class TimescaleInitService implements OnModuleInit {
  private readonly logger = new Logger(TimescaleInitService.name);

  constructor(@InjectDataSource() private dataSource: DataSource) {}

  async onModuleInit(): Promise<void> {
    // Run async, don't block startup
    this.initializeHypertables().catch((err) => {
      this.logger.error("Failed to initialize TimescaleDB hypertables", err);
    });
  }

  private async initializeHypertables(): Promise<void> {
    this.logger.log("🕒 Initializing TimescaleDB hypertables...");

    try {
      // Check if TimescaleDB extension is installed
      const hasTimescale = await this.checkTimescaleExtension();
      if (!hasTimescale) {
        this.logger.warn(
          "⚠️  TimescaleDB extension not found. Skipping hypertable setup.",
        );
        return;
      }

      // Convert tables to hypertables
      for (const spec of HYPERTABLES) {
        await this.createHypertable(
          spec.table,
          spec.timeColumn,
          spec.chunkInterval,
        );
      }

      // Compression must never queue an ACCESS EXCLUSIVE lock (runbook §0b)
      await this.setupCompressTruncateBehaviour();

      // Set up compression policies (preserves hourly data!)
      await this.setupCompressionPolicies();

      // Retention is the cleanup-old job's, not a policy's (runbook §0b)
      await this.setupRetentionPolicies();

      this.logger.log("✅ TimescaleDB hypertables initialized successfully!");
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Failed to initialize hypertables: ${errorMessage}`,
        error instanceof Error ? error.stack : undefined,
      );
    }
  }

  /**
   * Check if TimescaleDB extension is installed
   */
  private async checkTimescaleExtension(): Promise<boolean> {
    const result = await this.dataSource.query(
      `SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') as installed;`,
    );
    return result[0]?.installed || false;
  }

  /**
   * Convert a table to a TimescaleDB hypertable
   */
  private async createHypertable(
    tableName: string,
    timeColumn: string,
    chunkInterval: string,
  ): Promise<void> {
    try {
      // Check if already a hypertable
      const isHypertable = await this.isHypertable(tableName);

      if (isHypertable) {
        this.logger.debug(`  ✓ ${tableName} is already a hypertable`);
        return;
      }

      // Get current primary key constraint name AND its columns — the columns have
      // to be read before the constraint is dropped.
      const pkName = await this.getPrimaryKeyConstraintName(tableName);
      const pkColumns = await this.getPrimaryKeyColumns(tableName);

      // Drop primary key constraint
      if (pkName) {
        // SECURITY: Both tableName and pkName are validated, but use identifier quoting
        await this.dataSource.query(
          `ALTER TABLE ${this.quoteIdentifier(tableName)} DROP CONSTRAINT ${this.quoteIdentifier(pkName)};`,
        );
        this.logger.debug(`  ✓ Dropped primary key constraint: ${pkName}`);
      }

      // Add composite primary key BEFORE creating hypertable.
      //
      // Timescale requires the partitioning column to be part of every unique index,
      // so the key becomes "whatever the entity already declares, plus the time
      // column". Deriving it from the live schema rather than assuming a surrogate
      // "id" is what lets tables with a natural key work: wait_time_predictions
      // dropped its uuid id for (attractionId, createdAt, predictedTime,
      // predictionType), and an assumed `id` failed hypertable creation outright on
      // any freshly created database.
      const keyColumns = [...pkColumns];
      if (keyColumns.length > 0 && !keyColumns.includes(timeColumn)) {
        keyColumns.push(timeColumn);
      }

      if (keyColumns.length > 0) {
        // SECURITY: Table name and columns are validated, but use identifier quoting
        const compositePK = keyColumns
          .map((column) => this.quoteIdentifier(column))
          .join(", ");
        await this.dataSource.query(
          `ALTER TABLE ${this.quoteIdentifier(tableName)} ADD PRIMARY KEY (${compositePK});`,
        );
        this.logger.debug(`  ✓ Added composite primary key (${compositePK})`);
      }

      // Convert to hypertable — migrate_data preserves existing rows
      await this.dataSource.query(
        `SELECT create_hypertable($1, $2, chunk_time_interval => $3::interval, if_not_exists => TRUE, migrate_data => TRUE);`,
        [tableName, timeColumn, chunkInterval],
      );

      this.logger.debug(
        `  ✅ Created hypertable: ${tableName} (time_column: ${timeColumn}, chunk_interval: ${chunkInterval})`,
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.error(
        `  ❌ Failed to create hypertable ${tableName}: ${errorMessage}`,
      );
      throw error;
    }
  }

  /**
   * Check if a table is already a hypertable
   */
  private async isHypertable(tableName: string): Promise<boolean> {
    const result = await this.dataSource.query(
      `SELECT EXISTS(
        SELECT 1 FROM timescaledb_information.hypertables
        WHERE hypertable_name = $1
      ) as is_hypertable;`,
      [tableName],
    );
    return result[0]?.is_hypertable || false;
  }

  /**
   * Get the primary key constraint name for a table
   */
  private async getPrimaryKeyConstraintName(
    tableName: string,
  ): Promise<string> {
    // SECURITY: Use parameterized query
    const result = await this.dataSource.query(
      `SELECT conname
       FROM pg_constraint
       WHERE conrelid = $1::regclass
       AND contype = 'p';`,
      [tableName],
    );
    return result[0]?.conname || `PK_${tableName}`;
  }

  /**
   * Get the primary key's columns, in key order.
   *
   * Read this BEFORE dropping the constraint — afterwards there is nothing left to
   * read, and the hypertable's key would have to be guessed.
   */
  private async getPrimaryKeyColumns(tableName: string): Promise<string[]> {
    // SECURITY: Use parameterized query
    const result: { attname: string }[] = await this.dataSource.query(
      `SELECT a.attname
       FROM pg_constraint c
       JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
       JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
       WHERE c.conrelid = $1::regclass
         AND c.contype = 'p'
       ORDER BY k.ord;`,
      [tableName],
    );
    return result.map((row) => row.attname);
  }

  /**
   * Quote SQL identifier to prevent injection
   * Only allows alphanumeric and underscore characters
   */
  private quoteIdentifier(identifier: string): string {
    // SECURITY: Validate identifier contains only safe characters
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(identifier)) {
      throw new Error(`Invalid SQL identifier: ${identifier}`);
    }
    return `"${identifier}"`;
  }

  /**
   * Set up compression policies for hypertables
   *
   * CRITICAL: Compression preserves the source resolution — a chunk is
   * compressed, never downsampled. The per-table delay and the optional
   * segment/order columns are `HYPERTABLES`.
   *
   * Compression reduces storage by 90%+ while maintaining query performance.
   */
  private async setupCompressionPolicies(): Promise<void> {
    this.logger.log("📦 Setting up compression policies...");

    try {
      for (const spec of HYPERTABLES) {
        await this.enableCompression(
          spec.table,
          spec.timeColumn,
          spec.compressAfterDays,
          spec.description,
          spec.segmentBy,
          spec.orderBy,
        );
      }

      this.logger.log("✅ Compression policies configured");
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `⚠️  Failed to set up compression policies: ${errorMessage}`,
      );
      // Don't throw - compression is optional optimization
    }
  }

  /**
   * Enable compression on a hypertable
   */
  private async enableCompression(
    tableName: string,
    timeColumn: string,
    compressAfterDays: number,
    description: string,
    segmentBy?: string,
    orderBy?: string,
  ): Promise<void> {
    try {
      // Check if compression is already enabled
      const compressionCheck = await this.dataSource.query(
        `SELECT compression_enabled FROM timescaledb_information.hypertables WHERE hypertable_name = $1;`,
        [tableName],
      );

      const isEnabled = compressionCheck[0]?.compression_enabled;

      if (!isEnabled) {
        // Enable compression
        // SECURITY: Table name is validated, but use identifier quoting
        let alterSql = `ALTER TABLE ${this.quoteIdentifier(tableName)} SET (timescaledb.compress`;
        if (segmentBy) {
          alterSql += `, timescaledb.compress_segmentby = '${this.quoteIdentifier(segmentBy)}'`;
        }
        if (orderBy) {
          const parts = orderBy.split(" ");
          const col = parts[0];
          const dir = parts[1] || "";
          alterSql += `, timescaledb.compress_orderby = '${this.quoteIdentifier(col)} ${dir}'`;
        }
        alterSql += `);`;

        await this.dataSource.query(alterSql);
        this.logger.debug(`  ✓ Enabled compression on ${tableName}`);
      }

      // Add compression policy (compress chunks older than N days)
      // Check if policy already exists
      const policyCheck = await this.dataSource.query(
        `SELECT * FROM timescaledb_information.jobs
         WHERE hypertable_name = $1 AND proc_name = 'policy_compression';`,
        [tableName],
      );

      if (policyCheck.length === 0) {
        // SECURITY: Use parameterized query for table name
        await this.dataSource.query(
          `SELECT add_compression_policy($1, $2::interval);`,
          [tableName, `${compressAfterDays} days`],
        );
        this.logger.debug(
          `  ✓ ${tableName}: Compress after ${compressAfterDays} days (${description})`,
        );
      } else {
        this.logger.debug(
          `  ✓ ${tableName}: Compression policy already exists`,
        );
      }
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `  ⚠️  Failed to enable compression on ${tableName}: ${errorMessage}`,
      );
    }
  }

  /**
   * Retention is NOT a TimescaleDB policy here, and this removes the one that
   * older boots added.
   *
   * `drop_chunks` locks every table the hypertable has a foreign key to ACCESS
   * EXCLUSIVE before it even looks for a chunk to drop, and
   * `wait_time_predictions` references `attractions`. A background policy waits
   * for that lock for its whole `max_runtime` — five minutes — while every
   * query on `attractions` queues behind the request. That is what took the API
   * down in five-minute blocks on 2026-10-07/08. The nightly `cleanup-old` job
   * now drops the chunks itself, under a lock_timeout
   * (`MLService.dropExpiredPredictionChunks`).
   *
   * Removing a job waits on the job's own lock while a run is in progress, so
   * the statement gets a lock_timeout too; a busy boot leaves the policy for
   * the next one rather than stalling here.
   */
  private async setupRetentionPolicies(): Promise<void> {
    for (const tableName of ["wait_time_predictions"]) {
      try {
        const existing: unknown[] = await this.dataSource.query(
          `SELECT 1 FROM timescaledb_information.jobs
           WHERE hypertable_name = $1 AND proc_name = 'policy_retention';`,
          [tableName],
        );
        if (existing.length === 0) continue;

        await this.dataSource.transaction(async (em) => {
          await em.query(`SELECT set_config('lock_timeout', '5s', true)`);
          await em.query(
            `SELECT remove_retention_policy($1, if_exists => true);`,
            [tableName],
          );
        });
        this.logger.log(
          `  ✓ ${tableName}: removed the TimescaleDB retention policy (the cleanup-old job drops chunks now)`,
        );
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        this.logger.warn(
          `  ⚠️  Could not remove the retention policy on ${tableName}: ${errorMessage}`,
        );
      }
    }
  }

  /**
   * Lets a compression job give up the final ACCESS EXCLUSIVE lock instead of
   * queueing for it.
   *
   * Compressing a chunk ends by truncating the uncompressed rows, which needs
   * ACCESS EXCLUSIVE on the chunk. Under the default `truncate_only` the job
   * waits for that lock with no deadline — the compression policies run with
   * `max_runtime` 0 — and every reader of the chunk queues behind the waiting
   * request. One long-lived reader of a 30-day-old `queue_data` chunk was enough
   * to take the API down: 2026-09-28 (PAR-563) and again 2026-10-09 02:12–07:31,
   * when the 02:12 run of job 1000 waited five hours.
   *
   * `truncate_or_delete` tries the lock without entering the queue (for up to
   * five seconds) and falls back to deleting the rows, which needs no lock
   * stronger than the one the job already holds. The fallback leaves dead
   * tuples for autovacuum; that is the whole cost.
   *
   * Set on the database rather than the session because the policies run in
   * TimescaleDB background workers, which read database-level settings when
   * they connect. Idempotent: written only when it differs.
   */
  private async setupCompressTruncateBehaviour(): Promise<void> {
    try {
      const rows: Array<{ setconfig: string[] | null }> =
        await this.dataSource.query(
          `SELECT s.setconfig
             FROM pg_db_role_setting s
             JOIN pg_database d ON d.oid = s.setdatabase
            WHERE d.datname = current_database() AND s.setrole = 0`,
        );
      const current = rows[0]?.setconfig ?? [];
      if (current.includes(COMPRESS_TRUNCATE_SETTING)) return;

      await this.dataSource.query(
        `DO $$ BEGIN
           EXECUTE format('ALTER DATABASE %I SET timescaledb.compress_truncate_behaviour = %L',
                          current_database(), 'truncate_or_delete');
         END $$;`,
      );
      this.logger.log(
        "  ✓ timescaledb.compress_truncate_behaviour = truncate_or_delete (database default)",
      );
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : String(error);
      this.logger.warn(
        `  ⚠️  Could not set compress_truncate_behaviour: ${errorMessage}`,
      );
    }
  }
}

/** The pg_db_role_setting entry `setupCompressTruncateBehaviour` writes. */
const COMPRESS_TRUNCATE_SETTING =
  "timescaledb.compress_truncate_behaviour=truncate_or_delete";
