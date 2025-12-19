import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectDataSource } from "@nestjs/typeorm";
import { DataSource } from "typeorm";

/**
 * TimescaleDB Initialization Service
 *
 * Converts time-series tables to TimescaleDB hypertables on startup.
 * Hypertables provide optimized storage and queries for time-series data.
 *
 * Tables converted:
 * - queue_data (timestamp) - Hourly wait time data
 * - forecast_data (createdAt) - Forecast predictions
 * - weather_data (date) - Daily weather data
 * - show_live_data (timestamp) - Show live status and showtimes (Phase 6.4)
 * - restaurant_live_data (timestamp) - Dining availability (Phase 6.4)
 *
 * IMPORTANT: Compression policies preserve hourly resolution!
 * - queue_data: Compress after 30 days (keeps hourly data)
 * - forecast_data: Compress after 7 days, aggregate to daily after 90 days
 * - weather_data: Compress after 60 days (daily resolution)
 * - show_live_data: Compress after 30 days (keeps hourly data)
 * - restaurant_live_data: Compress after 30 days (keeps hourly data)
 *
 * Strategy:
 * 1. Check if tables are already hypertables
 * 2. If not, truncate and convert (OK per user - early development phase)
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
      await this.createHypertable("queue_data", "timestamp", "1 day");
      await this.createHypertable("forecast_data", "createdAt", "1 day");
      await this.createHypertable("weather_data", "date", "7 days");
      await this.createHypertable("show_live_data", "timestamp", "1 day");
      await this.createHypertable("restaurant_live_data", "timestamp", "1 day");
      await this.createHypertable("queue_data_aggregates", "hour", "7 days");

      // Set up compression policies (preserves hourly data!)
      await this.setupCompressionPolicies();

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

      // Get row count before truncation
      const countResult = await this.dataSource.query(
        `SELECT COUNT(*) as count FROM ${tableName};`,
      );
      const rowCount = parseInt(countResult[0]?.count || "0");

      if (rowCount > 0) {
        this.logger.warn(
          `  ⚠️  ${tableName} has ${rowCount} rows. Truncating before conversion...`,
        );
        await this.dataSource.query(`TRUNCATE TABLE ${tableName} CASCADE;`);
        this.logger.debug(`  ✓ Truncated ${tableName}`);
      }

      // Get current primary key constraint name
      const pkName = await this.getPrimaryKeyConstraintName(tableName);

      // Drop primary key constraint
      if (pkName) {
        await this.dataSource.query(
          `ALTER TABLE ${tableName} DROP CONSTRAINT "${pkName}";`,
        );
        this.logger.debug(`  ✓ Dropped primary key constraint: ${pkName}`);
      }

      // Add composite primary key BEFORE creating hypertable
      // All live data tables use (id, timestamp) as composite PK
      // - queue_data, forecast_data: (id, timeColumn)
      // - weather_data: (parkId, timeColumn) - special case for weather
      // - show_live_data: (id, timeColumn)
      // - restaurant_live_data: (id, timeColumn)
      let compositePK: string;
      if (tableName === "weather_data") {
        compositePK = `"parkId", "${timeColumn}"`;
      } else {
        // All other tables use (id, timestamp)
        compositePK = `"id", "${timeColumn}"`;
      }

      await this.dataSource.query(
        `ALTER TABLE ${tableName} ADD PRIMARY KEY (${compositePK});`,
      );
      this.logger.debug(`  ✓ Added composite primary key (${compositePK})`);

      // Convert to hypertable
      await this.dataSource.query(
        `SELECT create_hypertable('${tableName}', '${timeColumn}', chunk_time_interval => INTERVAL '${chunkInterval}', if_not_exists => TRUE);`,
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
   * Set up compression policies for hypertables
   *
   * CRITICAL: Compression preserves hourly resolution!
   * - queue_data: Compress after 30 days (keeps all hourly data)
   * - forecast_data: Compress after 7 days (keeps all hourly data)
   * - weather_data: Compress after 60 days (daily data, no hourly loss)
   *
   * Compression reduces storage by 90%+ while maintaining query performance.
   */
  private async setupCompressionPolicies(): Promise<void> {
    this.logger.log("📦 Setting up compression policies...");

    try {
      // Enable compression on queue_data (compress after 30 days)
      await this.enableCompression(
        "queue_data",
        "timestamp",
        30,
        "Hourly wait time data",
      );

      // Enable compression on forecast_data (compress after 7 days)
      await this.enableCompression(
        "forecast_data",
        "createdAt",
        7,
        "Forecast predictions",
      );

      // Enable compression on weather_data (compress after 60 days)
      await this.enableCompression(
        "weather_data",
        "date",
        60,
        "Daily weather data",
      );

      // Enable compression on show_live_data (compress after 30 days)
      await this.enableCompression(
        "show_live_data",
        "timestamp",
        30,
        "Show live status and showtimes",
      );

      // Enable compression on restaurant_live_data (compress after 30 days)
      await this.enableCompression(
        "restaurant_live_data",
        "timestamp",
        30,
        "Dining availability and wait times",
      );

      // Enable compression on queue_data_aggregates (compress after 30 days)
      await this.enableCompression(
        "queue_data_aggregates",
        "hour",
        30,
        "Hourly percentile aggregates",
      );

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
        await this.dataSource.query(
          `ALTER TABLE ${tableName} SET (timescaledb.compress);`,
        );
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
        await this.dataSource.query(
          `SELECT add_compression_policy('${tableName}', INTERVAL '${compressAfterDays} days');`,
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
}
