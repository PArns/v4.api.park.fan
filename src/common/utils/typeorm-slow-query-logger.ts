import { Logger as TypeOrmLogger, QueryRunner } from "typeorm";
import { Logger } from "@nestjs/common";
import { logToFile } from "./file-logger.util";

/**
 * Custom TypeORM logger that writes slow queries to logs/slow-queries.log.
 * All other query events (errors, schema builds) are forwarded to the NestJS
 * Logger so they still appear in docker logs and follow the standard format.
 */
export class SlowQueryFileLogger implements TypeOrmLogger {
  private readonly logger = new Logger("TypeORM");

  logQuerySlow(
    time: number,
    query: string,
    parameters?: unknown[],
    _queryRunner?: QueryRunner,
  ): void {
    logToFile("slow-queries", {
      durationMs: time,
      query,
      parameters: parameters?.length ? parameters : undefined,
    });
  }

  logQuery(): void {}
  logQueryError(
    error: string | Error,
    query: string,
    parameters?: unknown[],
  ): void {
    // Keep query errors visible in the main log
    this.logger.error(`Query failed: ${query}`, {
      error,
      parameters,
    });
  }
  logSchemaBuild(message: string): void {
    this.logger.log(`Schema: ${message}`);
  }
  logMigration(message: string): void {
    this.logger.log(`Migration: ${message}`);
  }
  log(level: "log" | "info" | "warn", message: unknown): void {
    if (level === "warn") {
      this.logger.warn(String(message));
    }
  }
}
