import { Logger as TypeOrmLogger, QueryRunner } from "typeorm";
import { logToFile } from "./file-logger.util";

/**
 * Custom TypeORM logger that writes slow queries to logs/slow-queries.log.
 * All other query events (errors, schema builds) are forwarded to the NestJS
 * default console output so they still appear in docker logs.
 */
export class SlowQueryFileLogger implements TypeOrmLogger {
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
    console.error("[TypeORM] Query failed:", { error, query, parameters });
  }
  logSchemaBuild(message: string): void {
    console.log("[TypeORM] Schema:", message);
  }
  logMigration(message: string): void {
    console.log("[TypeORM] Migration:", message);
  }
  log(level: "log" | "info" | "warn", message: unknown): void {
    if (level === "warn") console.warn("[TypeORM]", message);
  }
}
