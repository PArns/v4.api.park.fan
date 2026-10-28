import { Logger as TypeOrmLogger, QueryRunner } from "typeorm";
import { Logger } from "@nestjs/common";
import { isIP } from "net";
import { logToFile } from "./file-logger.util";
import { truncateIp } from "./request.util";

/**
 * The bound parameters of a statement for a log, with every address cut to its
 * network. A slow or failed `INSERT INTO admin_audit_log` carries `actor_ip`
 * as a parameter, and the privacy policy says no log line holds an address
 * (docs/rules/no-address-in-logs.md). Strings and the strings of an array are
 * checked; nothing else can be one.
 */
export function withoutAddresses(
  parameters: unknown[] | undefined,
): unknown[] | undefined {
  const cut = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(cut);
    if (typeof value !== "string") return value;
    const bare = value.startsWith("::ffff:") ? value.slice(7) : value;
    return isIP(bare) ? (truncateIp(value) ?? value) : value;
  };
  return parameters?.map(cut);
}

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
      parameters: parameters?.length ? withoutAddresses(parameters) : undefined,
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
      parameters: withoutAddresses(parameters),
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
