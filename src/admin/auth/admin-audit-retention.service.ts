import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { truncateIp } from "../../common/utils/request.util";
import { AdminAuditLog } from "./entities/admin-audit-log.entity";

/**
 * How long an audit row keeps the network its actor came from. Past it the
 * column is cleared and the row stays: who did what, and when, outlives the one
 * detail that says from where.
 */
export const AUDIT_ADDRESS_RETENTION_DAYS = 90;

/** Rows the address sweep reads, and writes back, in one statement. */
export const AUDIT_SWEEP_BATCH_SIZE = 500;

/** What one run of the address sweep did, in rows. */
export interface AuditAddressSweep {
  /** Cleared because the row is older than the retention. */
  expired: number;
  /** Full addresses cut to their network. */
  truncated: number;
  /** Cleared because the text was no address and no network we wrote. */
  unreadable: number;
}

/** Sorts below every uuid, so the first page of the sweep starts at the first row. */
const FIRST_ID = "00000000-0000-0000-0000-000000000000";

/**
 * What a stored value is cut to: a full address to its network, a network
 * already cut to itself, null for text that is neither.
 *
 * `truncateIp` reads an address, not the `/24` it writes, so the suffix goes
 * first; that is what makes a second run find nothing to do.
 */
function networkOf(stored: string): string | null {
  return truncateIp(stored.split("/", 1)[0]);
}

/**
 * The address half of the audit log's retention.
 *
 * Brings the column to the state `AdminAuditService.record` writes: a full
 * address already stored becomes its network, and no row keeps even the network
 * past {@link AUDIT_ADDRESS_RETENTION_DAYS}. A run reaches that state from any
 * start, so one that dies half way is simply run again.
 */
@Injectable()
export class AdminAuditRetentionService {
  constructor(
    @InjectRepository(AdminAuditLog)
    private readonly repository: Repository<AdminAuditLog>,
  ) {}

  /**
   * Clears the old and cuts the rest, in that order, so an old row is never
   * rewritten before it is emptied. Returns the counts and nothing that
   * identifies a row: the job logs them, and a log line carries no address
   * (docs/rules/no-address-in-logs.md).
   */
  async sweepAddresses(): Promise<AuditAddressSweep> {
    const expired = await this.clearExpired();
    const { truncated, unreadable } = await this.cutRemaining();
    return { expired, truncated, unreadable };
  }

  /**
   * One statement, on `idx_admin_audit_created`. The cut-off is the database's
   * clock minus an interval, an instant and not a park's day, so no timezone
   * helper applies (docs/rules/park-timezone.md).
   */
  private async clearExpired(): Promise<number> {
    const rows: Array<{ cleared: number }> = await this.repository.query(
      `WITH cleared AS (
         UPDATE admin_audit_log
            SET actor_ip = NULL
          WHERE actor_ip IS NOT NULL
            AND created_at < now() - $1::interval
        RETURNING 1
       )
       SELECT count(*)::int AS cleared FROM cleared`,
      [`${AUDIT_ADDRESS_RETENTION_DAYS} days`],
    );
    return rows[0]?.cleared ?? 0;
  }

  /**
   * Walks the rows that still hold a value by primary key and writes back only
   * those whose network differs from what is stored. The cut itself is
   * `truncateIp`, in TypeScript, so there is one place that decides what a
   * network is. The update names rows by id, re-checks that the column is
   * still set (a concurrent sweep may have cleared it), and binds networks
   * only, never an address.
   */
  private async cutRemaining(): Promise<{
    truncated: number;
    unreadable: number;
  }> {
    let truncated = 0;
    let unreadable = 0;
    let after = FIRST_ID;

    for (;;) {
      const rows: Array<{ id: string; actor_ip: string }> =
        await this.repository.query(
          `SELECT id, actor_ip
             FROM admin_audit_log
            WHERE actor_ip IS NOT NULL AND id > $1::uuid
            ORDER BY id
            LIMIT $2`,
          [after, AUDIT_SWEEP_BATCH_SIZE],
        );
      if (rows.length === 0) break;
      after = rows[rows.length - 1].id;

      const cuts = new Map<string, string | null>();
      for (const row of rows) {
        const network = networkOf(row.actor_ip);
        if (network !== row.actor_ip) cuts.set(row.id, network);
      }

      if (cuts.size > 0) {
        const changed: Array<{ id: string }> = await this.repository.query(
          `WITH changed AS (
             UPDATE admin_audit_log AS log
                SET actor_ip = cut.network
               FROM unnest($1::uuid[], $2::text[]) AS cut(id, network)
              WHERE log.id = cut.id AND log.actor_ip IS NOT NULL
            RETURNING log.id
           )
           SELECT id FROM changed`,
          [[...cuts.keys()], [...cuts.values()]],
        );
        for (const { id } of changed) {
          if (cuts.get(id) === null) unreadable++;
          else truncated++;
        }
      }

      if (rows.length < AUDIT_SWEEP_BATCH_SIZE) break;
    }

    return { truncated, unreadable };
  }
}
