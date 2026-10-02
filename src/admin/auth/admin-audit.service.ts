import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { AdminAuditLog } from "./entities/admin-audit-log.entity";
import type { AdminPrincipal } from "./admin-principal";

export interface AuditWrite {
  actor: AdminPrincipal;
  action: string;
  entityType: string;
  entityId?: string | null;
  entityLabel?: string | null;
  before?: Record<string, unknown> | null;
  after?: Record<string, unknown> | null;
  reason?: string | null;
  sourceUrl?: string | null;
}

/**
 * A curated value checked against its source and found right. The row's
 * `before` holds the values as they stood, `after` is null, and nothing was
 * written to the park (PAR-622).
 */
export const PARK_VERIFY_ACTION = "park.verify";

/**
 * Whether an audit row records a change or a check that changed nothing.
 *
 * Handed to the admin with every history entry, because a verify row has a
 * `before` and no `after`, which is the same shape a delete has, and the
 * history would otherwise render "checked against the source" as "removed".
 */
export type AuditEntryKind = "change" | "verification";

export function auditEntryKind(action: string): AuditEntryKind {
  return action.endsWith(".verify") ? "verification" : "change";
}

export type AuditEntry = AdminAuditLog & { kind: AuditEntryKind };

export interface AuditQuery {
  entityType?: string;
  entityId?: string;
  actorId?: string;
  action?: string;
  limit?: number;
  offset?: number;
}

/**
 * Writes and reads the administrative audit trail.
 *
 * Deliberately never throws into the caller. An audit failure must not undo a
 * curation the operator has already been told succeeded — the write has
 * happened, the log is the weaker guarantee of the two, and a swallowed-and-
 * logged failure is honest about which one gave way. (The alternative,
 * wrapping every curation in a transaction with its audit row, was rejected
 * because the cache invalidation and the frontend revalidation that follow a
 * curation are not transactional either, so the atomicity would be an
 * illusion held over one of four steps.)
 */
@Injectable()
export class AdminAuditService {
  private readonly logger = new Logger(AdminAuditService.name);

  constructor(
    @InjectRepository(AdminAuditLog)
    private readonly auditRepository: Repository<AdminAuditLog>,
  ) {}

  async record(entry: AuditWrite): Promise<AdminAuditLog | null> {
    try {
      const row = this.auditRepository.create({
        actorId: entry.actor.userId,
        actorEmail: entry.actor.email,
        action: entry.action,
        entityType: entry.entityType,
        entityId: entry.entityId ?? null,
        entityLabel: entry.entityLabel ?? null,
        before: entry.before ?? null,
        after: entry.after ?? null,
        reason: entry.reason ?? null,
        sourceUrl: entry.sourceUrl ?? null,
        actorIp: entry.actor.ip ?? null,
      });
      return await this.auditRepository.save(row);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(
        `Could not write audit row for ${entry.action} on ${entry.entityType}:${entry.entityId ?? "-"}: ${message}`,
      );
      return null;
    }
  }

  /**
   * The changed-fields diff between two snapshots.
   *
   * Returns null when nothing changed, which is how the curation endpoints
   * avoid writing an audit row for a save that saved nothing — a UI that
   * PATCHes the whole form on every blur would otherwise fill the log with
   * empty edits and bury the real ones.
   */
  static diff(
    before: Record<string, unknown>,
    after: Record<string, unknown>,
  ): {
    before: Record<string, unknown>;
    after: Record<string, unknown>;
  } | null {
    const changedBefore: Record<string, unknown> = {};
    const changedAfter: Record<string, unknown> = {};
    let changed = false;

    for (const key of Object.keys(after)) {
      const a = before[key];
      const b = after[key];
      // JSON comparison, because the values compared here are exactly what
      // goes into a jsonb column: scalars, nulls, and small arrays such as
      // season months or glossary term ids. Reference equality would report
      // every array as changed.
      if (JSON.stringify(a ?? null) === JSON.stringify(b ?? null)) continue;
      changedBefore[key] = a ?? null;
      changedAfter[key] = b ?? null;
      changed = true;
    }

    return changed ? { before: changedBefore, after: changedAfter } : null;
  }

  async list(query: AuditQuery = {}): Promise<{
    entries: AuditEntry[];
    total: number;
  }> {
    const qb = this.auditRepository
      .createQueryBuilder("log")
      // Property name — see the note in ParkSeasonService.list().
      .orderBy("log.createdAt", "DESC")
      .take(Math.min(query.limit ?? 50, 200))
      .skip(query.offset ?? 0);

    if (query.entityType) {
      qb.andWhere("log.entity_type = :entityType", {
        entityType: query.entityType,
      });
    }
    if (query.entityId) {
      qb.andWhere("log.entity_id = :entityId", { entityId: query.entityId });
    }
    if (query.actorId) {
      qb.andWhere("log.actor_id = :actorId", { actorId: query.actorId });
    }
    if (query.action) {
      // Prefix match so `park.` selects the whole family of park actions —
      // the useful question is almost always "what happened to parks", not
      // "what happened via park.season.update".
      qb.andWhere("log.action LIKE :action", { action: `${query.action}%` });
    }

    const [rows, total] = await qb.getManyAndCount();
    const entries = rows.map((row) =>
      Object.assign(row, { kind: auditEntryKind(row.action) }),
    );
    return { entries, total };
  }

  /** One audit row by id, for the undo path. */
  async findOne(id: string): Promise<AdminAuditLog | null> {
    return this.auditRepository.findOne({ where: { id } });
  }

  /** Mark an entry as undone by another entry. */
  async markReverted(id: string, revertedBy: string): Promise<void> {
    await this.auditRepository.update({ id }, { revertedBy });
  }
}
