import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, DataSource } from "typeorm";
import { AttractionReviewMark } from "../entities/attraction-review-mark.entity";

export interface ReviewMarkRequest {
  kind: "not_a_duplicate" | "not_retired";
  attractionId: string;
  /** Only for pair marks; order does not matter, it is canonicalised here. */
  otherAttractionId?: string | null;
  reason: string;
  /** ISO date; omit for a permanent mark. */
  recheckAfter?: string | null;
}

export interface RetirementCandidate {
  attractionId: string;
  park: string;
  name: string;
  wentSilent: string;
  maxWait: number;
}

/**
 * Records that a human has already investigated a candidate, so the
 * behavioural detectors stop re-asking the same question.
 *
 * See the entity for why this exists. The short version: detection describes
 * the feed, not the world, so its candidates never go away on their own.
 */
@Injectable()
export class AttractionReviewService {
  private readonly logger = new Logger(AttractionReviewService.name);

  constructor(
    @InjectRepository(AttractionReviewMark)
    private readonly markRepository: Repository<AttractionReviewMark>,
    private readonly dataSource: DataSource,
  ) {}

  async record(requests: ReviewMarkRequest[]): Promise<number> {
    let written = 0;

    for (const request of requests) {
      // Canonical order, so "A not a duplicate of B" and its mirror are one row.
      const [a, b] = request.otherAttractionId
        ? [request.attractionId, request.otherAttractionId].sort()
        : [request.attractionId, null];

      // Delete-then-insert rather than ON CONFLICT: a single-attraction mark
      // leaves other_attraction_id NULL, and Postgres treats NULLs in a unique
      // index as distinct, so the conflict target would never fire for exactly
      // the marks that need it. This is idempotent either way.
      await this.dataSource.query(
        `DELETE FROM attraction_review_marks
          WHERE kind = $1 AND attraction_id = $2
            AND other_attraction_id IS NOT DISTINCT FROM $3`,
        [request.kind, a, b],
      );
      const result = await this.dataSource.query(
        `INSERT INTO attraction_review_marks
           (id, kind, attraction_id, other_attraction_id, reason, recheck_after, reviewed_at)
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, now())
         RETURNING id`,
        [request.kind, a, b, request.reason, request.recheckAfter ?? null],
      );
      if (result.length > 0) written++;
    }

    this.logger.log(`📝 Recorded ${written} review mark(s)`);
    return written;
  }

  /**
   * Attractions that had a real queue, stopped reporting more than 30 days ago
   * on a date no sibling in their park shares, and are still receiving
   * reconciliation rows — minus everything a human has already cleared.
   *
   * The `same_day` clause is the load-bearing part: a whole block of a park
   * falling silent on one date is a seasonal closure (Wet'n'Wild's 13+9 on
   * 2026-06-29 is the Southern-Hemisphere winter), not a set of retirements.
   */
  async findRetirementCandidates(): Promise<RetirementCandidate[]> {
    const rows: Array<{
      id: string;
      park: string;
      name: string;
      d: string;
      max_wait: number;
    }> = await this.dataSource.query(`
      WITH act AS (
        SELECT "attractionId" AS aid,
               max(timestamp) FILTER (WHERE status = 'OPERATING') AS last_op,
               max(timestamp) AS last_row,
               max("waitTime") AS max_wait
          FROM queue_data
         WHERE timestamp > now() - interval '400 days'
         GROUP BY 1
      ), cand AS (
        SELECT a.id, a.name, a."parkId", p.name AS park,
               act.last_op::date AS d, act.max_wait
          FROM act
          JOIN attractions a ON a.id = act.aid
          JOIN parks p ON p.id = a."parkId"
         WHERE act.last_op < now() - interval '30 days'
           AND act.last_row > now() - interval '2 days'
           AND act.max_wait > 0
           AND a.retired_at IS NULL
      ), same_day AS (
        SELECT "parkId", d, count(*) AS n FROM cand GROUP BY 1, 2
      )
      SELECT c.id, c.park, c.name, c.d::text AS d, c.max_wait
        FROM cand c
        JOIN same_day s ON s."parkId" = c."parkId" AND s.d = c.d
        -- Already investigated, and the verdict has not expired.
        LEFT JOIN attraction_review_marks m
               ON m.attraction_id = c.id
              AND m.kind = 'not_retired'
              AND (m.recheck_after IS NULL OR m.recheck_after > CURRENT_DATE)
       WHERE s.n <= 2
         AND m.id IS NULL
       ORDER BY c.park, c.name
    `);

    return rows.map((r) => ({
      attractionId: r.id,
      park: r.park,
      name: r.name,
      wentSilent: r.d,
      maxWait: Number(r.max_wait),
    }));
  }
}
