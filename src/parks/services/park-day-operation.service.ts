import { forwardRef, Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { formatInTimeZone } from "date-fns-tz";
import { ParkDayOperation } from "../entities/park-day-operation.entity";
import { ParksService } from "../parks.service";
import {
  isMeasuredOperationDay,
  MeasuredOperationDayStats,
} from "../utils/measured-operation.gate";
import { MEASURED_OPERATION_DAY_STATS_SQL } from "../utils/measured-operation.sql";

/** The park fields a verdict needs. Keeps the callers free of the entity. */
export interface ParkDayOperationTarget {
  id: string;
  slug: string;
  timezone: string;
}

/** What one range run wrote, for the job log. */
export interface ParkDayOperationRunResult {
  daysJudged: number;
  operatingDays: number;
}

/**
 * The measured-operation verdict per park-local day: taken here, stored in
 * `park_day_operations`, read by everything that has to know whether a day the
 * operator called shut was actually an operating day.
 *
 * Three steps per day, in this order, and the order is the whole point of the
 * design (PAR-697, decision of 2026-10-07):
 *
 * 1. {@link MEASURED_OPERATION_DAY_STATS_SQL} reduces the day's qualifying raw
 *    readings to four numbers — one aggregate over one day's chunks.
 * 2. `isMeasuredOperationDay` judges those numbers. Pure, so the thresholds are
 *    tested against the two Walibi Holland days without a database.
 * 3. The row is upserted, and the readers only ever do a primary-key lookup.
 *
 * Doing it this way is what makes the rule affordable at all: in the read path
 * the same gate cost the ML-accuracy query 1.4 s → 7.4 s, because every reader
 * re-derived it per candidate day.
 */
@Injectable()
export class ParkDayOperationService {
  private readonly logger = new Logger(ParkDayOperationService.name);

  constructor(
    @InjectRepository(ParkDayOperation)
    private readonly repository: Repository<ParkDayOperation>,
    @Inject(forwardRef(() => ParksService))
    private readonly parksService: ParksService,
  ) {}

  /**
   * The days in `[fromDay, toDay]` whose stored verdict says the park operated.
   *
   * A day with no row is absent from the result: nobody judged it, so the
   * operator's entry stands. Both bounds are park-local `YYYY-MM-DD` and
   * inclusive.
   */
  async getMeasuredOperationDays(
    parkId: string,
    fromDay: string,
    toDay: string,
  ): Promise<Set<string>> {
    const rows: Array<{ day: Date | string }> = await this.repository.query(
      `SELECT day
         FROM park_day_operations
        WHERE park_id = $1::uuid
          AND day BETWEEN $2::date AND $3::date
          AND measured_operation`,
      [parkId, fromDay, toDay],
    );

    // `day` comes back as the driver's mapping of a `date` — a string on some
    // drivers, a Date at UTC midnight on others. Both read back as the same
    // park-local calendar day that was written.
    return new Set(
      rows.map((r) =>
        typeof r.day === "string" ? r.day : r.day.toISOString().slice(0, 10),
      ),
    );
  }

  /**
   * Judges every park-local day in `[fromDay, toDay]` for one park and stores
   * the verdicts.
   *
   * Idempotent: the rows are upserted on (park_id, day), so re-running a day
   * refreshes its verdict and its `computed_at`. That is what lets the nightly
   * job and the fill share one path — and what lets a day be re-judged after a
   * threshold changes.
   *
   * The derived hours come from one `getDerivedHistoricalHours` call over the
   * whole range rather than one per day, because that function reads the
   * `attraction_hourly_history` rollup and caches per range.
   *
   * **`toDay` is clamped to the park's last finished day.** The gate judges a
   * day's shape — how long its block ran, whether it has variety — and a day in
   * progress has none of that yet. The clamp lives here rather than in the
   * callers because the readers would honour such a row: the calendar refuses a
   * non-past day on its own, the four statistics callers do not, and the two
   * disagreeing about one day is exactly what this issue closes. A fill asked
   * for `… → today` therefore stops at yesterday instead of writing a verdict
   * that only half the readers would apply.
   */
  async computeRange(
    park: ParkDayOperationTarget,
    fromDay: string,
    toDay: string,
  ): Promise<ParkDayOperationRunResult> {
    const lastFinishedDay = parkLocalDayBefore(
      formatInTimeZone(new Date(), park.timezone, "yyyy-MM-dd"),
    );
    const judgeableTo = toDay < lastFinishedDay ? toDay : lastFinishedDay;
    const days = enumerateParkLocalDays(fromDay, judgeableTo);
    if (days.length === 0) return { daysJudged: 0, operatingDays: 0 };

    const derivedHours = await this.parksService
      .getDerivedHistoricalHours(park.id, fromDay, judgeableTo, park.timezone)
      .catch((err: unknown) => {
        // The hours are stored beside the verdict, not part of it. A rollup
        // that is not there yet must not cost the day its judgement.
        this.logger.warn(
          `Derived hours unavailable for ${park.slug} ${fromDay}…${judgeableTo}: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
        return new Map<string, { openingTime: string; closingTime: string }>();
      });

    const rows: ParkDayOperation[] = [];
    const computedAt = new Date();
    for (const day of days) {
      const stats = await this.collectStats(park.id, day, park.timezone);
      const hours = derivedHours.get(day);
      rows.push(
        this.repository.create({
          parkId: park.id,
          day,
          measuredOperation: isMeasuredOperationDay(stats),
          derivedOpen: hours ? new Date(hours.openingTime) : null,
          derivedClose: hours ? new Date(hours.closingTime) : null,
          computedAt,
        }),
      );
    }

    await this.repository.upsert(rows, ["parkId", "day"]);

    return {
      daysJudged: rows.length,
      operatingDays: rows.filter((r) => r.measuredOperation).length,
    };
  }

  /**
   * One day's qualifying readings as the four numbers the gate judges.
   *
   * The aggregate has no `GROUP BY`, so it always returns exactly one row — a
   * day without a single qualifying reading comes back with `block_minutes`
   * null, which the gate reads as "no day here".
   */
  private async collectStats(
    parkId: string,
    day: string,
    timezone: string,
  ): Promise<MeasuredOperationDayStats> {
    const rows: Array<{
      distinct_waits: number | string | null;
      dead_hour_readings: number | string | null;
      block_minutes: number | string | null;
    }> = await this.repository.query(MEASURED_OPERATION_DAY_STATS_SQL, [
      parkId,
      day,
      timezone,
    ]);

    const row = rows[0];
    return {
      distinctWaits: Number(row?.distinct_waits ?? 0),
      deadHourReadings: Number(row?.dead_hour_readings ?? 0),
      blockMinutes:
        row?.block_minutes === null || row?.block_minutes === undefined
          ? null
          : Number(row.block_minutes),
    };
  }
}

/** The park-local calendar day before `day`, as `YYYY-MM-DD`. */
export function parkLocalDayBefore(day: string): string {
  const cursor = new Date(`${day}T00:00:00Z`);
  cursor.setUTCDate(cursor.getUTCDate() - 1);
  return cursor.toISOString().slice(0, 10);
}

/**
 * The inclusive list of `YYYY-MM-DD` days between two park-local endpoints.
 *
 * Walked in UTC on purpose: both endpoints are already park-local calendar
 * days, so this is string arithmetic on a calendar and must not be shifted by a
 * timezone a second time.
 */
export function enumerateParkLocalDays(
  fromDay: string,
  toDay: string,
): string[] {
  const out: string[] = [];
  const cursor = new Date(`${fromDay}T00:00:00Z`);
  const end = new Date(`${toDay}T00:00:00Z`);
  while (cursor <= end) {
    out.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}
