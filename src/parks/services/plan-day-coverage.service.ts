import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Park } from "../entities/park.entity";
import { PlanDayCoverage } from "../entities/plan-day-coverage.entity";
import { PlanDayService } from "./plan-day.service";
import {
  isStructuralPlanDayReason,
  isUnknownPlanDayReason,
} from "../utils/plan-day-availability.util";
import { mapWithDbBudget } from "../../common/utils/db-job-budget";

/** How far ahead the sweep asks. */
const LEAD_DAYS = 30;

/** Parks per batch, matching the other park-wide nightly jobs. */
const BATCH_SIZE = 5;

export interface PlanDayCoverageSummary {
  measuredOn: string;
  plannedDate: string;
  /**
   * Parks with a park-wide OPERATING entry for the planned date — the whole
   * population, including the ones the sweep could not ask.
   *
   * Counted from the schedule rather than from the rows it managed to write. A
   * denominator that shrinks with every failure makes a systematic outage read
   * as an improving coverage rate, which is the one way this number could lie
   * in the reassuring direction.
   */
  parksOpen: number;
  /** Of those, the ones that produced at least one ride curve. */
  parksWithPlan: number;
  /** Of those, the ones that did not and where the gap is ours to close. */
  parksWithoutPlan: number;
  /** Empty plans whose reason is a property of the park, not a data gap. */
  parksStructural: number;
  /**
   * Parks the sweep could not get an answer about at all — `buildPlanDay`
   * threw, or a dependency behind it failed. Neither planned nor unplanned:
   * unmeasured.
   */
  parksFailed: number;
  byReason: Record<string, number>;
}

/**
 * The nightly answer to "how many open parks cannot be planned for".
 *
 * Asked at a fixed distance of {@link LEAD_DAYS} days rather than for
 * tomorrow, because that is where the endpoint is actually used — people book
 * summer in January — and because the near days hide the problem: inside the
 * model's 24-hour window a park can carry the model's own hourly answer without
 * any measured history at all.
 *
 * The sweep calls the real `buildPlanDay` rather than re-deriving the rules in
 * SQL. A second implementation of "would this park produce curves" is a second
 * thing to keep in step, and the first time the two disagree the counter
 * becomes the thing that is wrong. The cost is the cost of the endpoint on
 * warm caches, once a day, over the parks that are open.
 */
@Injectable()
export class PlanDayCoverageService {
  private readonly logger = new Logger(PlanDayCoverageService.name);

  constructor(
    @InjectRepository(Park)
    private readonly parkRepository: Repository<Park>,
    @InjectRepository(PlanDayCoverage)
    private readonly coverageRepository: Repository<PlanDayCoverage>,
    private readonly planDayService: PlanDayService,
  ) {}

  /** Runs the sweep and stores one row per open park. */
  async sweep(now: Date = new Date()): Promise<PlanDayCoverageSummary> {
    const measuredOn = PlanDayCoverageService.utcDate(now);
    const plannedDate = PlanDayCoverageService.utcDate(
      new Date(now.getTime() + LEAD_DAYS * 86_400_000),
    );

    const parks = await this.openParks(plannedDate);
    const rows: PlanDayCoverage[] = [];

    for (let i = 0; i < parks.length; i += BATCH_SIZE) {
      const batch = parks.slice(i, i + BATCH_SIZE);
      const built = await mapWithDbBudget(batch, async (park) => {
        try {
          const plan = await this.planDayService.buildPlanDay(
            park,
            plannedDate,
          );
          const reason = plan.ridesUnavailable?.reason ?? null;
          return this.coverageRepository.create({
            measuredOn,
            parkId: park.id,
            plannedDate,
            leadDays: LEAD_DAYS,
            rideCount: plan.rides.length,
            reason,
            isStructural: reason ? isStructuralPlanDayReason(reason) : false,
          });
        } catch (error) {
          // One park's failure is not the sweep's. It is left out of the row
          // set rather than recorded as an empty plan, because "we could not
          // ask" and "there was no answer" are the two things this whole
          // endpoint exists to keep apart.
          this.logger.warn(
            `Plan-day coverage: ${park.slug} failed: ${(error as Error).message}`,
          );
          return null;
        }
      });
      rows.push(...built.filter((row): row is PlanDayCoverage => row !== null));
    }

    if (rows.length > 0) {
      // Re-runnable: a second sweep on the same day replaces the first rather
      // than doubling the count.
      await this.coverageRepository.upsert(rows, ["measuredOn", "parkId"]);
    }

    const summary = PlanDayCoverageService.summarise(
      measuredOn,
      plannedDate,
      rows,
      parks.length,
    );
    this.logger.log(
      `Plan-day coverage ${measuredOn} (+${LEAD_DAYS}d): ` +
        `${summary.parksWithPlan}/${summary.parksOpen} parks planned, ` +
        `${summary.parksWithoutPlan} without a plan ` +
        `(${summary.parksStructural} structural, ` +
        `${summary.parksFailed} not measured)`,
    );
    return summary;
  }

  /**
   * Parks the operator has published an operating day for on that date.
   *
   * Park-wide rows only (`attractionId IS NULL`) — a per-ride schedule entry
   * says nothing about the park being open, and counting one would put parks
   * into the denominator that never claimed to be open.
   */
  private async openParks(plannedDate: string): Promise<Park[]> {
    const ids: Array<{ id: string }> = await this.parkRepository.manager.query(
      `SELECT DISTINCT se."parkId" AS id
         FROM schedule_entries se
        WHERE se.date = $1::date
          AND se."scheduleType" = 'OPERATING'
          AND se."attractionId" IS NULL`,
      [plannedDate],
    );
    if (ids.length === 0) return [];
    return this.parkRepository.findByIds(ids.map((row) => row.id));
  }

  private static summarise(
    measuredOn: string,
    plannedDate: string,
    rows: readonly PlanDayCoverage[],
    parksOpen: number,
  ): PlanDayCoverageSummary {
    const byReason: Record<string, number> = {};
    let parksWithPlan = 0;
    let parksWithoutPlan = 0;
    let parksStructural = 0;
    let measured = 0;
    for (const row of rows) {
      measured++;
      if (!row.reason) {
        parksWithPlan++;
        continue;
      }
      byReason[row.reason] = (byReason[row.reason] ?? 0) + 1;
      if (isUnknownPlanDayReason(row.reason)) measured--;
      else if (row.isStructural) parksStructural++;
      else parksWithoutPlan++;
    }
    return {
      measuredOn,
      plannedDate,
      parksOpen,
      parksWithPlan,
      parksWithoutPlan,
      parksStructural,
      parksFailed: parksOpen - measured,
      byReason,
    };
  }

  /** `YYYY-MM-DD` in UTC — the sweep's own clock, not any park's. */
  private static utcDate(at: Date): string {
    return at.toISOString().slice(0, 10);
  }
}
