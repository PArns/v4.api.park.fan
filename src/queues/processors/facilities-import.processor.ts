import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import type { Job } from "bull";
import {
  FacilitiesService,
  type FacilityRunResult,
  type OsmRef,
} from "../../facilities/facilities.service";

/** Queue and job name, shared with the admin controller that adds the jobs. */
export const FACILITIES_QUEUE = "facilities";
export const FACILITIES_IMPORT_JOB = "import";

/** One park (`parkId`, optionally its outline), or every park. */
export interface FacilityImportJobData {
  parkId?: string;
  boundary?: OsmRef;
  /** Bulk only: also the parks whose last import went through. */
  all?: boolean;
}

/** A bulk run's place and tally, kept as the job's progress. */
export interface FacilityBulkProgress {
  /** The last park finished; a run resumed after a deploy starts behind it. */
  after: string;
  total: number;
  done: number;
  ok: number;
  noOutline: number;
  failed: number;
  points: number;
}

/**
 * The pause after each park of a bulk run. Overpass frees a slot only after a
 * cool-down that "grows with the load of the server and proportionate to the
 * execution time"; ten seconds keeps a full run to a few requests a minute.
 */
export const PAUSE_BETWEEN_PARKS_MS = 10_000;

/** Parks in a row Overpass did not answer for before a bulk run gives up. */
export const MAX_OVERPASS_FAILURES_IN_A_ROW = 3;

function resumePoint(job: Pick<Job, "progress">): FacilityBulkProgress | null {
  const progress: unknown = job.progress();
  return progress !== null &&
    typeof progress === "object" &&
    typeof (progress as Partial<FacilityBulkProgress>).after === "string"
    ? (progress as FacilityBulkProgress)
    : null;
}

/**
 * Imports park facilities from OpenStreetMap when an admin asks: one park or
 * all of them. Never scheduled, never run at boot.
 *
 * One handler for both kinds of job, on purpose: Bull runs one job per
 * handler at a time, so a single park asked for during a bulk run waits its
 * turn and Overpass never sees two of our queries at once.
 */
@Processor(FACILITIES_QUEUE)
export class FacilitiesImportProcessor {
  private readonly logger = new Logger(FacilitiesImportProcessor.name);

  constructor(private readonly facilities: FacilitiesService) {}

  /** Overridden in specs, which do not wait. */
  protected pause(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  @Process(FACILITIES_IMPORT_JOB)
  async handle(
    job: Job<FacilityImportJobData>,
  ): Promise<FacilityRunResult | FacilityBulkProgress> {
    if (job.data.parkId) {
      return this.facilities.runImport(job.data.parkId, job.data.boundary);
    }

    const ids = await this.facilities.parkIdsForBulkRun(job.data.all === true);
    const resumed = resumePoint(job);
    const pending = resumed ? ids.filter((id) => id > resumed.after) : ids;
    const tally: FacilityBulkProgress = resumed ?? {
      after: "",
      total: ids.length,
      done: 0,
      ok: 0,
      noOutline: 0,
      failed: 0,
      points: 0,
    };
    let overpassDown = 0;
    for (const parkId of pending) {
      const result = await this.facilities.runImport(parkId);
      tally.after = parkId;
      tally.done += 1;
      tally.points += result.points;
      if (result.outcome === "ok") tally.ok += 1;
      else if (result.outcome === "no_outline") tally.noOutline += 1;
      else tally.failed += 1;
      await job.progress(tally);

      overpassDown = result.overpassDown ? overpassDown + 1 : 0;
      if (overpassDown >= MAX_OVERPASS_FAILURES_IN_A_ROW) {
        // Each of these already waited through the client's backoff. The run
        // stops here; started again, it takes only what is not done.
        throw new Error(
          `Overpass did not answer for ${overpassDown} parks in a row; ` +
            `stopped after ${tally.done} of ${tally.total}`,
        );
      }
      await this.pause(PAUSE_BETWEEN_PARKS_MS);
    }
    this.logger.log(`Facility import done: ${JSON.stringify(tally)}`);
    return tally;
  }
}
