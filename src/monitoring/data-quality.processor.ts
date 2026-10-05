import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import {
  DataQualityMonitorService,
  SILENT_PARK_LOOKAHEAD_DAYS,
} from "./data-quality-monitor.service";

/** Bull hands a processor whatever was thrown; only the message is useful here. */
const asMessage = (e: unknown): string =>
  e instanceof Error ? e.message : String(e);

/**
 * Daily sweep for the silent-failure modes described in
 * DataQualityMonitorService.
 *
 * Logs a WARN naming what it found and a single quiet line when clean, the same
 * shape as the ride-profile term audit — a job that is red every night is a job
 * people learn to ignore.
 */
@Processor("analytics")
export class DataQualityProcessor {
  private readonly logger = new Logger(DataQualityProcessor.name);

  constructor(private readonly monitor: DataQualityMonitorService) {}

  @Process("monitor-data-quality")
  async handleMonitorDataQuality(_job: Job): Promise<void> {
    // Each detector catches its own throw. They are independent findings, and a
    // Promise.all that rejects takes the other two down with it — which is the
    // failure mode the module was written for: detect-seasonal threw on every
    // run for 73 days and nothing said so.
    //
    // `ran` is why the catch is not the whole fix. Three caught throws leave
    // three empty lists, and an empty list is what "nothing is wrong" looks
    // like: the clean line below would have printed ✅ directly under three
    // ERRORs. It is the same false green one level up.
    let ran = 0;
    const guard = <T>(name: string, p: Promise<T[]>): Promise<T[]> =>
      p
        .then((rows) => {
          ran++;
          return rows;
        })
        .catch((e) => {
          this.logger.error(`${name} check failed: ${asMessage(e)}`);
          return [] as T[];
        });

    const [clusters, silentParks, failing, unreviewed] = await Promise.all([
      guard("Silenced-cluster", this.monitor.findSilencedClusters()),
      guard("Scheduled-but-silent", this.monitor.findScheduledButSilentParks()),
      guard("Failing-job", this.monitor.findFailingJobs()),
      guard(
        "Absence-retired-unreviewed",
        this.monitor.findAbsenceRetiredUnreviewed(),
      ),
    ]);

    // Its own guard and outside the clean/inconclusive count: a candidate pair
    // is a question, not a fault, and most of the list is legitimately two
    // attractions in one building. One line, so the count is visible nightly.
    let reissue: Awaited<
      ReturnType<DataQualityMonitorService["findReissueCandidates"]>
    > = [];
    try {
      reissue = await this.monitor.findReissueCandidates();
    } catch (e) {
      this.logger.error(`Re-issue-candidate check failed: ${asMessage(e)}`);
    }
    const likely = reissue.filter((c) => c.namesMatch).length;
    if (likely > 0) {
      this.logger.warn(
        `🔁 ${likely} retired ride(s) have a live ride with a similar name within 30 m — ` +
          `likely re-issued under a new name (${reissue.length} nearby pairs in all). ` +
          `Merge or dismiss under /admin/duplicates.`,
      );
    }

    for (const c of clusters) {
      this.logger.warn(
        `🔇 ${c.parkName}: ${c.attractionCount} attractions stopped reporting on ${c.lastOperating} ` +
          `(e.g. ${c.sampleNames.slice(0, 3).join(", ")}). Dropped feed, or a section closed for the season?`,
      );
    }

    for (const p of silentParks) {
      const seen = p.lastReading
        ? `no reading since ${p.lastReading}`
        : "no reading in 400 days";
      this.logger.warn(
        `📵 ${p.parkName}: ${p.attractionCount} attractions, ${seen}, yet ` +
          `${p.operatingDaysAhead} operating day(s) scheduled in the next ` +
          `${SILENT_PARK_LOOKAHEAD_DAYS} days ` +
          `and a calendar running to ${p.lastScheduledDay}. ` +
          `Feed dropped, or a schedule nobody can confirm?`,
      );
    }

    for (const f of failing) {
      this.logger.warn(
        `💥 ${f.queue}/${f.jobName}: ${f.failures} failed run(s), last ${f.lastFailedAt ?? "unknown"} — ${f.lastReason}`,
      );
    }

    // One line per park rather than per ride: Wet'n'Wild alone is 22 rows of
    // restrooms and lockers, and a nightly wall of them is how a list stops
    // being read. The admin page shows every row.
    const byPark = new Map<string, string[]>();
    for (const r of unreviewed) {
      byPark.set(r.parkName, [...(byPark.get(r.parkName) ?? []), r.name]);
    }
    for (const [parkName, names] of byPark) {
      this.logger.warn(
        `🪦 ${parkName}: ${names.length} ride(s) retired as absent upstream with no season decided ` +
          `(e.g. ${names.slice(0, 3).join(", ")}). Out of season, or gone? Answer under /admin/data-quality.`,
      );
    }

    const nothingFound =
      clusters.length === 0 &&
      silentParks.length === 0 &&
      failing.length === 0 &&
      unreviewed.length === 0;

    if (nothingFound && ran === 4) {
      this.logger.log(
        "✅ Data quality clean: no silenced clusters, no silent scheduled parks, no failing jobs, no unreviewed absence retirements",
      );
    } else if (nothingFound) {
      this.logger.warn(
        `⚠️ Data quality inconclusive: ${ran} of 4 checks ran, the rest threw`,
      );
    }
  }
}
