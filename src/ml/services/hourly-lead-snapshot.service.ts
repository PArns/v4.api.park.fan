import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { HourlyLeadSnapshot } from "../entities/hourly-lead-snapshot.entity";
import { WaitTimePrediction } from "../entities/wait-time-prediction.entity";
import { bandLeadBucket } from "../utils/catboost-band-calibration.util";
import type { ServedIntradayValue } from "./prediction-accuracy.service";

/** Minutes. A slot is sampled when its lead lies in `(edge − WINDOW, edge]`. */
const SAMPLE_WINDOW_MINUTES = 60;

/** Rows per INSERT; ten columns each, far below Postgres' 65 535 parameters. */
const INSERT_CHUNK = 1000;

/**
 * The hourly lead-time error curve, built forward (PAR-658). See
 * {@link HourlyLeadSnapshot} for why `prediction_accuracy` cannot answer it.
 *
 * Called from `PredictionAccuracyService.recordPredictions` with the same
 * rows and served values the accuracy upsert gets, so every snapshot has its
 * `prediction_accuracy` partner for the actual.
 */
@Injectable()
export class HourlyLeadSnapshotService {
  /**
   * Upper edge of each sampling window, in minutes ahead: one per lead
   * bucket of `bandLeadBucket`. Only the slot on the full hour is sampled
   * (one of the four 15-minute slots, which share their run and differ
   * little), and the four runs inside its one-hour window see it; the first
   * insert wins, so the row records the lead at the edge, and a skipped run
   * is covered by the next one. `h24+` samples at 36 h rather than at the
   * 48 h horizon, whose last slot is not always predicted.
   */
  static readonly SAMPLE_LEADS = [120, 360, 1440, 2160] as const;

  constructor(
    @InjectRepository(HourlyLeadSnapshot)
    private readonly snapshotRepository: Repository<HourlyLeadSnapshot>,
  ) {}

  /**
   * The rows of this run that fall into a sampling window. Pure, so the
   * window arithmetic is testable without a database. `served` is
   * index-aligned with `predictions`.
   */
  static pick(
    predictions: WaitTimePrediction[],
    served: Array<ServedIntradayValue | null> | undefined,
    now: Date,
  ): HourlyLeadSnapshot[] {
    const rows: HourlyLeadSnapshot[] = [];
    predictions.forEach((p, i) => {
      if (p.predictionType !== "hourly") return;
      if (p.predictedTime.getUTCMinutes() !== 0) return;
      const madeAt = p.createdAt ?? now;
      const lead = Math.floor(
        (p.predictedTime.getTime() - madeAt.getTime()) / 60_000,
      );
      const inWindow = HourlyLeadSnapshotService.SAMPLE_LEADS.some(
        (edge) => lead <= edge && lead > edge - SAMPLE_WINDOW_MINUTES,
      );
      if (!inWindow) return;

      const row = new HourlyLeadSnapshot();
      row.attractionId = p.attractionId;
      row.targetTime = p.predictedTime;
      row.leadBucket = bandLeadBucket(lead);
      row.predictionTime = madeAt;
      row.leadMinutes = lead;
      row.predictedWaitTime = p.predictedWaitTime;
      row.uncertaintyMinutes = p.uncertaintyMinutes ?? null;
      row.modelVersion = p.modelVersion;
      row.servedWaitTime = served?.[i]?.wait ?? null;
      row.servedSource = served?.[i]?.source ?? null;
      rows.push(row);
    });
    return rows;
  }

  /** Write this run's sampled rows. Returns how many were offered. */
  async record(
    predictions: WaitTimePrediction[],
    served: Array<ServedIntradayValue | null> | undefined,
    now: Date = new Date(),
  ): Promise<number> {
    const rows = HourlyLeadSnapshotService.pick(predictions, served, now);
    for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
      // orIgnore, not upsert: the first answer inside a window is the one at
      // that lead; a later run in the same window must not overwrite it.
      await this.snapshotRepository
        .createQueryBuilder()
        .insert()
        .into(HourlyLeadSnapshot)
        .values(rows.slice(i, i + INSERT_CHUNK))
        .orIgnore()
        .execute();
    }
    return rows.length;
  }
}
