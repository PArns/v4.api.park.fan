import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { TripsService } from "../../trips/trips.service";

/**
 * The sweep that gives a stored plan an end.
 *
 * `Trip.expiresAt` and `TripsService.sweepExpired()` were written together and
 * nothing ever called the second one, which made the first a comment: a read
 * treats an expired row as absent, so the feature behaved correctly and the
 * table simply never gave anything back. Every abandoned plan — a visitor who
 * opened the planner once, a bot that got past the limiter — stayed forever,
 * and the only signal would have been the table's size a year later.
 *
 * Daily is far more often than needed for a 400-day expiry, and that is the
 * point: it costs one indexed DELETE against `idx_trips_expires_at`, and a
 * sweep that runs every day cannot accumulate a backlog worth thinking about.
 */
@Processor("trips")
export class TripsMaintenanceProcessor {
  private readonly logger = new Logger(TripsMaintenanceProcessor.name);

  constructor(private readonly tripsService: TripsService) {}

  @Process("sweep-expired")
  async handleSweepExpired(_job: Job): Promise<void> {
    try {
      const removed = await this.tripsService.sweepExpired();
      // Quiet on zero: the normal answer for a young table, and a daily log
      // line saying "nothing happened" is how a log stops being read.
      if (removed > 0) {
        this.logger.log(`🧹 Swept ${removed} expired trip(s)`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`Trip sweep failed: ${message}`);
      throw error;
    }
  }
}
