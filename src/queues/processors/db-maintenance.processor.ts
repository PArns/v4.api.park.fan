import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import {
  DbVacuumService,
  VacuumTableResult,
} from "../../database/maintenance/db-vacuum.service";

/**
 * Database maintenance before the 06:00 UTC CatBoost training (PAR-868).
 *
 * `vacuum-churn-tables` runs at 04:50 UTC and VACUUMs the high-churn tables one
 * after another, so autovacuum has nothing large left to pick up while the
 * training subprocess needs the host's memory. The window, the table list and
 * the stop rules live in `database/maintenance/vacuum-plan.ts`.
 *
 * The job never fails: every table's error is caught and logged by the service,
 * and a retry would only arrive later, when the window is closed anyway.
 */
@Processor("db-maintenance")
export class DbMaintenanceProcessor {
  private readonly logger = new Logger(DbMaintenanceProcessor.name);

  constructor(private readonly vacuumService: DbVacuumService) {}

  @Process("vacuum-churn-tables")
  async handleVacuumChurnTables(_job: Job): Promise<VacuumTableResult[]> {
    try {
      return await this.vacuumService.vacuumChurnTables();
    } catch (error) {
      // The service catches per table; this is the belt for anything else.
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`🧽 VACUUM job aborted: ${message}`);
      return [];
    }
  }
}
