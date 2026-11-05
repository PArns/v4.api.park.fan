import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import {
  AdminAuditRetentionService,
  type AuditAddressSweep,
} from "../../admin/auth/admin-audit-retention.service";

/**
 * The nightly sweep that keeps the audit log's address to its retention
 * (docs/admin/authentication.md#address-retention): rows past the retention
 * lose it, and any full address still stored is cut to its network.
 *
 * It is the only thing that ever removes a stored address, so a failure is
 * rethrown: Bull retries it, and the failing-jobs sweep reports what is still
 * failing the next morning.
 */
@Processor("admin-audit")
export class AdminAuditMaintenanceProcessor {
  private readonly logger = new Logger(AdminAuditMaintenanceProcessor.name);

  constructor(private readonly retention: AdminAuditRetentionService) {}

  @Process("sweep-addresses")
  async handleSweepAddresses(_job: Job): Promise<AuditAddressSweep> {
    try {
      const sweep = await this.retention.sweepAddresses();
      // Quiet when there was nothing to do, which is every night once the
      // first run has gone through; that first run is the one whose counts
      // are worth reading (docs/rules/automated-retirement-and-identity.md).
      if (sweep.expired + sweep.truncated + sweep.unreadable > 0) {
        this.logger.log(
          `🧹 Audit log: cleared ${sweep.expired} past retention, cut ${sweep.truncated} to their network, cleared ${sweep.unreadable} unreadable`,
        );
      }
      return sweep;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`Audit log address sweep failed: ${message}`);
      throw error;
    }
  }
}
