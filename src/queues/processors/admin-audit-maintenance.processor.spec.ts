import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { AdminAuditMaintenanceProcessor } from "./admin-audit-maintenance.processor";
import {
  AdminAuditRetentionService,
  type AuditAddressSweep,
} from "../../admin/auth/admin-audit-retention.service";

describe("AdminAuditMaintenanceProcessor", () => {
  const processor = (sweep: () => Promise<AuditAddressSweep>) =>
    new AdminAuditMaintenanceProcessor({
      sweepAddresses: jest.fn(sweep),
    } as unknown as AdminAuditRetentionService);

  afterEach(() => jest.restoreAllMocks());

  it("logs the counts of a run that changed rows, and nothing else", async () => {
    const log = jest
      .spyOn(Logger.prototype, "log")
      .mockImplementation(() => undefined);

    await processor(async () => ({
      expired: 4,
      truncated: 31,
      unreadable: 1,
    })).handleSweepAddresses({} as Job);

    expect(log).toHaveBeenCalledTimes(1);
    expect(log.mock.calls[0][0]).toBe(
      "🧹 Audit log: cleared 4 past retention, cut 31 to their network, cleared 1 unreadable",
    );
  });

  it("stays quiet when there was nothing to do", async () => {
    const log = jest
      .spyOn(Logger.prototype, "log")
      .mockImplementation(() => undefined);

    await processor(async () => ({
      expired: 0,
      truncated: 0,
      unreadable: 0,
    })).handleSweepAddresses({} as Job);

    expect(log).not.toHaveBeenCalled();
  });

  it("fails the job when the sweep fails, so Bull retries it and the monitor sees it", async () => {
    jest.spyOn(Logger.prototype, "error").mockImplementation(() => undefined);

    await expect(
      processor(async () => {
        throw new Error("connection lost");
      }).handleSweepAddresses({} as Job),
    ).rejects.toThrow("connection lost");
  });
});
