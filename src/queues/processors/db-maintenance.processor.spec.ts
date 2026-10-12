import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { DbMaintenanceProcessor } from "./db-maintenance.processor";
import { DbVacuumService } from "../../database/maintenance/db-vacuum.service";

describe("DbMaintenanceProcessor (PAR-868)", () => {
  afterEach(() => jest.restoreAllMocks());

  it("returns the per-table results", async () => {
    const results = [
      {
        table: "pcn_forecasts",
        outcome: "vacuumed" as const,
        durationMs: 1,
        deadBefore: 2,
        deadAfter: 0,
      },
    ];
    const service = {
      vacuumChurnTables: jest.fn().mockResolvedValue(results),
    } as unknown as DbVacuumService;

    await expect(
      new DbMaintenanceProcessor(service).handleVacuumChurnTables({} as Job),
    ).resolves.toBe(results);
  });

  it("never fails the job, so Bull never retries it into the training window", async () => {
    jest.spyOn(Logger.prototype, "error").mockImplementation(() => undefined);
    const service = {
      vacuumChurnTables: jest.fn().mockRejectedValue(new Error("boom")),
    } as unknown as DbVacuumService;

    await expect(
      new DbMaintenanceProcessor(service).handleVacuumChurnTables({} as Job),
    ).resolves.toEqual([]);
  });
});
