import { AdminController } from "../../admin/admin.controller";
import { AdminSearchConsoleController } from "../../search-console/search-console.controller";
import { McpToolError } from "../mcp-tool";
import { runJobTool, runMaintenanceTool } from "./jobs.tools";
import { adminContext, callAs } from "./__fixtures__/admin-context";

const queued = { message: "queued", jobId: "7" };

describe("run_job", () => {
  it("runs the admin route's trigger with its one parameter, audited as that route", async () => {
    const triggerRideStats = jest.fn(async () => queued);
    const triggerPcn = jest.fn(async () => queued);
    const { ctx, audit } = adminContext("editor", [
      [AdminController, { triggerRideStats, triggerPcn }],
    ]);
    expect(
      await runJobTool.handler({ job: "import-ride-stats", limit: 25 }, ctx),
    ).toEqual({ job: "import-ride-stats", ...queued });
    expect(triggerRideStats).toHaveBeenCalledWith({ limit: 25 });

    await runJobTool.handler({ job: "pcn-score" }, ctx);
    expect(triggerPcn).toHaveBeenCalledWith("score");

    expect(audit.map(({ action, after }) => [action, after])).toEqual([
      ["job.import-ride-stats", { limit: 25 }],
      ["job.pcn/score", null],
    ]);
  });

  it("reaches the Search Console import through its own controller", async () => {
    const importNow = jest.fn(async () => ({ queued: true }));
    const { ctx, audit } = adminContext("editor", [
      [AdminSearchConsoleController, { importNow }],
    ]);
    await runJobTool.handler({ job: "search-console-import" }, ctx);
    expect(importNow).toHaveBeenCalled();
    expect(audit[0].action).toBe("job.search-console/import");
  });

  it("refuses a parameter the job does not take, and an author", async () => {
    const { ctx, audit } = adminContext("editor", [[AdminController, {}]]);
    await expect(
      runJobTool.handler({ job: "sync-parks", days: 3 }, ctx),
    ).rejects.toEqual(new McpToolError("sync-parks takes no days"));
    expect(audit).toEqual([]);
    expect(
      (await callAs(runJobTool, "author", { job: "sync-parks" })).isError,
    ).toBe(true);
  });
});

describe("run_maintenance", () => {
  it("says what a flush would do and flushes only on dryRun false", async () => {
    const flushCache = jest.fn(async () => ({ keysDeleted: 12 }));
    const { ctx, audit } = adminContext("owner", [
      [AdminController, { flushCache }],
    ]);
    const preview = await runMaintenanceTool.handler(
      { op: "flush-cache", dryRun: true },
      ctx,
    );
    expect(preview).toMatchObject({ dryRun: true, op: "flush-cache" });
    expect(flushCache).not.toHaveBeenCalled();
    expect(audit).toEqual([]);

    await runMaintenanceTool.handler({ op: "flush-cache", dryRun: false }, ctx);
    expect(flushCache).toHaveBeenCalled();
    expect(audit[0].action).toBe("job.flush-cache");
  });

  it("runs the park check without repairs on a dry run", async () => {
    const validateAndRepairParks = jest.fn(async () => ({ validation: {} }));
    const { ctx } = adminContext("owner", [
      [AdminController, { validateAndRepairParks }],
    ]);
    await runMaintenanceTool.handler(
      { op: "validate-and-repair-parks", dryRun: true },
      ctx,
    );
    await runMaintenanceTool.handler(
      { op: "validate-and-repair-parks", dryRun: false },
      ctx,
    );
    expect(validateAndRepairParks.mock.calls).toEqual([
      [{ autoFix: false }],
      [{ autoFix: true }],
    ]);
  });

  it("wants both dates for a backfill and none elsewhere, and an owner", async () => {
    const triggerParkDayOperationBackfill = jest.fn(async () => queued);
    const { ctx } = adminContext("owner", [
      [AdminController, { triggerParkDayOperationBackfill }],
    ]);
    await expect(
      runMaintenanceTool.handler(
        {
          op: "backfill-park-day-operation",
          fromDate: "2026-01-01",
          dryRun: false,
        },
        ctx,
      ),
    ).rejects.toBeInstanceOf(McpToolError);
    await expect(
      runMaintenanceTool.handler(
        { op: "flush-cache", fromDate: "2026-01-01", dryRun: true },
        ctx,
      ),
    ).rejects.toBeInstanceOf(McpToolError);
    await runMaintenanceTool.handler(
      {
        op: "backfill-park-day-operation",
        fromDate: "2026-01-01",
        toDate: "2026-01-31",
        dryRun: false,
      },
      ctx,
    );
    expect(triggerParkDayOperationBackfill).toHaveBeenCalledWith({
      fromDate: "2026-01-01",
      toDate: "2026-01-31",
    });
    expect(
      (await callAs(runMaintenanceTool, "editor", { op: "flush-cache" }))
        .isError,
    ).toBe(true);
  });
});
