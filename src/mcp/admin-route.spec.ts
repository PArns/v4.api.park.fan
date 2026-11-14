import {
  BadRequestException,
  InternalServerErrorException,
} from "@nestjs/common";
import { McpToolError } from "./mcp-tool";
import { runAdminRoute } from "./admin-route";
import { adminContext } from "./tools/__fixtures__/admin-context";

describe("runAdminRoute", () => {
  it("records the interceptor's row after the handler succeeds", async () => {
    const { ctx, audit } = adminContext("editor", []);
    const value = await runAdminRoute(
      ctx,
      { method: "POST", path: "sync-parks", body: { token: "x", limit: 3 } },
      async () => ({ queued: true }),
    );
    expect(value).toEqual({ queued: true });
    expect(audit).toEqual([
      expect.objectContaining({
        actor: ctx.admin,
        action: "job.sync-parks",
        entityType: "system",
        entityLabel: "POST sync-parks",
        after: { token: "[redacted]", limit: 3 },
      }),
    ]);
  });

  it("leaves a self-audited route to write its own row", async () => {
    const { ctx, audit } = adminContext("editor", []);
    await runAdminRoute(
      ctx,
      { method: "PATCH", path: "content/parks/x", selfAudited: true },
      async () => null,
    );
    expect(audit).toEqual([]);
  });

  it("hands a 4xx to the model and records nothing", async () => {
    const { ctx, audit } = adminContext("editor", []);
    await expect(
      runAdminRoute(ctx, { method: "POST", path: "x" }, async () => {
        throw new BadRequestException(["fields must be an object", "no"]);
      }),
    ).rejects.toEqual(new McpToolError("fields must be an object; no"));
    expect(audit).toEqual([]);
  });

  it("keeps a 5xx away from the model", async () => {
    const { ctx } = adminContext("editor", []);
    const failure = runAdminRoute(ctx, { method: "POST", path: "x" }, () =>
      Promise.reject(new InternalServerErrorException("db at 10.0.0.5")),
    );
    await expect(failure).rejects.toBeInstanceOf(InternalServerErrorException);
  });
});
