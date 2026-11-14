import {
  BadRequestException,
  InternalServerErrorException,
} from "@nestjs/common";
import { McpToolError } from "./mcp-tool";
import { routeBody, runAdminRoute, toolAnnotations } from "./admin-route";
import { adminContext } from "./tools/__fixtures__/admin-context";
import { CurationPatchDto } from "../admin/content/dto/curation.dto";

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

describe("routeBody", () => {
  it("refuses what the global pipe refuses", async () => {
    await expect(
      routeBody(CurationPatchDto, { fields: {}, extra: 1 }),
    ).rejects.toBeInstanceOf(McpToolError);
  });

  it("turns a valid body into the DTO", async () => {
    const body = await routeBody(CurationPatchDto, {
      fields: { curatedName: "TARON" },
      reason: "operator page",
    });
    expect(body).toBeInstanceOf(CurationPatchDto);
  });
});

describe("toolAnnotations", () => {
  const base = {
    name: "x",
    title: "X",
    description: "",
    input: {},
    handler: async () => null,
  };
  it("marks a read read-only and a delete destructive", () => {
    expect(toolAnnotations(base)).toMatchObject({
      readOnlyHint: true,
      destructiveHint: false,
    });
    expect(toolAnnotations({ ...base, writes: "delete" })).toMatchObject({
      readOnlyHint: false,
      destructiveHint: true,
    });
    expect(toolAnnotations({ ...base, writes: "add" })).toMatchObject({
      destructiveHint: false,
      idempotentHint: false,
    });
  });
});
