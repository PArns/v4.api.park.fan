import { z } from "zod";
import { capped, defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import type { McpToolContext } from "../mcp-tool";
import { dryRunInput, routeBody } from "../mcp-write";
import { runAdminRoute } from "../admin-route";
import { AdminApiKeyRequestsController } from "../../api-keys/admin-api-key-requests.controller";
import type { ApiKeyRequestItem } from "../../api-keys/api-key-request.service";
import { API_KEY_USAGES } from "../../api-keys/api-key.constants";
import {
  ApproveApiKeyRequestDto,
  RejectApiKeyRequestDto,
} from "../../api-keys/dto/api-key-request.dto";
import { REQUEST_LIMITS } from "../../api-keys/api-key-request.policy";
import {
  API_KEY_REQUEST_STATUSES,
  type ApiKeyRequest,
} from "../../api-keys/entities/api-key-request.entity";
import {
  MAX_ALLOWED_PARKS,
  MAX_REQUESTS_PER_HOUR,
} from "../../api-keys/dto/create-api-key.dto";
import { keyView, PERMISSIONS } from "./api-keys.tools";
import {
  APPLICANT_DATA_DESCRIPTION,
  APPLICANT_DATA_NOTICE,
} from "./visitor-data";

/**
 * The key requests developers file on their dashboard, as the admin's API
 * keys page reviews them. Owner only like the page; every decision is audited
 * by the route's own handler (`api-key-request.*`). docs/admin/mcp.md.
 */

const requests = (ctx: McpToolContext) =>
  ctx.get(AdminApiKeyRequestsController);

/**
 * One request: the choices of the form as they are, and what the applicant
 * typed under `visitorData`, since anyone can register and fill it in.
 */
function requestView(item: ApiKeyRequest & Partial<ApiKeyRequestItem>) {
  return {
    id: item.id,
    status: item.status,
    tier: item.tier,
    kind: item.kind,
    platforms: item.platforms,
    source: item.source,
    revenue: item.revenue,
    permissions: item.permissions,
    volume: item.volume,
    serverSide: item.serverSide,
    locale: item.locale,
    createdAt: item.createdAt,
    decidedAt: item.decidedAt,
    decidedBy: item.decidedBy,
    decisionNote: item.decisionNote,
    keyId: item.keyId,
    visitorData: {
      applicant: item.applicant ?? null,
      projectName: item.projectName,
      website: item.website,
      languages: item.languages,
      description: item.description,
      repository: item.repository,
      license: item.license,
      endpoints: item.endpoints,
      parks: item.parks,
      caching: item.caching,
      attributionUrl: item.attributionUrl,
      message: item.message,
    },
  };
}

/** One request answered with the notice its applicant text needs. */
function oneRequest(item: ApiKeyRequest & Partial<ApiKeyRequestItem>) {
  return {
    visitorDataNotice: APPLICANT_DATA_NOTICE,
    request: requestView(item),
  };
}

/** The request as it stands, for a dry run and a refusal the model can read. */
async function findRequest(
  ctx: McpToolContext,
  id: string,
): Promise<ApiKeyRequestItem> {
  const { items } = await requests(ctx).list();
  const item = items.find((candidate) => candidate.id === id);
  if (!item) {
    throw new McpToolError(`No key request ${id}; api_key_requests lists them`);
  }
  return item;
}

export const apiKeyRequestsTool = defineMcpTool({
  name: "api_key_requests",
  title: "API key requests",
  description:
    "The key requests developers file on park.fan's developer dashboard, undecided first, " +
    "newest first within each, as the admin's API keys page lists them: status, tier (free or " +
    "paid, from the answers: free only for open source that earns nothing but donations), kind, " +
    "platforms, source, revenue, the permissions asked for, expected volume per day, whether the " +
    "key stays on a server, the decision with its note, and keyId once approved (api_keys has " +
    "the key). What the applicant typed (project, description, endpoints, parks, caching, " +
    "backlink page, message, the account's address and name) is under each request's " +
    `visitorData. ${APPLICANT_DATA_DESCRIPTION} Owner only.`,
  minRole: "owner",
  input: {
    status: z
      .enum([...API_KEY_REQUEST_STATUSES, "all"])
      .default("all")
      .describe('Which requests; "pending" for the undecided ones'),
    id: uuidInput().optional().describe("Only this request"),
    limit: z
      .number()
      .int()
      .min(1)
      .max(100)
      .default(20)
      .describe("Most requests"),
  },
  async handler({ status, id, limit }, ctx) {
    const { items } = await requests(ctx).list();
    const wanted = items.filter(
      (item) =>
        (status === "all" || item.status === status) &&
        (!id || item.id === id.toLowerCase()),
    );
    return {
      visitorDataNotice: APPLICANT_DATA_NOTICE,
      ...capped(wanted.map(requestView), limit),
    };
  },
});

export const approveApiKeyRequestTool = defineMcpTool({
  name: "approve_api_key_request",
  title: "Approve a key request",
  description:
    "Approves an undecided request and issues its key, as the admin's approval does: bound to " +
    "the applicant's account, with the permissions, usage, allowance, parks and end date given " +
    "here (the request's own permissions only suggest them). Nobody sees the key's first secret: " +
    "the developer picks it up on the dashboard, so the answer carries none. The decision cannot " +
    "be taken back; revoke_api_key stops the key. Commercial use only with a written agreement; " +
    "a paid tier means the terms ask for one. The dry run (default) shows the request and the key " +
    "it would issue. Owner only.",
  minRole: "owner",
  writes: "add",
  input: {
    id: uuidInput().describe("Request id from api_key_requests"),
    permissions: PERMISSIONS.describe(
      "What the key may read (live.waits needs no key)",
    ),
    name: z
      .string()
      .trim()
      .min(1)
      .max(80)
      .optional()
      .describe("The key's name; the project's name when absent"),
    usage: z
      .enum(API_KEY_USAGES)
      .default("non-commercial")
      .describe("commercial only with a written agreement"),
    requestsPerHour: z
      .number()
      .int()
      .min(1)
      .max(MAX_REQUESTS_PER_HOUR)
      .nullable()
      .optional()
      .describe("Requests per clock hour; null for no per-key limit"),
    allowedParkIds: z
      .array(uuidInput())
      .min(1)
      .max(MAX_ALLOWED_PARKS)
      .nullable()
      .optional()
      .describe("The parks the key may read; null or absent for every park"),
    expiresAt: z.iso
      .datetime({ offset: true })
      .nullable()
      .optional()
      .describe("When it stops working, ISO 8601; null or absent for never"),
    note: z.string().max(500).optional().describe("A note for the admin"),
    message: z
      .string()
      .max(REQUEST_LIMITS.reason)
      .optional()
      .describe("What the developer reads beside the approval"),
    dryRun: dryRunInput("the request and the key it would issue"),
  },
  async handler({ id, dryRun, ...approval }, ctx) {
    const requestId = id.toLowerCase();
    const body = await routeBody(ApproveApiKeyRequestDto, {
      ...approval,
      allowedParkIds:
        approval.allowedParkIds?.map((park) => park.toLowerCase()) ??
        approval.allowedParkIds,
    });
    const request = await findRequest(ctx, requestId);
    if (request.status !== "pending") {
      throw new McpToolError(
        `The request is ${request.status}; only an undecided request can be approved`,
      );
    }
    if (dryRun) {
      return {
        dryRun: true,
        ...oneRequest(request),
        wouldIssue: { ...body, name: body.name ?? request.projectName },
      };
    }
    const { item, key } = await runAdminRoute(
      ctx,
      {
        method: "POST",
        path: `api-key-requests/${requestId}/approve`,
        selfAudited: true,
      },
      () => requests(ctx).approve(ctx.admin, requestId, body),
    );
    return { dryRun: false, ...oneRequest(item), key: keyView(key) };
  },
});

export const rejectApiKeyRequestTool = defineMcpTool({
  name: "reject_api_key_request",
  title: "Turn a key request down",
  description:
    "Turns an undecided request down, as the admin does; the reason is what the developer reads " +
    "on the dashboard, so write it to them, in their language. reopen_api_key_request puts it " +
    "back into the queue. Owner only.",
  minRole: "owner",
  writes: "change",
  input: {
    id: uuidInput().describe("Request id from api_key_requests"),
    reason: z
      .string()
      .max(REQUEST_LIMITS.reason)
      .optional()
      .describe("Why, for the developer"),
  },
  async handler({ id, reason }, ctx) {
    const requestId = id.toLowerCase();
    const body = await routeBody(RejectApiKeyRequestDto, { reason });
    const { item } = await runAdminRoute(
      ctx,
      {
        method: "POST",
        path: `api-key-requests/${requestId}/reject`,
        selfAudited: true,
      },
      () => requests(ctx).reject(ctx.admin, requestId, body),
    );
    return oneRequest(item);
  },
});

export const reopenApiKeyRequestTool = defineMcpTool({
  name: "reopen_api_key_request",
  title: "Reopen a key request",
  description:
    "Puts a rejected request back into the queue, as the admin does, for a decision taken by " +
    "mistake; an approved one stays approved (revoke_api_key stops its key). Owner only.",
  minRole: "owner",
  writes: "change",
  input: {
    id: uuidInput().describe("Request id from api_key_requests, rejected"),
  },
  async handler({ id }, ctx) {
    const requestId = id.toLowerCase();
    const { item } = await runAdminRoute(
      ctx,
      {
        method: "POST",
        path: `api-key-requests/${requestId}/reopen`,
        selfAudited: true,
      },
      () => requests(ctx).reopen(ctx.admin, requestId),
    );
    return oneRequest(item);
  },
});

export const deleteApiKeyRequestTool = defineMcpTool({
  name: "delete_api_key_request",
  title: "Delete a key request",
  description:
    "Deletes a request for good, as the admin's delete button does; a key issued for it stays " +
    "(revoke_api_key stops it), and the audit log keeps the decision. The dry run (default) shows " +
    "the request. Owner only.",
  minRole: "owner",
  writes: "delete",
  input: {
    id: uuidInput().describe("Request id from api_key_requests"),
    dryRun: dryRunInput("the request that would be deleted"),
  },
  async handler({ id, dryRun }, ctx) {
    const requestId = id.toLowerCase();
    const request = await findRequest(ctx, requestId);
    if (dryRun) return { dryRun: true, ...oneRequest(request) };
    await runAdminRoute(
      ctx,
      {
        method: "DELETE",
        path: `api-key-requests/${requestId}`,
        selfAudited: true,
      },
      () => requests(ctx).remove(ctx.admin, requestId),
    );
    return { dryRun: false, deleted: requestId, keyId: request.keyId };
  },
});
