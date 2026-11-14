import { z } from "zod";
import { capped, defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import type { McpToolContext } from "../mcp-tool";
import { dryRunInput, routeBody } from "../mcp-write";
import { runAdminRoute } from "../admin-route";
import { AdminApiKeysController } from "../../api-keys/admin-api-keys.controller";
import type { ApiKeyItem } from "../../api-keys/api-key.service";
import {
  API_KEY_USAGES,
  DEFAULT_REQUESTS_PER_HOUR,
  MAX_GRACE_HOURS,
} from "../../api-keys/api-key.constants";
import {
  CreateApiKeyDto,
  MAX_ALLOWED_PARKS,
  MAX_REQUESTS_PER_HOUR,
  RotateApiKeyDto,
  UpdateApiKeyDto,
} from "../../api-keys/dto/create-api-key.dto";

/**
 * The admin's API keys page: the data keys that open the forecast routes.
 * Owner only like the page, every write audited by the route's own handler
 * (`api-key.*`). docs/frontend/api-keys.md.
 */

const keys = (ctx: McpToolContext) => ctx.get(AdminApiKeysController);

/** One key as the list shows it, its daily counts summed unless asked for. */
function keyView(item: ApiKeyItem, withDays = false) {
  const days = item.stats?.days ?? [];
  return {
    id: item.id,
    name: item.name,
    usage: item.usage,
    prefix: item.prefix,
    contact: item.contact,
    note: item.note,
    requestsPerHour: item.requestsPerHour,
    allowedParks: item.allowedParks ?? item.allowedParkIds,
    expiresAt: item.expiresAt,
    createdAt: item.createdAt,
    lastUsedAt: item.lastUsedAt,
    revokedAt: item.revokedAt,
    rotatedAt: item.rotatedAt,
    previousExpiresAt: item.previousExpiresAt,
    previousLastUsedAt: item.previousLastUsedAt,
    ...(item.requestsThisHour !== undefined && {
      requestsThisHour: item.requestsThisHour,
    }),
    ...(item.stats && {
      requests30d: days.reduce((sum, day) => sum + day.requests, 0),
      limited30d: days.reduce((sum, day) => sum + day.limited, 0),
      months: item.stats.months,
      ...(withDays && { days }),
    }),
  };
}

/** The key as it stands, for a dry run and a refusal the model can read. */
async function findKey(ctx: McpToolContext, id: string): Promise<ApiKeyItem> {
  const { items } = await keys(ctx).list();
  const item = items.find((candidate) => candidate.id === id);
  if (!item) throw new McpToolError(`No API key ${id}; api_keys lists them`);
  return item;
}

const SECRET_NOTICE =
  "The key is in this answer and nowhere else; only its hash is stored. Hand it to its holder " +
  "and do not repeat it.";

export const apiKeysTool = defineMcpTool({
  name: "api_keys",
  title: "Data API keys",
  description:
    "The data API keys that open park.fan's forecast routes, active first, as the admin's API " +
    "keys page lists them: name, usage (commercial or not), prefix, contact, hourly allowance, " +
    "parks, end date, last use, requests this hour, in the last 30 days and per month, and the " +
    "rotation and its grace period. Never a secret. Owner only.",
  minRole: "owner",
  input: {
    status: z
      .enum(["active", "revoked", "all"])
      .default("active")
      .describe("Which keys"),
    withDays: z
      .boolean()
      .default(false)
      .describe("Each key's requests per UTC day for 30 days"),
    limit: z.number().int().min(1).max(200).default(50).describe("Most keys"),
  },
  async handler({ status, withDays, limit }, ctx) {
    const { items } = await keys(ctx).list();
    const wanted = items.filter(
      (item) =>
        status === "all" ||
        (status === "revoked") === (item.revokedAt !== null),
    );
    return capped(
      wanted.map((item) => keyView(item, withDays)),
      limit,
    );
  },
});

const REQUESTS_PER_HOUR = z
  .number()
  .int()
  .min(1)
  .max(MAX_REQUESTS_PER_HOUR)
  .nullable()
  .optional();
const ALLOWED_PARK_IDS = z
  .array(uuidInput())
  .min(1)
  .max(MAX_ALLOWED_PARKS)
  .nullable()
  .optional();
const EXPIRES_AT = z.iso.datetime({ offset: true }).nullable().optional();

export const createApiKeyTool = defineMcpTool({
  name: "create_api_key",
  title: "Issue an API key",
  description:
    "Issues a data API key, as the admin's API keys page does. Commercial use only for a holder " +
    `with a written agreement. ${SECRET_NOTICE} Owner only.`,
  minRole: "owner",
  writes: "add",
  input: {
    name: z
      .string()
      .trim()
      .min(1)
      .max(80)
      .describe("Who or what the key is for"),
    contact: z
      .string()
      .max(200)
      .optional()
      .describe("An email or URL to reach the holder"),
    usage: z
      .enum(API_KEY_USAGES)
      .default("non-commercial")
      .describe("commercial only with a written agreement"),
    note: z.string().max(500).optional().describe("A note for the admin"),
    requestsPerHour: REQUESTS_PER_HOUR.describe(
      `Requests per clock hour; null for no per-key limit; absent ${DEFAULT_REQUESTS_PER_HOUR}`,
    ),
    allowedParkIds: ALLOWED_PARK_IDS.describe(
      "The parks the key may read; null or absent for every park",
    ),
    expiresAt: EXPIRES_AT.describe(
      "When it stops working, ISO 8601 in the future; null or absent for never",
    ),
  },
  async handler(input, ctx) {
    const body = await routeBody(CreateApiKeyDto, {
      ...input,
      allowedParkIds: input.allowedParkIds?.map((id) => id.toLowerCase()),
    });
    const created = await runAdminRoute(
      ctx,
      { method: "POST", path: "api-keys", selfAudited: true },
      () => keys(ctx).create(ctx.admin, body),
    );
    return {
      key: created.key,
      notice: SECRET_NOTICE,
      item: keyView(created.item),
    };
  },
});

export const updateApiKeyTool = defineMcpTool({
  name: "update_api_key",
  title: "Change an API key",
  description:
    "Changes an active key's hourly allowance, parks or end date, as the admin does; a field " +
    "left out stays, null clears it (no per-key limit, every park, never). A new end date " +
    "brings an expired key back. Applies from the next request. Owner only.",
  minRole: "owner",
  writes: "change",
  input: {
    id: uuidInput().describe("Key id from api_keys"),
    requestsPerHour: REQUESTS_PER_HOUR.describe(
      "Requests per clock hour; null for no per-key limit",
    ),
    allowedParkIds: ALLOWED_PARK_IDS.describe(
      "The parks the key may read; null for every park",
    ),
    expiresAt: EXPIRES_AT.describe("When it stops working; null for never"),
  },
  async handler({ id, ...change }, ctx) {
    if (Object.values(change).every((value) => value === undefined)) {
      throw new McpToolError(
        "Name requestsPerHour, allowedParkIds or expiresAt",
      );
    }
    const keyId = id.toLowerCase();
    const body = await routeBody(UpdateApiKeyDto, {
      ...change,
      allowedParkIds:
        change.allowedParkIds?.map((park) => park.toLowerCase()) ??
        change.allowedParkIds,
    });
    const { item } = await runAdminRoute(
      ctx,
      { method: "PATCH", path: `api-keys/${keyId}`, selfAudited: true },
      () => keys(ctx).update(ctx.admin, keyId, body),
    );
    return { item: keyView(item) };
  },
});

export const rotateApiKeyTool = defineMcpTool({
  name: "rotate_api_key",
  title: "Rotate an API key",
  description:
    "Gives a key a new secret, as the admin does; name, allowance, parks, end date and " +
    "statistics stay. The old secret keeps working graceHours beside the new one (0 refuses it " +
    "at once); a rotation during a grace period ends that grace at once. " +
    `${SECRET_NOTICE} Owner only.`,
  minRole: "owner",
  writes: "change",
  input: {
    id: uuidInput().describe("Key id from api_keys"),
    graceHours: z
      .number()
      .int()
      .min(0)
      .max(MAX_GRACE_HOURS)
      .describe("Hours the old secret keeps working, so its holder can switch"),
  },
  async handler({ id, graceHours }, ctx) {
    const keyId = id.toLowerCase();
    const body = await routeBody(RotateApiKeyDto, { graceHours });
    const rotated = await runAdminRoute(
      ctx,
      { method: "POST", path: `api-keys/${keyId}/rotate`, selfAudited: true },
      () => keys(ctx).rotate(ctx.admin, keyId, body),
    );
    return {
      key: rotated.key,
      notice: SECRET_NOTICE,
      item: keyView(rotated.item),
    };
  },
});

export const revokeApiKeyTool = defineMcpTool({
  name: "revoke_api_key",
  title: "Revoke an API key",
  description:
    "Revokes a key, as the admin does: its holder is refused from the next request on, and the " +
    "key stays in the list until delete_api_key. With secret previous it only ends a " +
    "rotation's grace period, so the replaced secret stops and the current one works on. The " +
    "dry run (default) shows the key, its last use and its traffic. Owner only.",
  minRole: "owner",
  writes: "delete",
  input: {
    id: uuidInput().describe("Key id from api_keys"),
    secret: z
      .enum(["key", "previous"])
      .default("key")
      .describe(
        "key revokes the key; previous ends the grace of the replaced secret",
      ),
    dryRun: dryRunInput("the key as it stands"),
  },
  async handler({ id, secret, dryRun }, ctx) {
    const keyId = id.toLowerCase();
    const item = await findKey(ctx, keyId);
    if (dryRun) return { dryRun: true, secret, item: keyView(item) };
    if (secret === "previous") {
      await runAdminRoute(
        ctx,
        {
          method: "DELETE",
          path: `api-keys/${keyId}/previous`,
          selfAudited: true,
        },
        () => keys(ctx).revokePrevious(ctx.admin, keyId),
      );
    } else {
      await runAdminRoute(
        ctx,
        { method: "DELETE", path: `api-keys/${keyId}`, selfAudited: true },
        () => keys(ctx).revoke(ctx.admin, keyId),
      );
    }
    return { dryRun: false, secret, revoked: keyId, name: item.name };
  },
});

export const deleteApiKeyTool = defineMcpTool({
  name: "delete_api_key",
  title: "Delete a revoked API key",
  description:
    "Deletes a revoked key for good, as the admin's delete button does: it leaves the list and " +
    "its request statistics go with it; the audit log keeps its history. Refused for a key not " +
    "yet revoked (revoke_api_key first). The dry run (default) shows the key. Owner only.",
  minRole: "owner",
  writes: "delete",
  input: {
    id: uuidInput().describe("Key id from api_keys with status revoked"),
    dryRun: dryRunInput("the key that would be deleted"),
  },
  async handler({ id, dryRun }, ctx) {
    const keyId = id.toLowerCase();
    const item = await findKey(ctx, keyId);
    if (!item.revokedAt) {
      throw new McpToolError(
        `${item.name} is not revoked; revoke_api_key first`,
      );
    }
    if (dryRun) return { dryRun: true, item: keyView(item) };
    await runAdminRoute(
      ctx,
      { method: "POST", path: `api-keys/${keyId}/delete`, selfAudited: true },
      () => keys(ctx).remove(ctx.admin, keyId),
    );
    return { dryRun: false, deleted: keyId, name: item.name };
  },
});
