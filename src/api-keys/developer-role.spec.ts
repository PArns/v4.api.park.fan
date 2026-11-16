import "reflect-metadata";
import { readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import {
  GUARDS_METADATA,
  METHOD_METADATA,
  PATH_METADATA,
} from "@nestjs/common/constants";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import {
  ADMIN_MIN_ROLE_KEY,
  ADMIN_PUBLIC_KEY,
} from "../admin/auth/admin-auth.decorators";
import {
  roleAtLeast,
  type AdminRole,
} from "../admin/auth/entities/admin-user.entity";
import { MCP_TOOLS } from "../mcp/tools";
import { grantableScope } from "../admin/auth/admin-api-token.constants";
import { requestTier } from "./api-key-request.policy";

const SRC = join(__dirname, "..");

function controllerFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return controllerFiles(path);
    return name.endsWith(".controller.ts") ? [path] : [];
  });
}

/** Every handler behind the admin's guard, with the role it needs. */
async function adminHandlers() {
  const found: { name: string; minRole: AdminRole; isPublic: boolean }[] = [];
  for (const file of controllerFiles(SRC)) {
    const exported = (await import(file)) as Record<string, unknown>;
    for (const [className, cls] of Object.entries(exported)) {
      if (typeof cls !== "function") continue;
      if (Reflect.getMetadata(PATH_METADATA, cls) === undefined) continue;
      const guards =
        (Reflect.getMetadata(GUARDS_METADATA, cls) as unknown[]) ?? [];
      if (!guards.includes(AdminAuthGuard)) continue;
      const proto = (cls as { prototype: Record<string, unknown> }).prototype;
      for (const handler of Object.getOwnPropertyNames(proto)) {
        const target = proto[handler];
        if (typeof target !== "function" || handler === "constructor") continue;
        if (Reflect.getMetadata(METHOD_METADATA, target) === undefined)
          continue;
        const minRole = (Reflect.getMetadata(ADMIN_MIN_ROLE_KEY, target) ??
          Reflect.getMetadata(ADMIN_MIN_ROLE_KEY, cls) ??
          "viewer") as AdminRole;
        const isPublic =
          Reflect.getMetadata(ADMIN_PUBLIC_KEY, target) === true ||
          Reflect.getMetadata(ADMIN_PUBLIC_KEY, cls) === true;
        found.push({
          name: `${relative(SRC, file)} ${className}.${handler}`,
          minRole,
          isPublic,
        });
      }
    }
  }
  return found;
}

/** What every account reaches: its own session, password, second factor and account. */
const OWN_ACCOUNT_HANDLERS = [
  "AccountController.read",
  "AccountController.remove",
  "AccountController.write",
  "AdminAuthController.beginTotp",
  "AdminAuthController.changePassword",
  "AdminAuthController.confirmTotp",
  "AdminAuthController.disableTotp",
  "AdminAuthController.listSessions",
  "AdminAuthController.logout",
  "AdminAuthController.me",
  "AdminAuthController.revokeAll",
  "AdminAuthController.revokeSession",
];

/**
 * A developer account is an outside developer's, so it reaches its own
 * session, password and second factor, and the developer API, and nothing
 * else of the admin: no dashboards, no audit log, no tokens, no MCP.
 */
describe("the developer role", () => {
  let handlers: Awaited<ReturnType<typeof adminHandlers>> = [];
  beforeAll(async () => {
    handlers = await adminHandlers();
  }, 60_000);

  it("reaches only its own session endpoints, its account and the developer API", () => {
    const reachable = handlers
      .filter((h) => !h.isPublic && roleAtLeast("developer", h.minRole))
      .map((h) => h.name.split(" ")[1])
      .sort();
    expect(reachable).toEqual(
      [
        ...OWN_ACCOUNT_HANDLERS,
        "DeveloperController.createRequest",
        "DeveloperController.endGrace",
        "DeveloperController.overview",
        "DeveloperController.revokeKey",
        "DeveloperController.rotateKey",
        "DeveloperController.withdrawRequest",
      ].sort(),
    );
  });

  it("sees no MCP tool, and the MCP endpoint refuses it", () => {
    expect(
      MCP_TOOLS.filter((tool) =>
        roleAtLeast("developer", tool.minRole ?? "viewer"),
      ),
    ).toEqual([]);
    const mcp = handlers.filter((h) => h.name.includes("McpController"));
    expect(mcp.length).toBeGreaterThan(0);
    expect(mcp.every((h) => !roleAtLeast("developer", h.minRole))).toBe(true);
  });

  it("ranks below every staff role", () => {
    for (const role of ["owner", "editor", "author", "viewer"] as const) {
      expect(roleAtLeast("developer", role)).toBe(false);
      expect(roleAtLeast(role, "developer")).toBe(true);
    }
    // A token's scope is capped by its account's role; the token endpoints
    // refuse the role before this, but the cap never lifts it either.
    expect(grantableScope("admin:read", "developer")).toBe("admin:read");
  });
});

/**
 * A visitor's account reaches its own session endpoints and its synced state,
 * and nothing of the developer API, the admin or MCP.
 */
describe("the user role", () => {
  let handlers: Awaited<ReturnType<typeof adminHandlers>> = [];
  beforeAll(async () => {
    handlers = await adminHandlers();
  }, 60_000);

  it("reaches only its own session endpoints and its account", () => {
    const reachable = handlers
      .filter((h) => !h.isPublic && roleAtLeast("user", h.minRole))
      .map((h) => h.name.split(" ")[1])
      .sort();
    expect(reachable).toEqual([...OWN_ACCOUNT_HANDLERS].sort());
  });

  it("ranks below the developer and every staff role, and sees no MCP tool", () => {
    for (const role of [
      "owner",
      "editor",
      "author",
      "viewer",
      "developer",
    ] as const) {
      expect(roleAtLeast("user", role)).toBe(false);
    }
    expect(
      MCP_TOOLS.filter((tool) => roleAtLeast("user", tool.minRole ?? "viewer")),
    ).toEqual([]);
  });
});

describe("requestTier", () => {
  it("allows a free key only for open source that earns nothing but donations", () => {
    expect(requestTier({ source: "open-source", revenue: ["none"] })).toBe(
      "free",
    );
    expect(requestTier({ source: "open-source", revenue: ["donations"] })).toBe(
      "free",
    );
    expect(
      requestTier({ source: "open-source", revenue: ["donations", "ads"] }),
    ).toBe("paid");
    expect(requestTier({ source: "open-source", revenue: ["affiliate"] })).toBe(
      "paid",
    );
    expect(requestTier({ source: "closed-source", revenue: ["none"] })).toBe(
      "paid",
    );
  });
});
