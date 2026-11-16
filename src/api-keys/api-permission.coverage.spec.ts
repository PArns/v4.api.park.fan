import "reflect-metadata";
import { readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import {
  GUARDS_METADATA,
  METHOD_METADATA,
  PATH_METADATA,
} from "@nestjs/common/constants";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import { API_OPEN_METADATA } from "./api-permission.decorator";
import { API_PERMISSION_METADATA } from "./forecast-access.guard";
import { isApiPermission, INTERNAL_PERMISSION } from "./permissions";

const SRC = join(__dirname, "..");

/** Every `*.controller.ts` under src, specs aside. */
function controllerFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return controllerFiles(path);
    return name.endsWith(".controller.ts") ? [path] : [];
  });
}

interface Route {
  file: string;
  controller: string;
  handler: string;
  target: object;
  cls: object;
}

/** The route handlers of every controller class the files export. */
async function routes(): Promise<Route[]> {
  const found: Route[] = [];
  for (const file of controllerFiles(SRC)) {
    const exported = (await import(file)) as Record<string, unknown>;
    for (const [name, value] of Object.entries(exported)) {
      if (typeof value !== "function") continue;
      if (Reflect.getMetadata(PATH_METADATA, value) === undefined) continue;
      const proto = (value as { prototype: Record<string, unknown> }).prototype;
      for (const handler of Object.getOwnPropertyNames(proto)) {
        const target = proto[handler];
        if (typeof target !== "function" || handler === "constructor") continue;
        if (Reflect.getMetadata(METHOD_METADATA, target) === undefined)
          continue;
        found.push({
          file: relative(SRC, file),
          controller: name,
          handler,
          target,
          cls: value,
        });
      }
    }
  }
  return found;
}

/** The guards on a handler and its controller. */
function guardsOf(route: Route): unknown[] {
  return [
    ...((Reflect.getMetadata(GUARDS_METADATA, route.cls) as unknown[]) ?? []),
    ...((Reflect.getMetadata(GUARDS_METADATA, route.target) as unknown[]) ??
      []),
  ];
}

/**
 * Every route of the API is classified: behind the admin's own guard (admin,
 * MCP, the developer dashboard), behind one data permission, or open for a
 * stated reason. A new route without either would serve its data to anyone;
 * this is where it fails instead.
 */
describe("public route permissions", () => {
  let all: Route[] = [];
  beforeAll(async () => {
    all = await routes();
  }, 60_000);

  it("finds the routes", () => {
    expect(all.length).toBeGreaterThan(150);
  });

  it("classifies every route", () => {
    const unclassified = all
      .filter((route) => {
        if (guardsOf(route).includes(AdminAuthGuard)) return false;
        const meta = (key: string) =>
          Reflect.getMetadata(key, route.target) ??
          Reflect.getMetadata(key, route.cls);
        return !meta(API_PERMISSION_METADATA) && !meta(API_OPEN_METADATA);
      })
      .map((route) => `${route.file} ${route.controller}.${route.handler}`);
    expect(unclassified).toEqual([]);
  });

  it("names only permissions the catalog knows", () => {
    const unknown = all
      .map(
        (route) =>
          Reflect.getMetadata(API_PERMISSION_METADATA, route.target) as
            { permission: string } | undefined,
      )
      .filter(
        (meta) =>
          meta &&
          meta.permission !== INTERNAL_PERMISSION &&
          !isApiPermission(meta.permission),
      );
    expect(unknown).toEqual([]);
  });

  it("leaves only the live wait times open without a key", () => {
    const keyless = all
      .filter(
        (route) =>
          (
            Reflect.getMetadata(API_PERMISSION_METADATA, route.target) as
              { permission: string } | undefined
          )?.permission === "live.waits",
      )
      .map((route) => `${route.controller}.${route.handler}`);
    expect(keyless).toEqual([
      "ParksController.getParkWaitTimesByGeographicPath",
    ]);
  });
});
