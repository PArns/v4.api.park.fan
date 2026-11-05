import "reflect-metadata";
import { RequestMethod } from "@nestjs/common";
import { METHOD_METADATA, PATH_METADATA } from "@nestjs/common/constants";
import { AdminController } from "./admin.controller";
import { ADMIN_MIN_ROLE_KEY } from "./auth/admin-auth.decorators";

/**
 * "Reads need any session. Job triggers and curation need `editor`." A write
 * route without `@AdminMinRole` is open to every `viewer` (PAR-1129:
 * `rebuild-downtime` was), so every write route must name its role.
 */
describe("AdminController write routes", () => {
  const writes = [
    RequestMethod.POST,
    RequestMethod.PATCH,
    RequestMethod.PUT,
    RequestMethod.DELETE,
  ];
  const proto = AdminController.prototype as unknown as Record<string, unknown>;
  const routes = Object.getOwnPropertyNames(proto)
    .map((name) => ({ name, handler: proto[name] }))
    .filter(
      ({ handler }) =>
        typeof handler === "function" &&
        writes.includes(Reflect.getMetadata(METHOD_METADATA, handler)),
    );

  it("finds the write routes", () => {
    expect(routes.length).toBeGreaterThan(20);
  });

  it.each(routes.map((r) => [r.name, r.handler] as const))(
    "%s requires at least editor",
    (_name, handler) => {
      const path = Reflect.getMetadata(PATH_METADATA, handler as object);
      const role = Reflect.getMetadata(ADMIN_MIN_ROLE_KEY, handler as object);
      expect({ path, role }).toEqual({
        path,
        role: expect.stringMatching(/^(editor|owner)$/),
      });
    },
  );
});
