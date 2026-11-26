import "reflect-metadata";
import { HttpException, RequestMethod } from "@nestjs/common";
import { METHOD_METADATA } from "@nestjs/common/constants";
import { ADMIN_MIN_ROLE_KEY } from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { API_OPEN_METADATA } from "../api-keys/api-permission.decorator";
import {
  AdminAppConfigController,
  AppConfigController,
} from "./app-config.controller";
import { AppConfigService } from "./app-config.service";
import type { AppConfig } from "./app-config.entity";

const ADMIN = {
  userId: "u1",
  email: "owner@park.fan",
  role: "owner",
} as AdminPrincipal;
const body = (over: Record<string, unknown> = {}) => ({
  minVersion: "1.0.1",
  minBuild: 4,
  updateUrl: "https://apps.apple.com/app/id1",
  message: { de: "Bitte aktualisieren", en: "" },
  ...over,
});

describe("app config", () => {
  let row: AppConfig | null;
  let fail: boolean;
  let service: AppConfigService;
  let admin: AdminAppConfigController;
  let open: AppConfigController;

  beforeEach(() => {
    row = null;
    fail = false;
    const repo = {
      findOneBy: jest.fn(async () => {
        if (fail) throw new Error("db down");
        return row;
      }),
      create: (v: Partial<AppConfig>) => v as AppConfig,
      save: jest.fn(
        async (r: AppConfig) => (row = { ...r, updatedAt: new Date() }),
      ),
    };
    service = new AppConfigService(repo as never);
    admin = new AdminAppConfigController(service);
    open = new AppConfigController(service);
  });

  it("blocks nothing until set, and never fails", async () => {
    const off = {
      minVersion: null,
      minBuild: null,
      updateUrl: null,
      message: null,
    };
    const none = { ios: off, android: off };
    expect(await open.get()).toEqual(none);
    fail = true;
    expect(await open.get()).toEqual(none);
  });

  it("stores the values, drops empty messages and echoes a sample", async () => {
    const view = await admin.set("ios", body(), ADMIN, "1.0.0", "3");
    expect(view.platforms.ios).toEqual({
      minVersion: "1.0.1",
      minBuild: 4,
      updateUrl: "https://apps.apple.com/app/id1",
      message: { de: "Bitte aktualisieren" },
    });
    expect(view.samples.ios.text).toBe("1.0.0 (3) → blocked");
    expect(view.samples.android.text).toBe("1.0.0 (3) → allowed");
    expect(view.updatedBy).toBe("owner@park.fan");
    expect((await open.get()).ios.minVersion).toBe("1.0.1");
    expect((await open.get()).android.minVersion).toBeNull();
    expect((await admin.get("1.0.1", "4")).samples.ios.text).toBe(
      "1.0.1 (4) → allowed",
    );
  });

  it.each([
    ["version", { minVersion: "1.0.1.2" }],
    ["build", { minBuild: 0 }],
    ["http url", { updateUrl: "http://x.test" }],
    ["long message", { message: { de: "x".repeat(301) } }],
    ["unknown language", { message: { xx: "hi" } }],
    ["build without version", { minVersion: null }],
  ])("refuses a bad %s", async (_name, over) => {
    await expect(admin.set("ios", body(over), ADMIN)).rejects.toBeInstanceOf(
      HttpException,
    );
    expect(row).toBeNull();
  });

  it("keeps the platforms apart and refuses an unknown one", async () => {
    await admin.set("ios", body(), ADMIN);
    const view = await admin.set(
      "android",
      body({
        minVersion: "2",
        minBuild: 30,
        updateUrl: "https://play.google.com/store/apps/details?id=x",
      }),
      ADMIN,
      "1.0.1",
      "4",
    );
    expect(view.platforms.ios.minVersion).toBe("1.0.1");
    expect(view.platforms.android.minBuild).toBe(30);
    expect(view.samples.android.blocked).toBe(true);
    expect(() => admin.set("windows", body(), ADMIN)).toThrow(HttpException);
  });

  it("is open for the app and owner-only to write", () => {
    expect(
      Reflect.getMetadata(API_OPEN_METADATA, AppConfigController),
    ).toBeTruthy();
    const put = AdminAppConfigController.prototype.set;
    expect(Reflect.getMetadata(METHOD_METADATA, put)).toBe(RequestMethod.PUT);
    expect(Reflect.getMetadata(ADMIN_MIN_ROLE_KEY, put)).toBe("owner");
  });
});
