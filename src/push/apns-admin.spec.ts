import "reflect-metadata";
import { generateKeyPairSync } from "crypto";
import { HttpException, RequestMethod } from "@nestjs/common";
import { METHOD_METADATA } from "@nestjs/common/constants";
import { ADMIN_SELF_AUDITED_KEY } from "../admin/auth/admin-audit.interceptor";
import { ADMIN_MIN_ROLE_KEY } from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { AdminPushController } from "./admin-push.controller";
import { ApnsAdminService } from "./apns-admin.service";
import { ApnsClient, getApnsConfig, setStoredApnsCredentials } from "./apns";
import { ApnsSettings } from "./entities/apns-settings.entity";
import { PushController } from "./push.controller";

const pem = () =>
  generateKeyPairSync("ec", { namedCurve: "prime256v1" })
    .privateKey.export({ type: "pkcs8", format: "pem" })
    .toString();
const PEM = pem();
const BODY = PEM.split("\n")[1];
const ADMIN = {
  userId: "u1",
  email: "owner@park.fan",
  role: "owner",
} as AdminPrincipal;

describe("the APNs key in the admin", () => {
  const ENV = process.env;
  let saved: ApnsSettings | null;
  let devices: Record<string, unknown>[];
  let redis: { hash: Record<string, string>; count: number };
  let apns: ApnsClient;
  let check: jest.SpyInstance;
  let sendTest: jest.Mock;
  let audit: { record: jest.Mock };
  let service: ApnsAdminService;
  let controller: AdminPushController;

  const build = () =>
    new ApnsAdminService(
      {
        findOneBy: async () => (saved ? { ...saved } : null),
        create: (row: ApnsSettings) => ({ ...row }),
        save: async (row: ApnsSettings) => (saved = { ...row }),
      } as never,
      { find: async () => devices } as never,
      {
        hincrby: async (_key: string, field: string) =>
          (redis.hash[field] = String(Number(redis.hash[field] ?? 0) + 1)),
        hgetall: async (key: string) =>
          key.endsWith(new Date().toISOString().slice(0, 13)) ? redis.hash : {},
        expire: async () => 1,
        incr: async () => ++redis.count,
        ttl: async () => 600,
      } as never,
      apns,
      { sendTest } as never,
    );

  beforeEach(() => {
    process.env = { ...ENV, ASSISTANT_SECRET_KEY: "ab".repeat(32) };
    for (const name of Object.keys(process.env)) {
      if (name.startsWith("APNS_")) delete process.env[name];
    }
    saved = null;
    devices = [];
    redis = { hash: {}, count: 0 };
    apns = new ApnsClient();
    check = jest
      .spyOn(apns, "check")
      .mockImplementation(async (environment) =>
        environment === "production"
          ? { status: 400, reason: "BadDeviceToken" }
          : { status: 403, reason: "BadEnvironmentKeyIdInToken" },
      );
    sendTest = jest.fn();
    audit = { record: jest.fn() };
    service = build();
    controller = new AdminPushController(service, audit as never);
  });
  afterEach(() => {
    process.env = ENV;
    setStoredApnsCredentials(null);
  });

  const upload = (patch: Record<string, unknown> = {}) =>
    controller.uploadKey(
      { content: PEM, filename: "AuthKey_ABC123DEFG.p8", ...patch },
      ADMIN,
    );
  const refusal = async (attempt: Promise<unknown>) => {
    const error = await attempt.then(
      () => null,
      (thrown: HttpException) => thrown,
    );
    expect(error).toBeInstanceOf(HttpException);
    return {
      status: (error as HttpException).getStatus(),
      message: String((error as HttpException).getResponse()),
    };
  };

  it("stores the key sealed, uses it at once and checks it against Apple", async () => {
    expect(
      new PushController({} as never, {} as never, {} as never).status().apns,
    ).toBe(false);
    const status = await upload();
    expect(status).toMatchObject({
      configured: true,
      source: "database",
      keyId: "ABC123DEFG",
      teamId: "KC5QAXDX94",
      bundleId: "fan.park.app",
      fingerprint: expect.stringMatching(/^[0-9a-f]{8}$/),
      uploadedBy: "owner@park.fan",
      storedKeyUnreadable: false,
      checks: {
        production: {
          state: "ok",
          text: "Schlüssel wird von Apple akzeptiert.",
        },
        sandbox: { state: "warning", reason: "BadEnvironmentKeyIdInToken" },
      },
    });
    expect(check).toHaveBeenCalledTimes(2);
    // Sealed at rest, and no answer carries any of it.
    expect(saved?.keySealed).toMatch(/^v1\./);
    expect(JSON.stringify(saved)).not.toContain(BODY);
    expect(JSON.stringify(status)).not.toContain(BODY);
    expect(JSON.stringify(status)).not.toContain("PRIVATE KEY");
    expect(
      new PushController({} as never, {} as never, {} as never).status().apns,
    ).toBe(true);
  });

  it("audits the upload by fingerprint, never with the key", async () => {
    const status = await upload({
      teamId: "TEAM123456",
      bundleId: "fan.park.beta",
    });
    expect(audit.record).toHaveBeenCalledTimes(1);
    const entry = audit.record.mock.calls[0][0];
    expect(entry).toMatchObject({
      actor: ADMIN,
      action: "push.apns-key.upload",
      before: { source: null, fingerprint: null },
      after: {
        source: "database",
        keyId: "ABC123DEFG",
        teamId: "TEAM123456",
        bundleId: "fan.park.beta",
        fingerprint: status.fingerprint,
      },
    });
    expect(JSON.stringify(entry)).not.toContain(BODY);
  });

  it("a restart reads the stored key back", async () => {
    await upload();
    setStoredApnsCredentials(null);
    await build().onModuleInit();
    expect(getApnsConfig()).toMatchObject({
      source: "database",
      keyId: "ABC123DEFG",
    });
  });

  it("a stored key the server's secret cannot open is said so, and the environment applies", async () => {
    await upload();
    setStoredApnsCredentials(null);
    process.env.ASSISTANT_SECRET_KEY = "cd".repeat(32);
    await build().onModuleInit();
    expect(await service.status()).toMatchObject({
      configured: false,
      storedKeyUnreadable: true,
    });
  });

  it("takes the key id from the request over the file name, and asks for it when neither has one", async () => {
    expect(
      (await upload({ filename: "key.p8", keyId: "ZZZ999YYYY" })).keyId,
    ).toBe("ZZZ999YYYY");
    const { status, message } = await refusal(upload({ filename: "key.p8" }));
    expect(status).toBe(400);
    expect(message).toContain("Bitte die Key ID angeben");
  });

  it.each([
    [{ keyId: "short" }, "Key ID"],
    [{ teamId: "kc5qaxdx94" }, "Team ID"],
    [{ bundleId: "fan park" }, "Bundle-ID"],
    [{ content: "-----BEGIN CERTIFICATE-----\nMIIB" }, "Zertifikat"],
    [{ content: "hello" }, "nicht der APNs-Schlüssel"],
    [{ content: PEM + "x".repeat(5000) }, "zu groß"],
    [{ content: "x".repeat(30_000) }, "content"],
    [{ content: 5 }, "content"],
  ])(
    "refuses %j without storing, auditing or repeating it",
    async (patch, part) => {
      const { status, message } = await refusal(upload(patch));
      expect(status).toBe(400);
      expect(message).toContain(part);
      expect(message).not.toContain(BODY);
      expect(saved).toBeNull();
      expect(audit.record).not.toHaveBeenCalled();
      expect(getApnsConfig()).toBeNull();
    },
  );

  it("refuses to store without the server's secret, and says which", async () => {
    delete process.env.ASSISTANT_SECRET_KEY;
    const { status, message } = await refusal(upload());
    expect(status).toBe(409);
    expect(message).toContain("ASSISTANT_SECRET_KEY");
    expect(saved).toBeNull();
    expect((await service.status()).secretConfigured).toBe(false);
  });

  it("removal falls back to the environment's key, audited", async () => {
    process.env.APNS_KEY_ID = "ENVKEY1234";
    process.env.APNS_TEAM_ID = "KC5QAXDX94";
    process.env.APNS_BUNDLE_ID = "fan.park.app";
    process.env.APNS_KEY = pem();
    expect((await upload()).source).toBe("database");
    const status = await controller.removeKey(ADMIN);
    expect(status).toMatchObject({
      configured: true,
      source: "environment",
      keyId: "ENVKEY1234",
      uploadedBy: null,
      checks: { production: null, sandbox: null },
    });
    expect(saved?.keySealed).toBeNull();
    expect(audit.record).toHaveBeenLastCalledWith(
      expect.objectContaining({
        action: "push.apns-key.remove",
        before: expect.objectContaining({
          source: "database",
          keyId: "ABC123DEFG",
        }),
        after: expect.objectContaining({ source: "environment" }),
      }),
    );
    delete process.env.APNS_KEY;
    expect((await service.status()).configured).toBe(false);
  });

  it("checks on demand, and not without end", async () => {
    await upload();
    check.mockResolvedValue({ status: 403, reason: "InvalidProviderToken" });
    const status = await controller.check(ADMIN);
    expect(status.checks.production).toMatchObject({
      state: "error",
      text: expect.stringContaining("Key ID passt nicht"),
    });
    redis.count = 20;
    expect((await refusal(controller.check(ADMIN))).status).toBe(429);
  });

  it("counts devices by environment and kind, and Apple's refusals by reason", async () => {
    devices = [
      { topics: ["news", "show"], apnsEnvironment: "production" },
      { topics: ["news"], apnsEnvironment: "sandbox" },
      { topics: [], apnsEnvironment: null },
    ];
    apns.onError("production", { status: 403, reason: "InvalidProviderToken" });
    apns.onError("production", { status: 403, reason: "InvalidProviderToken" });
    apns.onError("sandbox", { status: 0, reason: "Unreachable" });
    await new Promise((done) => setImmediate(done));
    expect(await service.status()).toMatchObject({
      devices: {
        total: 3,
        byEnvironment: { production: 1, sandbox: 1, unknown: 1 },
        byKind: { news: 2, show: 1 },
      },
      errors: [
        { environment: "production", reason: "InvalidProviderToken", count: 2 },
        { environment: "sandbox", reason: "Unreachable", count: 1 },
      ],
    });
  });

  it("lists devices with the end of their token only", async () => {
    const token = "ab".repeat(29) + "123456";
    devices = [
      {
        id: "d1",
        apnsToken: token,
        topics: ["news"],
        locale: "de",
        timezone: "Europe/Berlin",
        apnsEnvironment: "sandbox",
        createdAt: new Date(0),
        lastNotifiedAt: null,
        apnsLastError: "Unreachable",
      },
    ];
    const list = await controller.devices();
    expect(list).toEqual([
      expect.objectContaining({
        id: "d1",
        tokenTail: "123456",
        environment: "sandbox",
        lastError: "Unreachable",
      }),
    ]);
    expect(JSON.stringify(list)).not.toContain(token);
  });

  it("sends a test and says what Apple answered", async () => {
    const deviceId = "6f1c2d3e-4a5b-4c6d-8e7f-0123456789ab";
    expect((await refusal(controller.test({ deviceId }, ADMIN))).status).toBe(
      409,
    );
    await upload();
    sendTest.mockResolvedValue({
      outcome: "sent",
      environment: "sandbox",
      status: 200,
      reason: "",
    });
    await expect(controller.test({ deviceId }, ADMIN)).resolves.toEqual({
      status: 200,
      reason: "",
      environment: "sandbox",
      state: "ok",
      text: "Apple hat die Mitteilung angenommen.",
    });
    sendTest.mockResolvedValue(null);
    expect((await refusal(controller.test({ deviceId }, ADMIN))).status).toBe(
      404,
    );
    expect(
      (await refusal(controller.test({ deviceId: "x" }, ADMIN))).status,
    ).toBe(400);
  });

  describe("permissions", () => {
    const proto = AdminPushController.prototype as unknown as Record<
      string,
      object
    >;
    const meta = (name: string) => ({
      write:
        Reflect.getMetadata(METHOD_METADATA, proto[name]) !== RequestMethod.GET,
      role: Reflect.getMetadata(ADMIN_MIN_ROLE_KEY, proto[name]),
      selfAudited:
        Reflect.getMetadata(ADMIN_SELF_AUDITED_KEY, proto[name]) === true,
    });

    it("every write is for owners; reading is for any account", () => {
      for (const name of ["uploadKey", "removeKey", "check", "test"]) {
        expect(meta(name)).toMatchObject({ write: true, role: "owner" });
      }
      for (const name of ["status", "devices"]) {
        expect(meta(name)).toMatchObject({ write: false, role: undefined });
      }
    });

    it("only the key's own audit rows replace the interceptor's", () => {
      expect(meta("uploadKey").selfAudited).toBe(true);
      expect(meta("removeKey").selfAudited).toBe(true);
      // The check and the test are recorded by `AdminAuditInterceptor`.
      expect(meta("check").selfAudited).toBe(false);
      expect(meta("test").selfAudited).toBe(false);
    });
  });
});
