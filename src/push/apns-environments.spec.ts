import { EventEmitter } from "events";
import { generateKeyPairSync } from "crypto";
import type { ClientHttp2Session } from "http2";
import { Repository } from "typeorm";
import {
  APNS_HOSTS,
  ApnsClient,
  getApnsConfig,
  isApnsConfigured,
  setStoredApnsCredentials,
  type ApnsEnvironment,
} from "./apns";
import { PushSubscription } from "./entities/push-subscription.entity";
import { PushService } from "./push.service";

const pem = () =>
  generateKeyPairSync("ec", { namedCurve: "prime256v1" })
    .privateKey.export({ type: "pkcs8", format: "pem" })
    .toString();

const CREDENTIALS = {
  keyId: "ABC123DEFG",
  teamId: "KC5QAXDX94",
  bundleId: "fan.park.app",
  pem: pem(),
};
const TOKEN = "ab".repeat(32);
const REQUEST = { headers: { "apns-push-type": "alert" }, body: "{}" };

type Reply = { status: number; reason?: string } | "error";

/** A fake HTTP/2 client: one session per `connect`, answering from a script per environment. */
function fakeApple(script: Record<ApnsEnvironment, Reply[]>) {
  const requests: {
    environment: ApnsEnvironment;
    headers: Record<string, string>;
  }[] = [];
  const sessions: { environment: ApnsEnvironment; closed: boolean }[] = [];
  const connectTo = (host: string): ClientHttp2Session => {
    const environment = (Object.keys(APNS_HOSTS) as ApnsEnvironment[]).find(
      (name) => APNS_HOSTS[name] === host,
    ) as ApnsEnvironment;
    const record = { environment, closed: false };
    sessions.push(record);
    const session = new EventEmitter() as EventEmitter &
      Record<string, unknown>;
    session.destroyed = false;
    session.unref = () => undefined;
    session.close = () => {
      record.closed = true;
      session.destroyed = true;
    };
    session.destroy = session.close;
    session.request = (headers: Record<string, string>) => {
      requests.push({ environment, headers });
      const reply = script[environment].shift() ?? { status: 500 };
      const stream = new EventEmitter() as EventEmitter &
        Record<string, unknown>;
      stream.setEncoding = () => undefined;
      stream.setTimeout = () => undefined;
      stream.end = () =>
        setImmediate(() => {
          if (reply === "error") return stream.emit("error", new Error("down"));
          stream.emit("response", { ":status": reply.status });
          if (reply.reason) {
            stream.emit("data", JSON.stringify({ reason: reply.reason }));
          }
          stream.emit("end");
        });
      return stream;
    };
    return session as unknown as ClientHttp2Session;
  };
  return { connectTo, requests, sessions };
}

const BAD = { status: 400, reason: "BadDeviceToken" };
const OK = { status: 200 };

describe("APNs in two environments", () => {
  const ENV = process.env;
  beforeEach(() => {
    process.env = { ...ENV };
    for (const name of Object.keys(process.env)) {
      if (name.startsWith("APNS_")) delete process.env[name];
    }
    setStoredApnsCredentials(CREDENTIALS);
  });
  afterEach(() => {
    process.env = ENV;
    setStoredApnsCredentials(null);
  });

  const client = (script: Record<ApnsEnvironment, Reply[]>) => {
    const apple = fakeApple(script);
    const apns = new ApnsClient();
    apns.connectTo = apple.connectTo;
    const errors: string[] = [];
    apns.onError = (environment, answer) =>
      errors.push(`${environment}:${answer.reason}`);
    return { apns, errors, ...apple };
  };
  const order = (requests: { environment: string }[]) =>
    requests.map((request) => request.environment);

  it("unknown device: production first, and its yes is the answer", async () => {
    const { apns, requests, errors } = client({
      production: [OK],
      sandbox: [],
    });
    await expect(apns.send(TOKEN, REQUEST)).resolves.toEqual({
      outcome: "sent",
      environment: "production",
      status: 200,
      reason: "",
    });
    expect(order(requests)).toEqual(["production"]);
    expect(requests[0].headers).toMatchObject({
      ":path": `/3/device/${TOKEN}`,
      "apns-topic": "fan.park.app",
      authorization: expect.stringMatching(/^bearer [\w-]+\.[\w-]+\.[\w-]+$/),
    });
    expect(errors).toEqual([]);
  });

  it("unknown device: BadDeviceToken in production is asked again in the sandbox", async () => {
    const { apns, requests, errors } = client({
      production: [BAD],
      sandbox: [OK],
    });
    await expect(apns.send(TOKEN, REQUEST)).resolves.toMatchObject({
      outcome: "sent",
      environment: "sandbox",
    });
    expect(order(requests)).toEqual(["production", "sandbox"]);
    // The miss in the first environment is not an error anyone counts.
    expect(errors).toEqual([]);
  });

  it("a device marked sandbox goes there first, and the other way round when it fails", async () => {
    const { apns, requests } = client({ production: [OK], sandbox: [OK, BAD] });
    await expect(apns.send(TOKEN, REQUEST, "sandbox")).resolves.toMatchObject({
      outcome: "sent",
      environment: "sandbox",
    });
    await expect(apns.send(TOKEN, REQUEST, "sandbox")).resolves.toMatchObject({
      outcome: "sent",
      environment: "production",
    });
    expect(order(requests)).toEqual(["sandbox", "sandbox", "production"]);
  });

  it("APNS_PRODUCTION=false only puts the sandbox first", async () => {
    process.env.APNS_PRODUCTION = "false";
    const { apns, requests } = client({ production: [OK], sandbox: [BAD] });
    await expect(apns.send(TOKEN, REQUEST)).resolves.toMatchObject({
      outcome: "sent",
      environment: "production",
    });
    expect(order(requests)).toEqual(["sandbox", "production"]);
  });

  it("a token both refuse is dead; 410 and a foreign topic are dead at once", async () => {
    const { apns, requests, errors } = client({
      production: [
        BAD,
        { status: 410, reason: "Unregistered" },
        { status: 400, reason: "DeviceTokenNotForTopic" },
      ],
      sandbox: [BAD],
    });
    await expect(apns.send(TOKEN, REQUEST)).resolves.toMatchObject({
      outcome: "dead",
      reason: "BadDeviceToken",
    });
    await expect(apns.send(TOKEN, REQUEST)).resolves.toMatchObject({
      outcome: "dead",
    });
    await expect(apns.send(TOKEN, REQUEST)).resolves.toMatchObject({
      outcome: "dead",
    });
    expect(order(requests)).toEqual([
      "production",
      "sandbox",
      "production",
      "production",
    ]);
    expect(errors).toHaveLength(3);
  });

  it("a second environment that cannot say leaves the device alone", async () => {
    const { apns } = client({
      production: [BAD, BAD],
      sandbox: [{ status: 503, reason: "ServiceUnavailable" }, "error"],
    });
    await expect(apns.send(TOKEN, REQUEST)).resolves.toMatchObject({
      outcome: "failed",
      reason: "ServiceUnavailable",
    });
    await expect(apns.send(TOKEN, REQUEST)).resolves.toMatchObject({
      outcome: "failed",
      status: 0,
    });
  });

  it("a key Apple restricts to production lets production's verdict stand", async () => {
    const { apns } = client({
      production: [BAD],
      sandbox: [{ status: 403, reason: "BadEnvironmentKeyIdInToken" }],
    });
    await expect(apns.send(TOKEN, REQUEST)).resolves.toMatchObject({
      outcome: "dead",
      environment: "production",
      reason: "BadDeviceToken",
    });
  });

  it("wrong-environment answers never open the breaker, and each environment has its own", async () => {
    const rounds = ApnsClient.BREAKER_FAILURES + 1;
    const { apns, requests } = client({
      production: Array(rounds + 1).fill(BAD),
      sandbox: Array(rounds).fill("error"),
    });
    for (let i = 0; i < rounds; i++) await apns.send(TOKEN, REQUEST);
    // The sandbox stopped being asked; production still is.
    expect(order(requests).filter((e) => e === "sandbox")).toHaveLength(
      ApnsClient.BREAKER_FAILURES,
    );
    await apns.send(TOKEN, REQUEST);
    expect(order(requests).filter((e) => e === "production")).toHaveLength(
      rounds + 1,
    );
  });

  it("keeps one connection per environment and one token for all", async () => {
    const { apns, requests, sessions } = client({
      production: [BAD, OK, OK],
      sandbox: [OK],
    });
    await apns.send(TOKEN, REQUEST);
    await apns.send(TOKEN, REQUEST);
    await apns.send(TOKEN, REQUEST);
    expect(order(sessions)).toEqual(["production", "sandbox"]);
    expect(new Set(requests.map((r) => r.headers.authorization)).size).toBe(1);
  });

  it("does not make a new token because Apple calls a young one expired", async () => {
    const { apns, requests } = client({
      production: [{ status: 403, reason: "ExpiredProviderToken" }, OK],
      sandbox: [],
    });
    await apns.send(TOKEN, REQUEST);
    await apns.send(TOKEN, REQUEST);
    expect(requests[0].headers.authorization).toBe(
      requests[1].headers.authorization,
    );
  });

  it("a new key is used at once, on new connections with a new token", async () => {
    const { apns, requests, sessions } = client({
      production: [OK, OK],
      sandbox: [],
    });
    await apns.send(TOKEN, REQUEST);
    setStoredApnsCredentials({
      ...CREDENTIALS,
      keyId: "ZZZ999YYYY",
      pem: pem(),
    });
    await apns.send(TOKEN, REQUEST);
    expect(sessions.map((session) => session.closed)).toEqual([true, false]);
    const kid = (authorization: string) =>
      JSON.parse(
        Buffer.from(
          authorization.split(" ")[1].split(".")[0],
          "base64url",
        ).toString(),
      ).kid;
    expect(requests.map((r) => kid(r.headers.authorization))).toEqual([
      "ABC123DEFG",
      "ZZZ999YYYY",
    ]);
  });

  it("checks a key on a connection of its own, with a push nobody would see", async () => {
    const { apns, requests, sessions, errors } = client({
      production: [BAD],
      sandbox: ["error"],
    });
    await expect(apns.check("production")).resolves.toEqual({
      status: 400,
      reason: "BadDeviceToken",
    });
    await expect(apns.check("sandbox")).resolves.toEqual({
      status: 0,
      reason: "Unreachable",
    });
    expect(sessions.map((session) => session.closed)).toEqual([true, true]);
    expect(requests[0].headers).toMatchObject({
      ":path": expect.stringMatching(/^\/3\/device\/[0-9a-f]{64}$/),
      "apns-push-type": "background",
      "apns-priority": "5",
    });
    expect(errors).toEqual([]);
  });

  describe("the uploaded key and the environment's", () => {
    it("the uploaded key wins, and its removal falls back", () => {
      process.env.APNS_KEY_ID = "ENVKEY1234";
      process.env.APNS_TEAM_ID = "ENVTEAM123";
      process.env.APNS_BUNDLE_ID = "fan.park.env";
      process.env.APNS_KEY = pem().replace(/\n/g, "\\n");
      expect(getApnsConfig()).toMatchObject({
        source: "database",
        keyId: "ABC123DEFG",
      });
      setStoredApnsCredentials(null);
      expect(getApnsConfig()).toMatchObject({
        source: "environment",
        keyId: "ENVKEY1234",
        bundleId: "fan.park.env",
      });
      delete process.env.APNS_KEY;
      expect(isApnsConfigured()).toBe(false);
    });

    it("an uploaded key that does not parse is no key", () => {
      expect(setStoredApnsCredentials({ ...CREDENTIALS, pem: "nope" })).toBe(
        false,
      );
      expect(isApnsConfigured()).toBe(false);
    });
  });

  describe("what a send teaches the device row", () => {
    const device = (patch: Partial<PushSubscription> = {}) =>
      ({
        id: "dev-1",
        apnsToken: TOKEN,
        topics: ["news"],
        locale: "de",
        apnsEnvironment: null,
        apnsLastError: null,
        ...patch,
      }) as PushSubscription;
    const news = { title: "t", body: "b", url: "/", tag: "news:x" };

    const service = (
      script: Record<ApnsEnvironment, Reply[]>,
      row = device(),
    ) => {
      const { apns, requests } = client(script);
      const repository = {
        update: jest.fn(),
        increment: jest.fn(),
        delete: jest.fn().mockResolvedValue({ affected: 0 }),
        findOne: jest.fn().mockResolvedValue(row),
      };
      return {
        requests,
        repository,
        push: new PushService(
          repository as unknown as Repository<PushSubscription>,
          apns,
        ),
      };
    };

    it("remembers the environment that took the token", async () => {
      const { push, repository } = service({
        production: [BAD],
        sandbox: [OK],
      });
      await expect(push.send(device(), news)).resolves.toBe(true);
      expect(repository.update).toHaveBeenCalledWith(
        { id: "dev-1" },
        expect.objectContaining({
          apnsEnvironment: "sandbox",
          apnsLastError: null,
          failureCount: 0,
        }),
      );
      expect(repository.delete).not.toHaveBeenCalled();
    });

    it("deletes a device both environments refuse, as an invalid token always was", async () => {
      const { push, repository } = service({
        production: [BAD],
        sandbox: [BAD],
      });
      await expect(push.send(device(), news)).resolves.toBe(false);
      expect(repository.delete).toHaveBeenCalledWith({ id: "dev-1" });
    });

    it("keeps the device and notes the reason when our key is the problem", async () => {
      const refused = { status: 403, reason: "InvalidProviderToken" };
      const { push, repository } = service({
        production: [refused, refused],
        sandbox: [],
      });
      const row = device();
      await push.send(row, news);
      await push.send(row, news);
      expect(repository.update).toHaveBeenCalledTimes(1);
      expect(repository.update).toHaveBeenCalledWith(
        { id: "dev-1" },
        { apnsLastError: "InvalidProviderToken" },
      );
      expect(repository.delete).not.toHaveBeenCalled();
      expect(repository.increment).not.toHaveBeenCalled();
    });

    it("sends the admin's test whatever kinds the device chose", async () => {
      const { push, requests } = service(
        { production: [], sandbox: [OK] },
        device({ topics: [], apnsEnvironment: "sandbox" }),
      );
      await expect(push.sendTest("dev-1")).resolves.toMatchObject({
        outcome: "sent",
        environment: "sandbox",
      });
      expect(order(requests)).toEqual(["sandbox"]);
    });

    it("has no test for a device that is gone", async () => {
      const { push, repository } = service({ production: [], sandbox: [] });
      repository.findOne.mockResolvedValue(null);
      await expect(push.sendTest("dev-1")).resolves.toBeNull();
    });
  });

  describe("registering a device", () => {
    const rows = new Map<string, PushSubscription>();
    const repository = {
      findOne: jest.fn(async ({ where }) => rows.get(where.endpoint) ?? null),
      create: jest.fn((row) => ({ ...row })),
      delete: jest.fn(),
      save: jest.fn(async (row) => rows.set(row.endpoint, row) && row),
    };
    const push = new PushService(repository as never, {} as never);
    const register = (token: string, environment?: ApnsEnvironment) =>
      push.registerDevice({
        installId: "6f1c2d3e-4a5b-4c6d-8e7f-0123456789ab",
        token,
        kinds: [],
        newsParks: null,
        environment,
      });

    it("stores what the app says, keeps it while the token stays, forgets it with a new token", async () => {
      expect((await register("aa", "sandbox"))?.apnsEnvironment).toBe(
        "sandbox",
      );
      expect((await register("aa"))?.apnsEnvironment).toBe("sandbox");
      expect((await register("bb"))?.apnsEnvironment).toBeNull();
      expect((await register("bb", "production"))?.apnsEnvironment).toBe(
        "production",
      );
    });
  });
});
