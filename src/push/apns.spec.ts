import { createPublicKey, generateKeyPairSync, verify } from "crypto";
import { Repository } from "typeorm";
import * as webpush from "web-push";
import {
  ApnsClient,
  apnsJwt,
  buildApnsRequest,
  classifyApnsResponse,
  getApnsConfig,
} from "./apns";
import { pushKindOf } from "./push-config";
import { PushService } from "./push.service";
import { PushSubscription } from "./entities/push-subscription.entity";

jest.mock("web-push", () => ({ sendNotification: jest.fn() }));

const { privateKey } = generateKeyPairSync("ec", { namedCurve: "prime256v1" });
const PEM = privateKey.export({ type: "pkcs8", format: "pem" }).toString();

const APNS_ENV = {
  APNS_KEY_ID: "ABC123DEFG",
  APNS_TEAM_ID: "KC5QAXDX94",
  APNS_BUNDLE_ID: "fan.park.app",
  // As pasted into one env line.
  APNS_KEY: PEM.replace(/\n/g, "\\n"),
};

describe("APNs", () => {
  const ENV = process.env;
  afterEach(() => {
    process.env = ENV;
  });

  describe("getApnsConfig", () => {
    it("is null without every variable — the no-op path", () => {
      process.env = { ...ENV, ...APNS_ENV, APNS_KEY_ID: "" };
      expect(getApnsConfig()).toBeNull();
    });

    it("is null for a key that does not parse", () => {
      process.env = { ...ENV, ...APNS_ENV, APNS_KEY: "not a key" };
      expect(getApnsConfig()).toBeNull();
    });

    it("reads an escaped PEM, production by default, sandbox on request", () => {
      process.env = { ...ENV, ...APNS_ENV };
      expect(getApnsConfig()?.host).toBe("https://api.push.apple.com");
      process.env = { ...ENV, ...APNS_ENV, APNS_PRODUCTION: "false" };
      expect(getApnsConfig()?.host).toBe("https://api.sandbox.push.apple.com");
    });
  });

  it("signs an ES256 provider token Apple can verify", () => {
    process.env = { ...ENV, ...APNS_ENV };
    const config = getApnsConfig()!;
    const [header, claims, signature] = apnsJwt(config, 1_700_000_000).split(
      ".",
    );
    expect(JSON.parse(Buffer.from(header, "base64url").toString())).toEqual({
      alg: "ES256",
      kid: "ABC123DEFG",
    });
    expect(JSON.parse(Buffer.from(claims, "base64url").toString())).toEqual({
      iss: "KC5QAXDX94",
      iat: 1_700_000_000,
    });
    expect(
      verify(
        "sha256",
        Buffer.from(`${header}.${claims}`),
        { key: createPublicKey(privateKey), dsaEncoding: "ieee-p1363" },
        Buffer.from(signature, "base64url"),
      ),
    ).toBe(true);
  });

  describe("buildApnsRequest", () => {
    const message = {
      title: "Taron: nur noch 15 Min.",
      body: "Phantasialand",
      url: "/parks/europe/germany/bruehl/phantasialand/taron",
      tag: "ride-alert:3f2c",
    };

    it("puts title, body, url and kind where the app reads them", () => {
      const request = buildApnsRequest(message, "ride-wait", 1_000_000);
      expect(JSON.parse(request.body)).toEqual({
        aps: {
          alert: { title: message.title, body: message.body },
          sound: "default",
          "thread-id": "ride-wait",
        },
        url: message.url,
        kind: "ride-wait",
      });
      expect(request.headers).toMatchObject({
        "apns-push-type": "alert",
        "apns-expiration": String(1000 + 900),
        "apns-collapse-id": "ride-alert:3f2c",
      });
    });

    it("hashes a tag past Apple's 64-byte collapse-id cap", () => {
      const tag = `next-up:${"x".repeat(80)}`;
      const id = buildApnsRequest({ ...message, tag }, "next-up", 0).headers[
        "apns-collapse-id"
      ];
      expect(id).toMatch(/^[0-9a-f]{64}$/);
    });
  });

  it("stops calling an unreachable Apple for a few minutes", async () => {
    process.env = { ...ENV, ...APNS_ENV };
    const client = new ApnsClient();
    const post = jest
      .spyOn(client as unknown as { post: () => Promise<unknown> }, "post")
      .mockRejectedValue(new Error("APNs timeout"));
    const request = { headers: {}, body: "{}" };
    let clock = 1_000_000;
    const now = jest.spyOn(Date, "now").mockImplementation(() => clock);
    try {
      for (let i = 0; i < ApnsClient.BREAKER_FAILURES; i++) {
        await expect(client.send("ab", request)).resolves.toBe("failed");
      }
      expect(post).toHaveBeenCalledTimes(ApnsClient.BREAKER_FAILURES);
      // Open: no request at all.
      await expect(client.send("ab", request)).resolves.toBe("failed");
      expect(post).toHaveBeenCalledTimes(ApnsClient.BREAKER_FAILURES);
      // After the pause Apple is asked again, and an answer closes it.
      clock += ApnsClient.BREAKER_OPEN_MS;
      post.mockResolvedValue({ status: 200, body: "" });
      await expect(client.send("ab", request)).resolves.toBe("sent");
    } finally {
      now.mockRestore();
    }
  });

  it("classifies dead tokens apart from bad minutes", () => {
    expect(classifyApnsResponse(200, "")).toBe("sent");
    expect(classifyApnsResponse(410, '{"reason":"Unregistered"}')).toBe("dead");
    expect(classifyApnsResponse(400, '{"reason":"BadDeviceToken"}')).toBe(
      "dead",
    );
    expect(
      classifyApnsResponse(400, '{"reason":"DeviceTokenNotForTopic"}'),
    ).toBe("dead");
    expect(classifyApnsResponse(400, '{"reason":"PayloadTooLarge"}')).toBe(
      "failed",
    );
    expect(classifyApnsResponse(500, "")).toBe("failed");
    expect(classifyApnsResponse(429, "not json")).toBe("failed");
  });

  it("knows each producer's kind by tag, or by its own kind", () => {
    expect(pushKindOf({ tag: "next-up:p:2026-10-17:e1:870" })).toBe("next-up");
    expect(pushKindOf({ tag: "show-start:9ab1:2026" })).toBe("show");
    expect(pushKindOf({ tag: "rain-soon:x" })).toBe("rain-soon");
    expect(pushKindOf({ tag: "morning-plan:x" })).toBe("morning-plan");
    expect(pushKindOf({ tag: "plan-change:p:2026-10-17" })).toBe("plan-change");
    expect(pushKindOf({ tag: "guides:freizeitpark-packliste" })).toBe("guides");
    expect(pushKindOf({ tag: "ride-alert:x" })).toBeNull();
    expect(pushKindOf({ tag: "ride-alert:x", kind: "ride-reopen" })).toBe(
      "ride-reopen",
    );
  });

  describe("PushService.send to a device", () => {
    let repository: Record<string, jest.Mock>;
    let apns: { send: jest.Mock };
    let service: PushService;
    const device = {
      id: "dev-1",
      endpoint: "apns:6f1c",
      apnsToken: "ab".repeat(32),
      p256dh: "",
      auth: "",
      topics: ["ride-wait", "news"],
      failureCount: 0,
    } as unknown as PushSubscription;
    const wait = {
      title: "t",
      body: "b",
      url: "/",
      tag: "ride-alert:1",
      kind: "ride-wait" as const,
    };

    beforeEach(() => {
      process.env = { ...ENV, ...APNS_ENV };
      repository = {
        update: jest.fn(),
        increment: jest.fn(),
        delete: jest.fn().mockResolvedValue({ affected: 0 }),
      };
      apns = { send: jest.fn().mockResolvedValue("sent") };
      service = new PushService(
        repository as unknown as Repository<PushSubscription>,
        apns as unknown as ApnsClient,
      );
    });

    it("sends a kind the device switched on, through APNs only", async () => {
      await expect(service.send(device, wait)).resolves.toBe(true);
      expect(apns.send).toHaveBeenCalledWith(
        device.apnsToken,
        expect.objectContaining({
          body: expect.stringContaining('"ride-wait"'),
        }),
      );
      expect(webpush.sendNotification).not.toHaveBeenCalled();
    });

    it("never sends a kind the device switched off", async () => {
      const reopen = { ...wait, kind: "ride-reopen" as const };
      const show = { title: "t", body: "b", url: "/", tag: "show-start:1:x" };
      await expect(service.send(device, reopen)).resolves.toBe(false);
      await expect(service.send(device, show)).resolves.toBe(false);
      const plan = {
        title: "t",
        body: "b",
        url: "/",
        tag: "plan-change:p:2026-10-17",
      };
      await expect(service.send(device, plan)).resolves.toBe(false);
      // A device with `news` that never asked for `guides` (an older app).
      const guide = { ...wait, tag: "guides:x", kind: "guides" as const };
      await expect(service.send(device, guide)).resolves.toBe(false);
      await expect(
        service.send({ ...device, topics: [] } as PushSubscription, wait),
      ).resolves.toBe(false);
      expect(apns.send).not.toHaveBeenCalled();
      expect(repository.increment).not.toHaveBeenCalled();
    });

    it("is a no-op without an APNs key, without blaming the row", async () => {
      process.env = { ...ENV, APNS_KEY_ID: "" };
      await expect(service.send(device, wait)).resolves.toBe(false);
      expect(apns.send).not.toHaveBeenCalled();
      expect(repository.increment).not.toHaveBeenCalled();
      expect(repository.delete).not.toHaveBeenCalled();
    });

    it("deletes the device the first time Apple calls its token dead", async () => {
      apns.send.mockResolvedValue("dead");
      await expect(service.send(device, wait)).resolves.toBe(false);
      expect(repository.delete).toHaveBeenCalledWith({ id: "dev-1" });
    });

    it("never blames the row for Apple's or our own bad minute", async () => {
      apns.send.mockResolvedValue("failed");
      for (let i = 0; i < 10; i++) {
        await expect(service.send(device, wait)).resolves.toBe(false);
      }
      expect(repository.increment).not.toHaveBeenCalled();
      expect(repository.delete).not.toHaveBeenCalled();
    });

    it("keeps `kind` out of a browser's payload", async () => {
      process.env = {
        ...ENV,
        VAPID_PUBLIC_KEY: "pub",
        VAPID_PRIVATE_KEY: "priv",
        VAPID_SUBJECT: "mailto:hello@park.fan",
      };
      (webpush.sendNotification as jest.Mock).mockResolvedValue(undefined);
      const browser = {
        id: "b",
        endpoint: "https://fcm.googleapis.com/fcm/send/e1",
        apnsToken: null,
        p256dh: "k",
        auth: "s",
      } as unknown as PushSubscription;
      await service.send(browser, wait);
      const payload = JSON.parse(
        (webpush.sendNotification as jest.Mock).mock.calls[0][1],
      );
      expect(payload).toEqual({
        title: "t",
        body: "b",
        url: "/",
        tag: "ride-alert:1",
      });
    });
  });
});
