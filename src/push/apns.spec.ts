import { createPublicKey, generateKeyPairSync, verify } from "crypto";
import { Repository } from "typeorm";
import * as webpush from "web-push";
import {
  ApnsClient,
  apnsJwt,
  APNS_KINDS,
  APNS_MAX_PAYLOAD_BYTES,
  TIME_SENSITIVE_CAP,
  TIME_SENSITIVE_WINDOW_MS,
  buildApnsRequest,
  classifyApnsResponse,
  getApnsConfig,
} from "./apns";
import { DEVICE_PUSH_KINDS, pushKindOf } from "./push-config";
import { PushService } from "./push.service";
import { resetPushMediaCache } from "./push-media";
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
      expect(getApnsConfig()?.first).toBe("production");
      process.env = { ...ENV, ...APNS_ENV, APNS_PRODUCTION: "false" };
      expect(getApnsConfig()?.first).toBe("sandbox");
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
      title: "Taron: 15 Min.",
      subtitle: "Phantasialand",
      body: "Unter deiner Grenze von 20 Min.",
      url: "/parks/europe/germany/bruehl/phantasialand/taron",
      tag: "ride-alert:3f2c",
      threadId: "phantasialand:2026-10-17",
      data: { attractionId: "a-1" },
    };
    const NOW = 1_000_000;
    const build = (kind: string, extra: object = {}, extras = {}) => {
      const request = buildApnsRequest(
        { ...message, ...extra },
        kind,
        NOW,
        extras,
      );
      return { headers: request.headers, payload: JSON.parse(request.body) };
    };

    it("puts every key where the app reads it", () => {
      const { headers, payload } = build(
        "ride-wait",
        {},
        { image: "https://media.park.fan/taron/01/abc-1200.webp" },
      );
      expect(payload).toEqual({
        aps: {
          alert: {
            title: message.title,
            subtitle: message.subtitle,
            body: message.body,
          },
          sound: "default",
          "thread-id": "phantasialand:2026-10-17",
          "interruption-level": "time-sensitive",
          "relevance-score": 0.9,
          category: "RIDE_WAIT",
          "mutable-content": 1,
          "target-content-id": message.url,
        },
        attractionId: "a-1",
        url: message.url,
        kind: "ride-wait",
        image: "https://media.park.fan/taron/01/abc-1200.webp",
      });
      expect(headers).toEqual({
        "apns-push-type": "alert",
        "apns-priority": "10",
        "apns-expiration": String(1000 + 600),
        "apns-collapse-id": "ride-alert:3f2c",
      });
    });

    it("asks for the service extension only when there is a photo", () => {
      const { payload } = build("ride-wait");
      expect(payload.aps).not.toHaveProperty("mutable-content");
      expect(payload).not.toHaveProperty("image");
    });

    it("stacks with its kind and has no subtitle when the message has none", () => {
      const { payload } = build("news", {
        threadId: undefined,
        subtitle: undefined,
      });
      expect(payload.aps["thread-id"]).toBe("news");
      expect(payload.aps.alert).toEqual({
        title: message.title,
        body: message.body,
      });
    });

    // The level decides whether a Focus is broken through: pinned per kind.
    it.each([
      ["ride-wait", "time-sensitive", "RIDE_WAIT", 0.9, "10", 600],
      ["ride-reopen", "time-sensitive", "RIDE_REOPEN", 0.8, "10", 1200],
      ["next-up", "time-sensitive", "NEXT_UP", 1, "10", 900],
      ["show", "time-sensitive", "SHOW", 0.8, "10", 900],
      ["rain-soon", "time-sensitive", "RAIN_SOON", 0.7, "10", 900],
      ["plan-change", "active", "PLAN_CHANGE", 0.6, "10", 3600],
      ["morning-plan", "active", "MORNING_PLAN", 0.5, "10", 3600],
      ["news", "passive", "NEWS", 0.2, "5", 43_200],
      ["guides", "passive", "GUIDE", 0.1, "5", 43_200],
    ])("%s is %s", (kind, interruption, category, relevance, priority, ttl) => {
      const { headers, payload } = build(kind);
      expect(payload.aps["interruption-level"]).toBe(interruption);
      expect(payload.aps.category).toBe(category);
      expect(payload.aps["relevance-score"]).toBe(relevance);
      expect(headers["apns-priority"]).toBe(priority);
      expect(headers["apns-expiration"]).toBe(String(1000 + ttl));
      // A passive notification makes no sound.
      expect("sound" in payload.aps).toBe(interruption !== "passive");
    });

    it("covers every kind a device can choose", () => {
      for (const kind of DEVICE_PUSH_KINDS) {
        expect(APNS_KINDS[kind]).toBeDefined();
      }
      expect(Object.keys(APNS_KINDS).sort()).toEqual(
        [...DEVICE_PUSH_KINDS].sort(),
      );
    });

    it("never lets news, guides or a plan change break through a Focus", () => {
      for (const kind of ["news", "guides", "plan-change", "morning-plan"]) {
        expect(APNS_KINDS[kind].level).not.toBe("time-sensitive");
      }
    });

    it("sends the admin's test as an ordinary alert without a category", () => {
      const { payload } = build("test");
      expect(payload.aps["interruption-level"]).toBe("active");
      expect(payload.aps).not.toHaveProperty("category");
    });

    it("expires when the message says its moment is over", () => {
      const start = NOW + 12 * 60_000;
      expect(
        build("next-up", { expiresAtMs: start }).headers["apns-expiration"],
      ).toBe(String(start / 1000));
    });

    it("never expires at once: APNs would not store it for a phone in a tunnel", () => {
      expect(
        build("next-up", { expiresAtMs: NOW - 5000 }).headers[
          "apns-expiration"
        ],
      ).toBe(String(1000 + 60));
    });

    it("delivers a time-sensitive kind as active once the device had its share", () => {
      const { payload } = build("ride-wait", {}, { calm: true });
      expect(payload.aps["interruption-level"]).toBe("active");
      expect(build("news", {}, { calm: true }).payload.aps).toMatchObject({
        "interruption-level": "passive",
      });
    });

    it("cuts the body, and only the body, to stay under Apple's 4 KB", () => {
      const request = buildApnsRequest(
        { ...message, body: "ä".repeat(5000) },
        "news",
        NOW,
      );
      expect(Buffer.byteLength(request.body)).toBeLessThanOrEqual(
        APNS_MAX_PAYLOAD_BYTES,
      );
      const payload = JSON.parse(request.body);
      expect(payload.aps.alert.title).toBe(message.title);
      expect(payload.aps.alert.body).toMatch(/^ä+…$/);
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
        await expect(client.send("ab", request)).resolves.toMatchObject({
          outcome: "failed",
        });
      }
      expect(post).toHaveBeenCalledTimes(ApnsClient.BREAKER_FAILURES);
      // Open: no request at all.
      await expect(client.send("ab", request)).resolves.toMatchObject({
        outcome: "failed",
      });
      expect(post).toHaveBeenCalledTimes(ApnsClient.BREAKER_FAILURES);
      // After the pause Apple is asked again, and an answer closes it.
      clock += ApnsClient.BREAKER_OPEN_MS;
      post.mockResolvedValue({ status: 200, reason: "" });
      await expect(client.send("ab", request)).resolves.toMatchObject({
        outcome: "sent",
      });
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

    const answer = (outcome: "sent" | "dead" | "failed") => ({
      outcome,
      environment: "production",
      status: outcome === "sent" ? 200 : 400,
      reason: "",
    });

    beforeEach(() => {
      process.env = { ...ENV, ...APNS_ENV };
      repository = {
        update: jest.fn(),
        increment: jest.fn(),
        delete: jest.fn().mockResolvedValue({ affected: 0 }),
      };
      apns = { send: jest.fn().mockResolvedValue(answer("sent")) };
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
        null,
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
      apns.send.mockResolvedValue(answer("dead"));
      await expect(service.send(device, wait)).resolves.toBe(false);
      expect(repository.delete).toHaveBeenCalledWith({ id: "dev-1" });
    });

    it("never blames the row for Apple's or our own bad minute", async () => {
      apns.send.mockResolvedValue(answer("failed"));
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

    it("gives a browser the place as the first line of the body, and nothing it cannot read", async () => {
      process.env = {
        ...ENV,
        VAPID_PUBLIC_KEY: "pub",
        VAPID_PRIVATE_KEY: "priv",
        VAPID_SUBJECT: "mailto:hello@park.fan",
      };
      const sendNotification = webpush.sendNotification as jest.Mock;
      sendNotification.mockReset().mockResolvedValue(undefined);
      const browser = {
        id: "b",
        endpoint: "https://fcm.googleapis.com/fcm/send/e1",
        apnsToken: null,
        p256dh: "k",
        auth: "s",
      } as unknown as PushSubscription;
      const rich = {
        ...wait,
        subtitle: "Phantasialand",
        photo: "taron/01",
        expiresAtMs: 1,
        threadId: "phantasialand:2026-10-17",
        data: { attractionId: "a" },
      };
      await service.send(browser, rich);
      await service.send(browser, { ...rich, body: "" });
      const payloads = sendNotification.mock.calls.map((c) => JSON.parse(c[1]));
      expect(payloads).toEqual([
        { title: "t", body: "Phantasialand\nb", url: "/", tag: "ride-alert:1" },
        { title: "t", body: "Phantasialand", url: "/", tag: "ride-alert:1" },
      ]);
    });

    describe("the photo and the Focus", () => {
      const realFetch = global.fetch;
      const payloadOf = (call: number) =>
        JSON.parse(apns.send.mock.calls[call][1].body);
      beforeEach(() => {
        resetPushMediaCache();
        global.fetch = jest.fn().mockResolvedValue({
          ok: true,
          json: async () => ({
            rides: { "europe/germany/bruehl/phantasialand/taron": "taron/01" },
            images: {
              "taron/01": { v: "ab12", widths: [1200], attribution: false },
            },
          }),
        }) as unknown as typeof fetch;
      });
      afterAll(() => {
        global.fetch = realFetch;
      });

      it("attaches the ride's photo and sends without one when there is none", async () => {
        await service.send(device, {
          ...wait,
          photo: "europe/germany/bruehl/phantasialand/taron",
        });
        await service.send(device, { ...wait, photo: "europe/x/y/z/none" });
        await service.send(device, wait);
        expect(payloadOf(0)).toMatchObject({
          image: "https://media.park.fan/taron/01/ab12-1200.webp",
          aps: { "mutable-content": 1 },
        });
        expect(payloadOf(1)).not.toHaveProperty("image");
        expect(payloadOf(2).aps).not.toHaveProperty("mutable-content");
        expect(global.fetch).toHaveBeenCalledTimes(1);
      });

      it("breaks through a Focus at most TIME_SENSITIVE_CAP times in the window, then arrives as active", async () => {
        const now = jest.spyOn(Date, "now").mockReturnValue(1_000_000);
        try {
          for (let i = 0; i <= TIME_SENSITIVE_CAP; i++) {
            await service.send(device, wait);
          }
          const levels = apns.send.mock.calls.map(
            (_, i) => payloadOf(i).aps["interruption-level"],
          );
          expect(levels).toEqual([
            ...Array(TIME_SENSITIVE_CAP).fill("time-sensitive"),
            "active",
          ]);
          // Another device has its own share; a passive kind never counts.
          await service.send(
            { ...device, id: "dev-2" } as PushSubscription,
            wait,
          );
          expect(payloadOf(levels.length).aps["interruption-level"]).toBe(
            "time-sensitive",
          );
          now.mockReturnValue(1_000_000 + TIME_SENSITIVE_WINDOW_MS);
          await service.send(device, wait);
          expect(payloadOf(levels.length + 1).aps["interruption-level"]).toBe(
            "time-sensitive",
          );
        } finally {
          now.mockRestore();
        }
      });
    });
  });
});
