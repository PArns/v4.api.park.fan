import { HttpException, NotFoundException } from "@nestjs/common";
import { Request } from "express";
import { Repository } from "typeorm";
import * as apns from "./apns";
import { APNS_KINDS, ApnsClient, buildApnsRequest } from "./apns";
import { PushController } from "./push.controller";
import { PushService } from "./push.service";
import { PushSubscription } from "./entities/push-subscription.entity";
import { PushFollowAccessGuard } from "./push-follow-access.guard";
import { TripsService } from "../trips/trips.service";
import { PUSH_PICTOGRAMS, withPictogram } from "./push-config";
import {
  PUSH_SELF_TEST_KINDS,
  selfTestMessage,
  type PushSelfTestKind,
} from "./push-self-test";
import * as messages from "./push-messages";
import * as news from "../queues/services/news-push.service";
import { PushDeviceTestDto } from "./dto/push.dto";

/**
 * The app's hidden "Push-Tests": a device asks for a sample of one kind and
 * the server sends it to that device's own token, as the real trigger would.
 */
describe("push self-test", () => {
  const INSTALL = "6F1C2B3A-1111-4222-8333-944455556666";
  const NOW = Date.parse("2026-11-25T13:02:00Z"); // 14:02 in Brühl
  const POSTS = async () => [
    {
      slug: "older",
      title: "Old",
      publishedAt: "2026-11-01T08:00:00Z",
      cover: "img-old",
    },
    {
      slug: "europa-park-winter",
      title: "Europa-Park startet in den Winter",
      teaser: "Ab Samstag.",
      publishedAt: "2026-11-24T08:00:00Z",
      cover: "img-new",
    },
  ];

  describe("selfTestMessage — every kind through its real writer", () => {
    it.each(PUSH_SELF_TEST_KINDS)(
      "%s: the real category, level and pictogram, marked as a test",
      async (kind) => {
        const message = await selfTestMessage(kind, "de", NOW, POSTS);
        const request = buildApnsRequest(withPictogram(message), kind, NOW);
        const payload = JSON.parse(request.body);
        expect(payload.aps.category).toBe(APNS_KINDS[kind].category);
        expect(payload.aps["interruption-level"]).toBe(APNS_KINDS[kind].level);
        expect(payload.aps.alert.title.startsWith(PUSH_PICTOGRAMS[kind])).toBe(
          true,
        );
        expect(payload.kind).toBe(kind);
        expect(payload.test).toBe("true");
        // Its own collapse id: a test never replaces a real banner.
        expect(request.headers["apns-collapse-id"]).toBe(`${kind}:test`);
      },
    );

    it.each<
      [
        PushSelfTestKind,
        (
          | "writeRideAlertMessage"
          | "writeRideReopenMessage"
          | "writeMessage"
          | "writePlanChangeMessage"
          | "writeRainSoonMessage"
          | "writeMorningPlanMessage"
        ),
      ]
    >([
      ["ride-wait", "writeRideAlertMessage"],
      ["ride-reopen", "writeRideReopenMessage"],
      ["next-up", "writeMessage"],
      ["plan-change", "writePlanChangeMessage"],
      ["rain-soon", "writeRainSoonMessage"],
      ["morning-plan", "writeMorningPlanMessage"],
    ])("%s is written by %s", async (kind, writer) => {
      const spy = jest.spyOn(messages, writer);
      await selfTestMessage(kind, "de", NOW, POSTS);
      expect(spy).toHaveBeenCalledTimes(1);
      spy.mockRestore();
    });

    it("news and guides are the newest real post, through writeNewsMessage", async () => {
      const spy = jest.spyOn(news, "writeNewsMessage");
      const m = await selfTestMessage("news", "de", NOW, POSTS);
      expect(spy).toHaveBeenCalledTimes(1);
      expect(m.title).toBe("Europa-Park startet in den Winter");
      expect(m.subtitle).toBe("Parky hat Neues für dich");
      expect(m.url).toBe("/news/europa-park-winter?test=1");
      expect(m.photo).toBe("img-new");
      const g = await selfTestMessage("guides", "de", NOW, POSTS);
      expect(g.url).toBe("/blog/europa-park-winter?test=1");
      spy.mockRestore();
    });

    it("an unreadable export still sends the kind", async () => {
      const m = await selfTestMessage("news", "de", NOW, async () => {
        throw new Error("offline");
      });
      expect(m.kind).toBe("news");
      expect(m.url).toBe("/news/?test=1");
    });

    it("reads in German like the real ones, times from the park's clock", async () => {
      const wait = await selfTestMessage("ride-wait", "de", NOW, POSTS);
      expect([wait.title, wait.subtitle, wait.body]).toEqual([
        "Taron: 15 Min.",
        "Phantasialand",
        "Unter deiner Grenze von 20 Min.",
      ]);
      const next = await selfTestMessage("next-up", "de", NOW, POSTS);
      expect([next.title, next.body]).toEqual([
        "Taron in 10 Min.",
        "Geplant für 14:15 Uhr.",
      ]);
    });

    it("every id an action writes with is a test id", async () => {
      const wait = await selfTestMessage("ride-wait", "de", NOW, POSTS);
      expect(wait.data).toEqual({ attractionId: "test", test: "true" });
      const next = await selfTestMessage("next-up", "de", NOW, POSTS);
      expect(next.data).toEqual({
        entry: "test",
        date: "2026-11-25",
        park: "europe/germany/bruehl/phantasialand",
        ride: "taron",
        test: "true",
      });
      const change = await selfTestMessage("plan-change", "de", NOW, POSTS);
      expect(change.data).toEqual({
        park: "europe/germany/bruehl/phantasialand",
        date: "2026-11-25",
        test: "true",
      });
      expect(change.photo).toBeUndefined();
      expect(change.url).toBe(
        "/trip-planner?park=europe%2Fgermany%2Fbruehl%2Fphantasialand&date=2026-11-25&test=1",
      );
      expect(wait.photo).toBe("europe/germany/bruehl/phantasialand/taron");
    });
  });

  describe("PushService.sendSelfTest", () => {
    let repository: Record<string, jest.Mock>;
    let client: ApnsClient;
    let service: PushService;
    const device = {
      id: "dev-1",
      endpoint: `apns:${INSTALL.toLowerCase()}`,
      apnsToken: "ab".repeat(32),
      apnsEnvironment: null,
      apnsLastError: null,
      topics: [],
      locale: "de",
    } as unknown as PushSubscription;

    beforeEach(() => {
      repository = {
        update: jest.fn(),
        delete: jest.fn().mockResolvedValue({ affected: 1 }),
        increment: jest.fn(),
      };
      client = new ApnsClient();
      service = new PushService(
        repository as unknown as Repository<PushSubscription>,
        client,
      );
    });

    it("skips the device's choice of kinds (it chose none) and keeps the bookkeeping", async () => {
      const send = jest.spyOn(client, "send").mockResolvedValue({
        outcome: "sent",
        status: 200,
        reason: "",
        environment: "sandbox",
      });
      const message = await selfTestMessage("ride-wait", "de", NOW, POSTS);
      const result = await service.sendSelfTest(device, message);
      expect(result.outcome).toBe("sent");
      expect(send.mock.calls[0][0]).toBe(device.apnsToken);
      expect(repository.update).toHaveBeenCalledWith(
        { id: "dev-1" },
        expect.objectContaining({ apnsEnvironment: "sandbox" }),
      );
    });

    it("a dead token is still deleted", async () => {
      jest.spyOn(client, "send").mockResolvedValue({
        outcome: "dead",
        status: 410,
        reason: "Unregistered",
        environment: "production",
      });
      await service.sendSelfTest(
        device,
        await selfTestMessage("news", "de", NOW, POSTS),
      );
      expect(repository.delete).toHaveBeenCalledWith({ id: "dev-1" });
    });

    it("stays time-sensitive past the daily share, and does not use it up", async () => {
      const send = jest.spyOn(client, "send").mockResolvedValue({
        outcome: "sent",
        status: 200,
        reason: "",
        environment: "sandbox",
      });
      const message = await selfTestMessage("ride-wait", "de", NOW, POSTS);
      for (let i = 0; i < apns.TIME_SENSITIVE_CAP + 2; i++) {
        await service.sendSelfTest(device, message);
      }
      const last = JSON.parse(
        send.mock.calls[send.mock.calls.length - 1][1].body,
      );
      expect(last.aps["interruption-level"]).toBe("time-sensitive");
    });
  });

  describe("POST /v1/push/devices/test", () => {
    let controller: PushController;
    let writeGuard: jest.Mock;
    let findByEndpoint: jest.Mock;
    let sendSelfTest: jest.Mock;
    const REQ = { headers: {} } as Request;
    const body = (kind = "ride-wait") =>
      ({ installId: INSTALL, kind }) as PushDeviceTestDto;
    const ENV = process.env;

    beforeEach(() => {
      process.env = { ...ENV };
      delete process.env.PUSH_SELF_TEST;
      writeGuard = jest.fn().mockResolvedValue(undefined);
      findByEndpoint = jest.fn().mockResolvedValue({
        id: "dev-1",
        apnsToken: "ab".repeat(32),
        locale: "de",
      });
      sendSelfTest = jest.fn().mockResolvedValue({
        outcome: "sent",
        status: 200,
        reason: "",
        environment: "sandbox",
      });
      controller = new PushController(
        { findByEndpoint, sendSelfTest } as unknown as PushService,
        {} as TripsService,
        { writeGuard } as unknown as PushFollowAccessGuard,
      );
      jest.spyOn(apns, "isApnsConfigured").mockReturnValue(true);
      global.fetch = jest.fn().mockRejectedValue(new Error("offline"));
    });
    afterEach(() => {
      process.env = ENV;
      jest.restoreAllMocks();
    });

    it("sends to the caller's own row, found by its install id", async () => {
      await expect(controller.testDevice(body(), REQ)).resolves.toEqual({
        state: "sent",
        status: 200,
        environment: "sandbox",
      });
      expect(findByEndpoint).toHaveBeenCalledWith(
        `apns:${INSTALL.toLowerCase()}`,
      );
      expect(sendSelfTest.mock.calls[0][0]).toEqual(
        expect.objectContaining({ id: "dev-1" }),
      );
    });

    it("counts the address and the install before it looks anything up", async () => {
      writeGuard.mockImplementation(async (_r, kind) => {
        if (kind === "push-test-install") {
          throw new HttpException("Too many", 429);
        }
      });
      await expect(controller.testDevice(body(), REQ)).rejects.toThrow(
        HttpException,
      );
      expect(writeGuard).toHaveBeenCalledWith(REQ, "push-test");
      expect(writeGuard).toHaveBeenCalledWith(
        REQ,
        "push-test-install",
        INSTALL.toLowerCase(),
      );
      expect(findByEndpoint).not.toHaveBeenCalled();
    });

    it("an unknown install is told no-token and nothing is sent", async () => {
      findByEndpoint.mockResolvedValue(null);
      await expect(controller.testDevice(body(), REQ)).resolves.toEqual({
        state: "no-token",
      });
      expect(sendSelfTest).not.toHaveBeenCalled();
    });

    it("without a key: not-configured", async () => {
      jest.spyOn(apns, "isApnsConfigured").mockReturnValue(false);
      await expect(controller.testDevice(body(), REQ)).resolves.toEqual({
        state: "not-configured",
      });
    });

    it("passes Apple's refusal on", async () => {
      sendSelfTest.mockResolvedValue({
        outcome: "dead",
        status: 400,
        reason: "BadDeviceToken",
        environment: "production",
      });
      await expect(controller.testDevice(body(), REQ)).resolves.toEqual({
        state: "apns-error",
        status: 400,
        reason: "BadDeviceToken",
        environment: "production",
      });
    });

    it("PUSH_SELF_TEST=false closes it", async () => {
      process.env.PUSH_SELF_TEST = "false";
      await expect(controller.testDevice(body(), REQ)).rejects.toThrow(
        NotFoundException,
      );
      expect(writeGuard).not.toHaveBeenCalled();
    });

    it("writes in the stored language unless one is sent", async () => {
      await controller.testDevice(body(), REQ);
      expect(sendSelfTest.mock.calls[0][1].body).toBe(
        "Unter deiner Grenze von 20 Min.",
      );
      await controller.testDevice({ ...body(), locale: "en" }, REQ);
      expect(sendSelfTest.mock.calls[1][1].body).toBe(
        "Below your 20 min limit.",
      );
    });
  });
});
