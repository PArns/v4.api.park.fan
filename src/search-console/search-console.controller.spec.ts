import { ConflictException } from "@nestjs/common";
import { generateKeyPairSync } from "crypto";
import { AdminSearchConsoleController } from "./search-console.controller";

function build(redisAnswers: Array<string | null | Error> = ["OK"]) {
  const reports = {
    status: jest.fn(async () => ({ configured: true })),
    queries: jest.fn(async () => ({ items: [] })),
    pages: jest.fn(async () => ({ items: [] })),
    series: jest.fn(async () => ({ series: [] })),
  };
  const queue = { add: jest.fn(async () => ({})) };
  const answers = [...redisAnswers];
  const redis = {
    set: jest.fn(async () => {
      const next = answers.shift() ?? null;
      if (next instanceof Error) throw next;
      return next;
    }),
  };
  const controller = new AdminSearchConsoleController(
    reports as never,
    queue as never,
    redis as never,
  );
  return { controller, reports, queue, redis };
}

function configure() {
  process.env.GSC_SERVICE_ACCOUNT_JSON = JSON.stringify({
    client_email: "gsc@x.iam.gserviceaccount.com",
    private_key: generateKeyPairSync("rsa", {
      modulusLength: 2048,
      publicKeyEncoding: { type: "spki", format: "pem" },
      privateKeyEncoding: { type: "pkcs8", format: "pem" },
    }).privateKey,
  });
  process.env.GSC_SITE_URL = "sc-domain:park.fan";
}

describe("AdminSearchConsoleController", () => {
  const saved = { ...process.env };
  afterEach(() => {
    process.env = { ...saved };
  });

  it("hands the reports their query as it came", async () => {
    const { controller, reports } = build();
    await controller.queries({ days: 7, page: "/de/parks" });
    await controller.pages({ query: "phantasialand" });
    await controller.series({ interval: "week" });
    await controller.status();
    expect(reports.queries).toHaveBeenCalledWith({
      days: 7,
      page: "/de/parks",
    });
    expect(reports.pages).toHaveBeenCalledWith({ query: "phantasialand" });
    expect(reports.series).toHaveBeenCalledWith({ interval: "week" });
    expect(reports.status).toHaveBeenCalled();
  });

  describe("import", () => {
    it("queues the import and answers at once", async () => {
      configure();
      const { controller, queue } = build();
      expect(await controller.importNow()).toEqual({ queued: true });
      expect(queue.add).toHaveBeenCalledWith(
        "import",
        { trigger: "manual" },
        expect.objectContaining({ removeOnComplete: true }),
      );
    });

    it("queues nothing more for a second click within the minute", async () => {
      configure();
      const { controller, queue } = build(["OK", null]);
      await controller.importNow();
      expect(await controller.importNow()).toEqual({ queued: true });
      expect(queue.add).toHaveBeenCalledTimes(1);
    });

    it("queues it when Redis cannot say", async () => {
      configure();
      const { controller, queue } = build([new Error("down")]);
      await controller.importNow();
      expect(queue.add).toHaveBeenCalledTimes(1);
    });

    it("refuses, with the settings to make, when nothing is configured", async () => {
      delete process.env.GSC_SERVICE_ACCOUNT_JSON;
      delete process.env.GSC_SITE_URL;
      const { controller, queue } = build();
      await expect(controller.importNow()).rejects.toBeInstanceOf(
        ConflictException,
      );
      expect(queue.add).not.toHaveBeenCalled();
    });
  });
});
