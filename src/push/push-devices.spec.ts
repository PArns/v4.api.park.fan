import { generateKeyPairSync } from "crypto";
import { HttpException } from "@nestjs/common";
import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import { Request } from "express";
import { PushController } from "./push.controller";
import { PushDeviceDto } from "./dto/push.dto";
import { PushService } from "./push.service";
import { TripsService } from "../trips/trips.service";
import { PushFollowAccessGuard } from "./push-follow-access.guard";

const PEM = generateKeyPairSync("ec", { namedCurve: "prime256v1" })
  .privateKey.export({ type: "pkcs8", format: "pem" })
  .toString();

const VALID = {
  installId: "6F1C2D3E-4A5B-4C6D-8E7F-0123456789AB",
  token: "AB".repeat(32),
  kinds: ["ride-wait", "news", "bogus"],
  newsParks: ["europe/germany/rust/europa-park"],
  locale: "de-AT",
  timezone: "Europe/Berlin",
};

describe("POST /v1/push/devices", () => {
  const ENV = process.env;
  let registerDevice: jest.Mock;
  let controller: PushController;
  const REQ = { headers: {} } as Request;

  beforeEach(() => {
    process.env = {
      ...ENV,
      APNS_KEY_ID: "ABC123DEFG",
      APNS_TEAM_ID: "KC5QAXDX94",
      APNS_BUNDLE_ID: "fan.park.app",
      APNS_KEY: PEM,
    };
    registerDevice = jest.fn(async (input) => ({
      endpoint: `apns:${input.installId}`,
      topics: input.kinds,
      newsParks: input.newsParks,
    }));
    controller = new PushController(
      { registerDevice } as unknown as PushService,
      {} as TripsService,
      { writeGuard: jest.fn() } as unknown as PushFollowAccessGuard,
    );
  });
  afterAll(() => {
    process.env = ENV;
  });

  const errorsOf = async (body: object) =>
    (await validate(plainToInstance(PushDeviceDto, body))).map(
      (e) => e.property,
    );

  it("accepts a valid body", async () => {
    expect(await errorsOf(VALID)).toEqual([]);
  });

  it.each([
    ["installId", { installId: "not-a-uuid" }],
    ["token", { token: "zz".repeat(32) }],
    ["token", { token: "ab" }],
    ["kinds", { kinds: "news" }],
  ])("refuses a bad %s", async (field, patch) => {
    expect(await errorsOf({ ...VALID, ...patch })).toContain(field);
  });

  it("stores known kinds only, lower-cased ids, and answers the endpoint to use", async () => {
    const answer = await controller.registerDevice(VALID as PushDeviceDto, REQ);
    expect(registerDevice).toHaveBeenCalledWith(
      expect.objectContaining({
        installId: VALID.installId.toLowerCase(),
        token: VALID.token.toLowerCase(),
        kinds: ["ride-wait", "news"],
        newsParks: VALID.newsParks,
        locale: "de-AT",
        timezone: "Europe/Berlin",
      }),
    );
    expect(answer.endpoint).toBe(`apns:${VALID.installId.toLowerCase()}`);
  });

  it("an empty choice is stored, not refused: the master switch is off", async () => {
    await controller.registerDevice(
      { ...VALID, kinds: [] } as PushDeviceDto,
      REQ,
    );
    expect(registerDevice).toHaveBeenCalledWith(
      expect.objectContaining({ kinds: [], newsParks: VALID.newsParks }),
    );
  });

  it("answers 503 and stores nothing without an APNs key", async () => {
    process.env = { ...ENV, APNS_KEY_ID: "" };
    await expect(
      controller.registerDevice(VALID as PushDeviceDto, REQ),
    ).rejects.toBeInstanceOf(HttpException);
    expect(registerDevice).not.toHaveBeenCalled();
    expect(controller.status().apns).toBe(false);
  });
});
