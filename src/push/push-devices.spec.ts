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
  kinds: ["ride-wait", "news", "plan-change", "bogus"],
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

  it("accepts a trip id or null, refuses one that cannot be an id", async () => {
    expect(await errorsOf({ ...VALID, tripId: "AbC_d-12" })).toEqual([]);
    expect(await errorsOf({ ...VALID, tripId: null })).toEqual([]);
    expect(await errorsOf({ ...VALID, tripId: "../x" })).toContain("tripId");
  });

  it("passes the trip pointer on as sent: id, null or omitted", async () => {
    await controller.registerDevice(
      { ...VALID, tripId: "t1" } as PushDeviceDto,
      REQ,
    );
    expect(registerDevice).toHaveBeenLastCalledWith(
      expect.objectContaining({ tripId: "t1" }),
    );
    await controller.registerDevice(
      { ...VALID, tripId: null } as PushDeviceDto,
      REQ,
    );
    expect(registerDevice).toHaveBeenLastCalledWith(
      expect.objectContaining({ tripId: null }),
    );
    await controller.registerDevice(VALID as PushDeviceDto, REQ);
    expect(registerDevice).toHaveBeenLastCalledWith(
      expect.objectContaining({ tripId: undefined }),
    );
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
        kinds: ["ride-wait", "news", "plan-change"],
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

describe("PushService.registerDevice — the trip pointer", () => {
  const ENV = process.env;
  let rows: Map<string, Record<string, unknown>>;
  let trips: Map<string, { id: string; expiresAt: Date }>;
  let service: PushService;

  beforeEach(() => {
    process.env = {
      ...ENV,
      APNS_KEY_ID: "ABC123DEFG",
      APNS_TEAM_ID: "KC5QAXDX94",
      APNS_BUNDLE_ID: "fan.park.app",
      APNS_KEY: PEM,
    };
    rows = new Map();
    trips = new Map([
      ["t1", { id: "t1", expiresAt: new Date(Date.now() + 86_400_000) }],
      ["old", { id: "old", expiresAt: new Date(Date.now() - 1) }],
    ]);
    const repository = {
      findOne: jest.fn(async ({ where }) => rows.get(where.endpoint) ?? null),
      create: jest.fn((row) => ({ ...row })),
      delete: jest.fn(async () => ({ affected: 0 })),
      save: jest.fn(async (row) => {
        rows.set(row.endpoint, row);
        return row;
      }),
      manager: {
        transaction: jest.fn(async (fn) =>
          fn({
            findOne: jest.fn(async (_entity, { where, lock }) => {
              expect(lock).toEqual({ mode: "pessimistic_write" });
              return trips.get(where.id) ?? null;
            }),
            getRepository: () => repository,
          }),
        ),
      },
    };
    service = new PushService(repository as never, {} as never);
  });
  afterAll(() => {
    process.env = ENV;
  });

  const register = (tripId?: string | null) =>
    service.registerDevice({
      installId: "6f1c2d3e-4a5b-4c6d-8e7f-0123456789ab",
      token: "ab".repeat(32),
      kinds: ["plan-change"],
      newsParks: null,
      ...(tripId !== undefined && { tripId }),
    });

  it("links a live trip, keeps it when omitted, clears it with null", async () => {
    expect((await register("t1"))?.tripId).toBe("t1");
    expect((await register())?.tripId).toBe("t1");
    expect((await register(null))?.tripId).toBeNull();
  });

  it("answers 404 for a trip that does not exist or expired", async () => {
    await expect(register("nope")).rejects.toThrow("Trip not found");
    await expect(register("old")).rejects.toThrow("Trip not found");
    expect(rows.size).toBe(0);
  });
});
