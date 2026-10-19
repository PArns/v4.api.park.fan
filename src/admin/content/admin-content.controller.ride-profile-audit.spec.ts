import { ValidationPipe } from "@nestjs/common";
import { validate } from "class-validator";
import { plainToInstance } from "class-transformer";
import { AdminContentController } from "./admin-content.controller";
import {
  RideProfileProvenanceDto,
  RideProfileWriteDto,
} from "./dto/curation.dto";

const ADMIN = { email: "legacy-pass" } as never;
const SOURCE = "https://www.rcdb.com/16847.htm";

function build() {
  const audit = { record: jest.fn(async () => undefined) };
  const rideProfiles = {
    find: jest.fn(async () => null),
    upsert: jest.fn(async () => ({
      profile: { manufacturerName: "Gerstlauer", elements: [], types: [] },
      unknownTermIds: [],
    })),
    remove: jest.fn(async () => true),
  };
  const curation = {
    findAttraction: jest.fn(async () => ({
      name: "Van Helsing's Factory",
      parkId: "park-1",
    })),
    publish: jest.fn(async () => undefined),
  };
  const controller = new AdminContentController(
    {} as never,
    {} as never,
    curation as never,
    rideProfiles as never,
    {} as never,
    audit as never,
  );
  return { controller, audit, rideProfiles };
}

describe("ride-profile audit provenance", () => {
  it("writes reason and sourceUrl into the ride-profile.write audit row", async () => {
    const { controller, audit } = build();

    await controller.putRideProfile(
      "ride-1",
      {
        manufacturerName: "Gerstlauer",
        reason: "Maker confirmed",
        sourceUrl: SOURCE,
      },
      ADMIN,
    );

    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "ride-profile.write",
        reason: "Maker confirmed",
        sourceUrl: SOURCE,
      }),
    );
  });

  it("does not hand the provenance fields to the profile upsert", async () => {
    const { controller, rideProfiles } = build();

    await controller.putRideProfile(
      "ride-1",
      { manufacturerName: "Gerstlauer", reason: "x", sourceUrl: SOURCE },
      ADMIN,
    );

    expect(rideProfiles.upsert).toHaveBeenCalledWith("ride-1", {
      manufacturerName: "Gerstlauer",
    });
  });

  it("writes reason and sourceUrl into the ride-profile.delete audit row", async () => {
    const { controller, audit } = build();

    await controller.deleteRideProfile("ride-1", ADMIN, {
      reason: "Duplicate profile",
      sourceUrl: SOURCE,
    });

    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "ride-profile.delete",
        reason: "Duplicate profile",
        sourceUrl: SOURCE,
      }),
    );
  });

  it("still deletes when the request carries no body", async () => {
    const { controller, audit } = build();

    await controller.deleteRideProfile("ride-1", ADMIN, undefined);

    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "ride-profile.delete",
        reason: undefined,
        sourceUrl: undefined,
      }),
    );
  });

  it("rejects a sourceUrl that is not an http(s) URL and accepts an absent one", async () => {
    const bad = plainToInstance(RideProfileWriteDto, { sourceUrl: "rcdb" });
    const ok = plainToInstance(RideProfileWriteDto, { manufacturerName: "X" });
    const good = plainToInstance(RideProfileWriteDto, { sourceUrl: SOURCE });

    expect((await validate(bad)).map((e) => e.property)).toEqual(["sourceUrl"]);
    expect(await validate(ok)).toHaveLength(0);
    expect(await validate(good)).toHaveLength(0);
  });

  describe("through the global ValidationPipe", () => {
    const pipe = new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    });
    const meta = {
      type: "body" as const,
      metatype: RideProfileProvenanceDto,
    };

    it("accepts a DELETE without a body", async () => {
      await expect(pipe.transform({}, meta)).resolves.toEqual({});
    });

    it("treats an empty sourceUrl as absent", async () => {
      const out = await pipe.transform({ sourceUrl: "" }, meta);
      expect(out.sourceUrl).toBeUndefined();
    });

    it("rejects a malformed sourceUrl with a 400", async () => {
      await expect(
        pipe.transform({ sourceUrl: "rcdb" }, meta),
      ).rejects.toThrow();
    });
  });
});
