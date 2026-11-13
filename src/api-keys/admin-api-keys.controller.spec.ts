import "reflect-metadata";
import { ConflictException, NotFoundException } from "@nestjs/common";
import { ADMIN_MIN_ROLE_KEY } from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { AdminApiKeysController } from "./admin-api-keys.controller";

const ID = "44444444-4444-4444-8444-444444444444";
const NEW_KEY = `pfk_${"n".repeat(43)}`;

const owner: AdminPrincipal = {
  userId: "11111111-1111-4111-8111-111111111111",
  email: "owner@park.fan",
  displayName: "Owner",
  role: "owner",
  sessionToken: "session",
  legacy: false,
  ip: null,
  mustChangePassword: false,
  mustEnrolTotp: false,
};

const item = {
  id: ID,
  name: "Coaster club",
  prefix: NEW_KEY.slice(0, 10),
  previousExpiresAt: new Date("2026-11-14T09:00:00.000Z"),
};

function build() {
  const apiKeys = {
    rotate: jest.fn(async (): Promise<unknown> => ({
      key: NEW_KEY,
      item,
      previousPrefix: "pfk_oooooo",
    })),
    revokePrevious: jest.fn(async (): Promise<unknown> => ({
      item: { ...item, previousExpiresAt: null },
      previousExpiresAt: item.previousExpiresAt,
    })),
  };
  const audit = { record: jest.fn(async () => null) };
  const controller = new AdminApiKeysController(
    apiKeys as never,
    audit as never,
  );
  return { controller, apiKeys, audit };
}

describe("AdminApiKeysController rotation", () => {
  it("is owner only, like issuing a key", () => {
    expect(
      Reflect.getMetadata(ADMIN_MIN_ROLE_KEY, AdminApiKeysController),
    ).toBe("owner");
  });

  it("answers the new key once, as create does, and audits no secret", async () => {
    const { controller, apiKeys, audit } = build();
    const answer = await controller.rotate(owner, ID, { graceHours: 24 });

    expect(apiKeys.rotate).toHaveBeenCalledWith(ID, 24);
    expect(answer).toEqual({ key: NEW_KEY, item });
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "api-key.rotate",
        entityType: "api_key",
        entityId: ID,
        before: { prefix: "pfk_oooooo" },
        after: {
          prefix: item.prefix,
          graceHours: 24,
          previousExpiresAt: item.previousExpiresAt,
        },
      }),
    );
    expect(JSON.stringify(audit.record.mock.calls)).not.toContain(NEW_KEY);
  });

  it("answers 404 for no such key and passes the 409 of a revoked one on", async () => {
    const { controller, apiKeys, audit } = build();
    apiKeys.rotate.mockResolvedValueOnce(null);
    await expect(
      controller.rotate(owner, ID, { graceHours: 0 }),
    ).rejects.toBeInstanceOf(NotFoundException);
    apiKeys.rotate.mockRejectedValueOnce(
      new ConflictException("A revoked key cannot be rotated"),
    );
    await expect(
      controller.rotate(owner, ID, { graceHours: 0 }),
    ).rejects.toBeInstanceOf(ConflictException);
    await expect(
      controller.rotate(owner, "not-a-uuid", { graceHours: 0 }),
    ).rejects.toThrow("id must be a uuid");
    expect(audit.record).not.toHaveBeenCalled();
  });

  it("ends a grace period with an audit row, and 404 when none runs", async () => {
    const { controller, apiKeys, audit } = build();
    await controller.revokePrevious(owner, ID);
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "api-key.previous.revoke",
        entityId: ID,
        before: { previousExpiresAt: item.previousExpiresAt },
        after: { previousExpiresAt: null },
      }),
    );

    apiKeys.revokePrevious.mockResolvedValueOnce(null);
    await expect(controller.revokePrevious(owner, ID)).rejects.toBeInstanceOf(
      NotFoundException,
    );
  });
});
