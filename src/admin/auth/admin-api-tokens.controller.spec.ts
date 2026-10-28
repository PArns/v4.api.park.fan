import {
  BadRequestException,
  ForbiddenException,
  NotFoundException,
} from "@nestjs/common";
import { AdminApiTokensController } from "./admin-api-tokens.controller";
import type { AdminPrincipal } from "./admin-principal";
import type { AdminRole } from "./entities/admin-user.entity";

const SELF = "11111111-1111-4111-8111-111111111111";
const OTHER = "33333333-3333-4333-8333-333333333333";
const TOKEN = "22222222-2222-4222-8222-222222222222";

function admin(role: AdminRole): AdminPrincipal {
  return {
    userId: SELF,
    email: "you@park.fan",
    displayName: "You",
    role,
    sessionToken: "session",
    legacy: false,
    ip: null,
    mustChangePassword: false,
    mustEnrolTotp: false,
  };
}

function build() {
  const tokens = {
    list: jest.fn(async () => []),
    createPersonalToken: jest.fn(async (_user: string, name: string) => ({
      token: "pfa_secret",
      item: {
        id: TOKEN,
        kind: "pat",
        name,
        clientName: null,
        prefix: "pfa_secr",
        scope: "admin:read",
        createdAt: "2026-10-28T00:00:00.000Z",
        lastUsedAt: null,
        expiresAt: null,
      },
    })),
    revoke: jest.fn(async (): Promise<unknown> => null),
  };
  const audit = { record: jest.fn(async () => null) };
  const controller = new AdminApiTokensController(
    tokens as never,
    audit as never,
  );
  return { controller, tokens, audit };
}

describe("AdminApiTokensController", () => {
  it("lists the caller's own tokens", async () => {
    const { controller, tokens } = build();
    await controller.list(admin("viewer"));
    expect(tokens.list).toHaveBeenCalledWith(SELF);
  });

  it("lets an owner list someone else's, and nobody else", async () => {
    const { controller, tokens } = build();
    await controller.list(admin("owner"), OTHER);
    expect(tokens.list).toHaveBeenCalledWith(OTHER);
    await expect(
      controller.list(admin("editor"), OTHER),
    ).rejects.toBeInstanceOf(ForbiddenException);
  });

  it("refuses a userId that is not a uuid before it reaches SQL", async () => {
    const { controller, tokens } = build();
    await expect(
      controller.list(admin("owner"), "1 or 1=1"),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(tokens.list).not.toHaveBeenCalled();
  });

  it("creates a token under a cleaned name and audits it without the secret", async () => {
    const { controller, tokens, audit } = build();
    const created = await controller.create(admin("viewer"), {
      name: "  Laptop‮ ",
      expiresInDays: 90,
    });
    expect(tokens.createPersonalToken).toHaveBeenCalledWith(SELF, "Laptop", 90);
    expect(created.token).toBe("pfa_secret");
    const row = (audit.record.mock.calls[0] as unknown[])[0];
    expect(row).toMatchObject({ action: "auth.token.create", entityId: TOKEN });
    expect(JSON.stringify(row)).not.toContain("pfa_secret");
  });

  it("refuses a name that is only invisible characters", async () => {
    const { controller } = build();
    await expect(
      controller.create(admin("viewer"), { name: "​​" }),
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it("revokes the caller's own token first", async () => {
    const { controller, tokens, audit } = build();
    tokens.revoke.mockResolvedValueOnce({
      id: TOKEN,
      userId: SELF,
      kind: "pat",
      name: "Laptop",
      clientName: null,
      prefix: "pfa_x",
    });
    await controller.revoke(admin("viewer"), TOKEN);
    expect(tokens.revoke).toHaveBeenCalledWith(TOKEN, SELF, "user");
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({ action: "auth.token.revoke", entityId: TOKEN }),
    );
  });

  it("lets only an owner revoke someone else's", async () => {
    const owner = build();
    owner.tokens.revoke.mockResolvedValueOnce(null).mockResolvedValueOnce({
      id: TOKEN,
      userId: OTHER,
      kind: "oauth",
      name: null,
      clientName: "Claude",
      prefix: "pfo_x",
    });
    await owner.controller.revoke(admin("owner"), TOKEN);
    expect(owner.tokens.revoke).toHaveBeenLastCalledWith(TOKEN, null, "owner");

    const editor = build();
    await expect(
      editor.controller.revoke(admin("editor"), TOKEN),
    ).rejects.toBeInstanceOf(NotFoundException);
    expect(editor.tokens.revoke).toHaveBeenCalledTimes(1);
  });
});
