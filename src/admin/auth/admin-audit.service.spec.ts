import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import { VerifyFieldsDto } from "../content/dto/curation.dto";
import { AdminAuditService, auditEntryKind } from "./admin-audit.service";
import type { AdminPrincipal } from "./admin-principal";

describe("auditEntryKind", () => {
  it("calls a verify a verification and everything else a change", () => {
    expect(auditEntryKind("park.verify")).toBe("verification");
    expect(auditEntryKind("park.curate")).toBe("change");
    expect(auditEntryKind("attraction.curate")).toBe("change");
    expect(auditEntryKind("park.season.update")).toBe("change");
  });
});

describe("AdminAuditService.list", () => {
  it("labels every entry with its kind, so the history can tell a check from an edit", async () => {
    const rows = [
      { id: "a", action: "park.verify", before: { x: 1 }, after: null },
      { id: "b", action: "park.curate", before: { x: 0 }, after: { x: 1 } },
    ];
    const qb = {
      orderBy: jest.fn().mockReturnThis(),
      take: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      andWhere: jest.fn().mockReturnThis(),
      getManyAndCount: jest.fn(async () => [rows, 2]),
    };
    const repository = { createQueryBuilder: jest.fn(() => qb) };
    const service = new AdminAuditService(repository as never);

    const { entries, total } = await service.list({ entityType: "park" });

    expect(total).toBe(2);
    expect(entries.map((entry) => [entry.id, entry.kind])).toEqual([
      ["a", "verification"],
      ["b", "change"],
    ]);
  });
});

describe("AdminAuditService.record", () => {
  const principal = (ip: string | null): AdminPrincipal => ({
    userId: "6f1c6a52-0f6e-4f43-9d0e-9f4e1d6c2a10",
    email: "editor@example.com",
    displayName: "Editor",
    role: "editor",
    sessionToken: "token",
    legacy: false,
    ip,
    mustChangePassword: false,
    mustEnrolTotp: false,
  });

  /** What the repository was handed to store for one `record` call. */
  const stored = async (ip: string | null) => {
    const repository = {
      create: jest.fn((row: Record<string, unknown>) => row),
      save: jest.fn(async (row: Record<string, unknown>) => row),
    };
    const service = new AdminAuditService(repository as never);
    await service.record({
      actor: principal(ip),
      action: "park.curate",
      entityType: "park",
    });
    return repository.save.mock.calls[0][0];
  };

  it.each([
    ["203.0.113.47", "203.0.113.0/24"],
    ["::ffff:203.0.113.47", "203.0.113.0/24"],
    ["2001:db8:1:2::9", "2001:db8:1::/48"],
  ])("stores the network of %s, never the address", async (ip, network) => {
    const row = await stored(ip);

    expect(row.actorIp).toBe(network);
    expect(JSON.stringify(row)).not.toContain(ip);
  });

  it("stores nothing for a caller with no address or text that is none", async () => {
    expect((await stored(null)).actorIp).toBeNull();
    expect((await stored("not-an-address")).actorIp).toBeNull();
  });
});

describe("VerifyFieldsDto", () => {
  const errorsFor = async (body: Record<string, unknown>) =>
    (await validate(plainToInstance(VerifyFieldsDto, body))).map(
      (error) => error.property,
    );

  it("accepts fields with a sourceUrl", async () => {
    expect(
      await errorsFor({
        fields: ["curatedStreetAddress"],
        sourceUrl: "https://www.heide-park.de/",
      }),
    ).toEqual([]);
  });

  it("rejects a body without a sourceUrl", async () => {
    expect(await errorsFor({ fields: ["curatedStreetAddress"] })).toEqual([
      "sourceUrl",
    ]);
    expect(
      await errorsFor({ fields: ["curatedStreetAddress"], sourceUrl: "" }),
    ).toEqual(["sourceUrl"]);
  });

  it("rejects an empty or non-list fields", async () => {
    const sourceUrl = "https://www.heide-park.de/";
    expect(await errorsFor({ fields: [], sourceUrl })).toEqual(["fields"]);
    expect(
      await errorsFor({ fields: "curatedStreetAddress", sourceUrl }),
    ).toEqual(["fields"]);
  });
});
