import { RedisMock } from "../../../test/mocks/redis.mock";
import { OAuthClientService } from "./oauth-client.service";
import { OAuthError } from "./oauth-error";

function build(unused = 0) {
  const saved: unknown[] = [];
  const repository = {
    delete: jest.fn(async () => ({ affected: 0 })),
    count: jest.fn(async () => unused),
    create: jest.fn((row: Record<string, unknown>) => row),
    save: jest.fn(async (row: Record<string, unknown>) => {
      const stored = { ...row, createdAt: new Date("2026-10-28T08:00:00Z") };
      saved.push(stored);
      return stored;
    }),
    findOne: jest.fn(async () => null),
    find: jest.fn(async () => [{ clientId: "pfc_oldest" }]),
    update: jest.fn(async () => ({ affected: 1 })),
  };
  const service = new OAuthClientService(
    repository as never,
    new RedisMock() as never,
  );
  return { service, repository, saved };
}

async function rejection(promise: Promise<unknown>): Promise<OAuthError> {
  try {
    await promise;
  } catch (error) {
    if (error instanceof OAuthError) return error;
    throw error;
  }
  throw new Error("expected a rejection");
}

describe("OAuthClientService.register", () => {
  beforeEach(() => {
    delete process.env.OAUTH_REDIRECT_HOSTS;
    delete process.env.OAUTH_REDIRECT_PATHS;
  });

  it("registers a public client and says so whatever was asked for", async () => {
    const { service } = build();
    const answer = await service.register(
      {
        client_name: "Claude Code (parkfan-admin)",
        redirect_uris: ["http://localhost:33418/callback"],
        grant_types: ["authorization_code", "refresh_token"],
        response_types: ["code"],
        token_endpoint_auth_method: "client_secret_post",
      },
      "198.51.100.7",
    );
    expect(answer).toMatchObject({
      client_name: "Claude Code (parkfan-admin)",
      redirect_uris: ["http://localhost:33418/callback"],
      token_endpoint_auth_method: "none",
      grant_types: ["authorization_code", "refresh_token"],
      scope: "admin:read",
    });
    expect(answer.client_id).toMatch(/^pfc_[\w-]{22}$/);
    expect(answer).not.toHaveProperty("client_secret");
  });

  it("refuses a redirect off the allowed hosts", async () => {
    const { service, repository } = build();
    const error = await rejection(
      service.register({ redirect_uris: ["https://evil.example/cb"] }, null),
    );
    expect(error.code).toBe("invalid_redirect_uri");
    expect(repository.save).not.toHaveBeenCalled();
  });

  it.each([
    [{}, "invalid_redirect_uri"],
    [{ redirect_uris: [] }, "invalid_redirect_uri"],
    [
      { redirect_uris: Array(11).fill("http://localhost/cb") },
      "invalid_redirect_uri",
    ],
    [
      {
        redirect_uris: ["http://localhost/cb"],
        grant_types: ["client_credentials"],
      },
      "invalid_client_metadata",
    ],
    [
      {
        redirect_uris: ["http://localhost/cb"],
        grant_types: ["refresh_token"],
      },
      "invalid_client_metadata",
    ],
    [
      { redirect_uris: ["http://localhost/cb"], response_types: ["token"] },
      "invalid_client_metadata",
    ],
    [
      { redirect_uris: ["http://localhost/cb"], client_name: 7 },
      "invalid_client_metadata",
    ],
  ])("refuses %j", async (body, code) => {
    const { service } = build();
    expect((await rejection(service.register(body, null))).code).toBe(code);
  });

  it("refuses a body that is not an object", async () => {
    const { service } = build();
    expect((await rejection(service.register([], null))).code).toBe(
      "invalid_client_metadata",
    );
  });

  it("strips control characters from the name and bounds it", async () => {
    const { service } = build();
    const answer = await service.register(
      {
        client_name: `Claude${String.fromCodePoint(0x00, 0x202e)}${"x".repeat(200)}`,
        redirect_uris: ["http://localhost/cb"],
      },
      null,
    );
    expect(answer.client_name.startsWith("Claude")).toBe(true);
    expect(answer.client_name).not.toContain(String.fromCodePoint(0x00));
    expect(answer.client_name).not.toContain(String.fromCodePoint(0x202e));
    expect(answer.client_name.length).toBeLessThanOrEqual(100);
  });

  it("limits registrations per address", async () => {
    const { service } = build();
    const body = { redirect_uris: ["http://localhost/cb"] };
    for (let i = 0; i < 10; i++) await service.register(body, "198.51.100.7");
    const error = await rejection(service.register(body, "198.51.100.7"));
    expect(error.status).toBe(429);
    await expect(service.register(body, "203.0.113.9")).resolves.toBeDefined();
  });

  it("makes room by deleting the oldest unused registrations past their first hour", async () => {
    const { service, repository } = build(1000);
    await expect(
      service.register({ redirect_uris: ["http://localhost/cb"] }, null),
    ).resolves.toBeDefined();
    const [query] = repository.find.mock.calls[0] as unknown as [
      { where: { createdAt: { value: Date } }; order: unknown; take: number },
    ];
    expect(query).toMatchObject({ order: { createdAt: "ASC" }, take: 1 });
    // Nothing registered within the last hour is a candidate.
    expect(query.where.createdAt.value.getTime()).toBeLessThanOrEqual(
      Date.now() - 60 * 60 * 1000 + 1000,
    );
    expect(repository.delete).toHaveBeenCalledTimes(2);
  });

  it("trims a registrant's own surplus first, whoever else is waiting", async () => {
    const { service, repository } = build(10);
    await service.register(
      { redirect_uris: ["http://localhost/cb"] },
      "198.51.100.7",
    );
    expect(repository.find).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({ registrant: expect.any(String) }),
        order: { createdAt: "DESC" },
        skip: 19,
      }),
    );
    expect(repository.save).toHaveBeenCalledWith(
      expect.objectContaining({
        registrant: expect.stringMatching(/^[0-9a-f]{32}$/),
      }),
    );
  });

  it("refuses only when the hard bound is full of young registrations", async () => {
    const { service, repository } = build(5000);
    repository.find.mockResolvedValue([]);
    const error = await rejection(
      service.register({ redirect_uris: ["http://localhost/cb"] }, null),
    );
    expect(error).toMatchObject({
      code: "temporarily_unavailable",
      status: 429,
    });
  });

  it("deletes stale registrations nobody used before counting", async () => {
    const { service, repository } = build();
    await service.register({ redirect_uris: ["http://localhost/cb"] }, null);
    expect(repository.delete).toHaveBeenCalledTimes(1);
  });
});

describe("OAuthClientService.find", () => {
  it("never queries for a value that is not a client id", async () => {
    const { service, repository } = build();
    expect(await service.find("'; drop table x; --")).toBeNull();
    expect(await service.find(undefined)).toBeNull();
    expect(repository.findOne).not.toHaveBeenCalled();
  });
});
