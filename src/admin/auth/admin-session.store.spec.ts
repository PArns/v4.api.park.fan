import { AdminSessionStore } from "./admin-session.store";
import { RedisMock } from "../../../test/mocks/redis.mock";

describe("AdminSessionStore", () => {
  let redis: RedisMock;
  let store: AdminSessionStore;
  let now: number;

  const baseSession = {
    userId: "user-1",
    email: "you@park.fan",
    displayName: "You",
    role: "editor" as const,
    ip: "203.0.113.0/24",
    userAgent: "jest",
    mustChangePassword: false,
    mustEnrolTotp: false,
  };

  beforeEach(() => {
    now = Date.UTC(2026, 7, 20, 9, 0, 0);
    redis = new RedisMock();
    redis.now = () => now;
    jest.spyOn(Date, "now").mockImplementation(() => now);
    store = new AdminSessionStore(redis as never);
  });

  afterEach(() => jest.restoreAllMocks());

  it("issues a token that resolves back to the session", async () => {
    const { token, session } = await store.create(baseSession);
    expect(token).toHaveLength(43); // 32 bytes, base64url, unpadded
    expect(session.role).toBe("editor");

    const resolved = await store.resolve(token);
    expect(resolved?.userId).toBe("user-1");
    expect(resolved?.email).toBe("you@park.fan");
  });

  it("never stores the token itself", async () => {
    const { token } = await store.create(baseSession);
    // The key is the SHA-256 of the token; a Redis dump must not hand anyone a
    // presentable credential.
    const direct = await redis.get(`admin:session:${token}`);
    expect(direct).toBeNull();
    expect(await store.resolve(token)).not.toBeNull();
  });

  it("slides the idle window while a session is being used", async () => {
    const { token } = await store.create(baseSession);

    // Eleven hours in — within the 12 h idle window, so still alive, and the
    // touch pushes the expiry another 12 h out.
    now += 11 * 60 * 60 * 1000;
    expect(await store.resolve(token)).not.toBeNull();

    // Eleven more. A non-sliding window would have dropped it at hour 12.
    now += 11 * 60 * 60 * 1000;
    expect(await store.resolve(token)).not.toBeNull();
  });

  it("drops a session left idle past the window", async () => {
    const { token } = await store.create(baseSession);
    now += 13 * 60 * 60 * 1000;
    expect(await store.resolve(token)).toBeNull();
  });

  it("stops at the absolute ceiling however active the session is", async () => {
    const { token } = await store.create(baseSession);
    // Touch it every 11 hours for eight days — always inside the idle window,
    // and still finished, because the absolute ceiling does not slide.
    for (let i = 0; i < 16; i++) {
      now += 11 * 60 * 60 * 1000;
      await store.resolve(token);
    }
    expect(await store.resolve(token)).toBeNull();
  });

  it("resolves nothing for an unknown or empty token", async () => {
    expect(await store.resolve("")).toBeNull();
    expect(await store.resolve("not-a-real-token")).toBeNull();
  });

  it("revokes every session of one user at once", async () => {
    const a = await store.create(baseSession);
    const b = await store.create(baseSession);
    const other = await store.create({ ...baseSession, userId: "user-2" });

    expect(await store.destroyAllForUser("user-1")).toBe(2);
    expect(await store.resolve(a.token)).toBeNull();
    expect(await store.resolve(b.token)).toBeNull();
    // A different account is untouched.
    expect(await store.resolve(other.token)).not.toBeNull();
  });

  it("applies a role change to sessions that are already live", async () => {
    const { token } = await store.create(baseSession);
    await store.patchUserSessions("user-1", { role: "viewer" });
    expect((await store.resolve(token))?.role).toBe("viewer");
  });

  it("lists sessions newest-first and marks the current one", async () => {
    const first = await store.create(baseSession);
    now += 60_000;
    const second = await store.create(baseSession);

    const sessions = await store.listForUser("user-1", second.token);
    expect(sessions).toHaveLength(2);
    expect(sessions[0].current).toBe(true);
    expect(sessions[1].current).toBe(false);
    // The id identifies a session without being able to present it.
    expect(sessions[0].id).toHaveLength(12);
    expect(first.token).not.toContain(sessions[1].id);
  });

  it("prunes index entries whose session has expired", async () => {
    await store.create(baseSession);
    now += 13 * 60 * 60 * 1000;
    expect(await store.listForUser("user-1")).toHaveLength(0);
  });

  it("refuses a short id that is not exactly twelve characters", async () => {
    // A prefix match meant `DELETE …/sessions/a` revoked whichever session's
    // hash happened to start with `a` first — possibly the current one — and
    // answered 204 without saying which.
    const { token } = await store.create(baseSession);
    const [listed] = await store.listForUser("user-1", token);

    expect(await store.destroyByShortId("user-1", listed.id.slice(0, 1))).toBe(
      false,
    );
    expect(await store.destroyByShortId("user-1", `${listed.id}extra`)).toBe(
      false,
    );
    expect(await store.resolve(token)).not.toBeNull();
  });

  it("revokes one session by its short id", async () => {
    const a = await store.create(baseSession);
    const b = await store.create(baseSession);
    const [listed] = await store.listForUser("user-1", a.token);

    expect(await store.destroyByShortId("user-1", listed.id)).toBe(true);
    expect(await store.destroyByShortId("user-1", "ffffffffffff")).toBe(false);
    // Exactly one of the two is gone.
    const remaining = [
      await store.resolve(a.token),
      await store.resolve(b.token),
    ].filter(Boolean);
    expect(remaining).toHaveLength(1);
  });

  it("destroying an unknown token is a no-op, not a throw", async () => {
    await expect(store.destroy("nonsense")).resolves.toBeUndefined();
    await expect(store.destroy("")).resolves.toBeUndefined();
  });
});
