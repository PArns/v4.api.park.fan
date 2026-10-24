import {
  diffScheduleDays,
  DerivedDay,
  recordScheduleDayVersions,
  ScheduleDayState,
} from "./schedule-version-log";

const day = (
  d: string,
  over: Partial<ScheduleDayState> = {},
): ScheduleDayState => ({
  day: d,
  state: "OPERATING",
  open_utc: "2026-11-01T09:00:00.000Z",
  close_utc: "2026-11-01T18:00:00.000Z",
  n_windows: 1,
  ticketed_event: false,
  private_event: false,
  extra_hours: false,
  extra_open_utc: null,
  extra_close_utc: null,
  ...over,
});

describe("diffScheduleDays", () => {
  it("writes nothing when the derived day equals the open version", () => {
    // pg hands timestamptz back as Date on one side; same instant, no change.
    const derived = day("2026-11-01", {
      open_utc: new Date("2026-11-01T09:00:00Z") as unknown as string,
    });
    expect(diffScheduleDays([day("2026-11-01")], [derived])).toEqual({
      changed: [],
      closeDays: [],
    });
  });

  it("appends a day that has no version yet, without closing anything", () => {
    const res = diffScheduleDays([], [day("2026-11-01")]);
    expect(res.changed.map((c) => c.day)).toEqual(["2026-11-01"]);
    expect(res.closeDays).toEqual([]);
  });

  it("closes and replaces a version whose hours changed", () => {
    const res = diffScheduleDays(
      [day("2026-11-01")],
      [day("2026-11-01", { close_utc: "2026-11-01T20:00:00.000Z" })],
    );
    expect(res.closeDays).toEqual(["2026-11-01"]);
    expect(res.changed[0].close_utc).toBe("2026-11-01T20:00:00.000Z");
  });

  it("records a retraction as a NONE version", () => {
    const res = diffScheduleDays([day("2026-11-01")], []);
    expect(res.closeDays).toEqual(["2026-11-01"]);
    expect(res.changed).toEqual([
      expect.objectContaining({ day: "2026-11-01", state: "NONE" }),
    ]);
  });

  it("does not log a day that was never published (NONE vs no version)", () => {
    const none = day("2026-11-01", {
      state: "NONE",
      open_utc: null,
      close_utc: null,
      n_windows: 0,
    });
    expect(diffScheduleDays([none], []).changed).toEqual([]);
  });

  it("logs a gap-fill promotion UNKNOWN → CLOSED", () => {
    const unknown = day("2026-11-01", {
      state: "UNKNOWN",
      open_utc: null,
      close_utc: null,
      n_windows: 0,
    });
    const res = diffScheduleDays([unknown], [{ ...unknown, state: "CLOSED" }]);
    expect(res.changed.map((c) => c.state)).toEqual(["CLOSED"]);
  });
});

describe("recordScheduleDayVersions", () => {
  /** A transaction double that answers the three reads and records the writes. */
  const fakeManager = (
    open: ScheduleDayState[],
    current: DerivedDay[],
    anyVersion = open.length > 0,
  ) => {
    const writes: Array<{ sql: string; params: unknown[] }> = [];
    const tx = {
      query: jest.fn(async (sql: string, params: unknown[]) => {
        if (sql.includes("pg_advisory_xact_lock")) return [];
        if (sql.includes("known_to IS NULL") && sql.trim().startsWith("SELECT"))
          return open;
        if (sql.includes("LIMIT 1"))
          return anyVersion ? [{ "?column?": 1 }] : [];
        if (sql.includes("FROM schedule_entries")) return current;
        writes.push({ sql, params });
        return [];
      }),
    };
    const manager = {
      transaction: (cb: (t: typeof tx) => Promise<number>) => cb(tx),
    };
    return { manager: manager as never, writes };
  };

  it("backfills a park's first versions with asof_exact = false", async () => {
    const { manager, writes } = fakeManager(
      [],
      [{ ...day("2026-11-01"), last_updated: "2026-10-01T03:00:00Z" }],
    );
    expect(await recordScheduleDayVersions(manager, "p1", "themeparks")).toBe(
      1,
    );
    expect(writes).toHaveLength(1);
    expect(writes[0].sql).toContain("INSERT INTO schedule_day_versions");
    // $3 = firstLog → known_from = max(updatedAt), source 'backfill', NOT exact.
    expect(writes[0].params[2]).toBe(true);
  });

  it("writes nothing on an unchanged re-sync", async () => {
    const { manager, writes } = fakeManager(
      [day("2026-11-01")],
      [day("2026-11-01")],
    );
    expect(await recordScheduleDayVersions(manager, "p1", "themeparks")).toBe(
      0,
    );
    expect(writes).toEqual([]);
  });

  it("closes the open version and appends the new one on a change", async () => {
    const { manager, writes } = fakeManager(
      [day("2026-11-01")],
      [
        day("2026-11-01", {
          state: "CLOSED",
          open_utc: null,
          close_utc: null,
          n_windows: 0,
        }),
      ],
    );
    await recordScheduleDayVersions(manager, "p1", "gap_fill");
    expect(writes.map((w) => w.sql.trim().split(/\s+/)[0])).toEqual([
      "UPDATE",
      "INSERT",
    ]);
    expect(writes[0].params).toEqual(["p1", ["2026-11-01"]]);
    // Not the first log: exact, labelled with the caller's source.
    expect(writes[1].params[2]).toBe(false);
    expect(writes[1].params[3]).toBe("gap_fill");
  });

  it("is idempotent: a second call over the logged state writes nothing", async () => {
    const changed = day("2026-11-01", {
      close_utc: "2026-11-01T20:00:00.000Z",
    });
    const first = fakeManager([day("2026-11-01")], [changed]);
    await recordScheduleDayVersions(first.manager, "p1", "themeparks");
    expect(first.writes).toHaveLength(2);

    // What the first call left open is now the open version.
    const second = fakeManager([changed], [changed]);
    await recordScheduleDayVersions(second.manager, "p1", "themeparks");
    expect(second.writes).toEqual([]);
  });
});
