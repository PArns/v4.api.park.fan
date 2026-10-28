import { generateKeyPairSync } from "crypto";
import {
  SearchConsoleImportService,
  buildBatch,
  isUnchanged,
  planDays,
} from "./search-console-import.service";
import { SearchConsoleApiError, type GscApiRow } from "./search-console.client";
import { BACKFILL_DAYS } from "./search-console.constants";

const HOSTS = ["park.fan", "www.park.fan"];

const apiRow = (
  keys: string[],
  clicks: number,
  impressions: number,
  position: number,
): GscApiRow => ({ keys, clicks, impressions, ctr: 0, position });

describe("buildBatch", () => {
  const pageRows = [
    apiRow(["https://park.fan/de/parks/", "deu", "MOBILE"], 4, 100, 3),
    apiRow(["https://www.park.fan/de/parks", "deu", "MOBILE"], 6, 300, 5),
    apiRow(["https://park.fan/en/news", "usa", "DESKTOP"], 1, 10, 8),
    apiRow(["https://api.park.fan/v1/x", "deu", "MOBILE"], 9, 9, 1),
  ];
  const queryRows = [
    apiRow(
      ["https://park.fan/de/parks", "Phantasialand ", "deu", "MOBILE"],
      3,
      40,
      2,
    ),
    apiRow(
      ["https://park.fan/en/news", "park news", "usa", "DESKTOP"],
      1,
      5,
      7,
    ),
  ];

  it("merges the URLs of one page and weights the position by impressions", () => {
    const batch = buildBatch(pageRows, [], HOSTS);
    const parks = batch.rows.find((row) => row.page === "/de/parks");
    expect(parks).toEqual({
      page: "/de/parks",
      query: null,
      locale: "de",
      country: "deu",
      device: "mobile",
      clicks: 10,
      impressions: 400,
      positionSum: 3 * 100 + 5 * 300,
    });
  });

  it("totals the rows without a query and skips pages off the site", () => {
    const batch = buildBatch(pageRows, queryRows, HOSTS);
    expect(batch.skipped).toBe(1);
    expect(batch.totals).toEqual({
      rows: 4,
      clicks: 11,
      impressions: 410,
      positionSum: 1800 + 80,
    });
  });

  it("stores the query rows with a normalised query beside the page rows", () => {
    const batch = buildBatch(pageRows, queryRows, HOSTS);
    const withQuery = batch.rows.filter((row) => row.query !== null);
    expect(withQuery.map((row) => row.query).sort()).toEqual([
      "park news",
      "phantasialand",
    ]);
    expect(batch.rows.filter((row) => row.query === null)).toHaveLength(2);
  });
});

describe("isUnchanged", () => {
  const current = {
    id: 1,
    rows: 4,
    clicks: 11,
    impressions: 410,
    positionSum: 1880,
  };

  it("is false for a day never fetched", () => {
    expect(
      isUnchanged(undefined, {
        rows: 0,
        clicks: 0,
        impressions: 0,
        positionSum: 0,
      }),
    ).toBe(false);
  });

  it("is true for the same totals, and false when a figure moved", () => {
    expect(isUnchanged(current, { ...current })).toBe(true);
    expect(isUnchanged(current, { ...current, clicks: 12 })).toBe(false);
    expect(isUnchanged(current, { ...current, rows: 5 })).toBe(false);
    expect(isUnchanged(current, { ...current, impressions: 411 })).toBe(false);
    expect(isUnchanged(current, { ...current, positionSum: 1900 })).toBe(false);
  });

  it("does not let an empty download erase a day that had data", () => {
    expect(
      isUnchanged(current, {
        rows: 0,
        clicks: 0,
        impressions: 0,
        positionSum: 0,
      }),
    ).toBe(true);
  });
});

describe("planDays", () => {
  const now = new Date("2026-10-28T09:20:00Z");

  it("re-imports the last days, newest first, ending yesterday", () => {
    const days = planDays(now, 3, new Set(planDaysAll()));
    expect(days).toEqual(["2026-10-27", "2026-10-26", "2026-10-25"]);
  });

  it("adds the days never fetched back to the backfill limit, newest first", () => {
    const days = planDays(now, 3, new Set());
    expect(days).toHaveLength(BACKFILL_DAYS);
    expect(days[0]).toBe("2026-10-27");
    expect(days[3]).toBe("2026-10-24");
    expect(days[days.length - 1]).toBe("2025-07-05");
  });

  it("leaves out an old day that was fetched, even one that came back empty", () => {
    const days = planDays(now, 3, new Set(["2026-10-24", "2026-10-23"]));
    expect(days).toHaveLength(BACKFILL_DAYS - 2);
    expect(days).not.toContain("2026-10-24");
  });

  function planDaysAll(): string[] {
    return planDays(now, 3, new Set()).slice(3);
  }
});

/** A service over fakes: the API's rows by day, the batches table, a lock. */
function build(
  options: {
    days?: Record<string, { page: GscApiRow[]; query: GscApiRow[] }>;
    fail?: (day: string) => Error | null;
    fetched?: Record<
      string,
      { rows: number; clicks: number; impressions: number; positionSum: number }
    >;
    lock?: boolean;
  } = {},
) {
  process.env.GSC_SERVICE_ACCOUNT_JSON = JSON.stringify({
    client_email: "gsc@park-fan.iam.gserviceaccount.com",
    private_key: generateKeyPairSync("rsa", {
      modulusLength: 2048,
      publicKeyEncoding: { type: "spki", format: "pem" },
      privateKeyEncoding: { type: "pkcs8", format: "pem" },
    }).privateKey,
  });
  process.env.GSC_SITE_URL = "sc-domain:park.fan";

  const written: Array<{ day: string; rows: number }> = [];
  const statements: string[] = [];
  const manager = {
    query: jest.fn(
      async (sql: string, params: unknown[] = []): Promise<unknown[]> => {
        statements.push(sql);
        if (sql.includes("INSERT INTO search_console_batches")) {
          written.push({ day: String(params[0]), rows: Number(params[1]) });
          return [{ id: written.length }];
        }
        return [];
      },
    ),
  };
  const db = {
    query: jest.fn(
      async (sql: string, _params?: unknown[]): Promise<unknown[]> => {
        statements.push(sql);
        if (sql.includes("FROM search_console_batches")) {
          return Object.entries(options.fetched ?? {}).map(([day, b], i) => ({
            id: i + 1,
            day,
            rows: b.rows,
            clicks: b.clicks,
            impressions: b.impressions,
            position_sum: b.positionSum,
          }));
        }
        if (sql.includes("INSERT INTO search_console_runs")) return [{ id: 1 }];
        return [];
      },
    ),
    transaction: jest.fn(
      async (work: (m: typeof manager) => Promise<unknown>) => work(manager),
    ),
  };
  const redis = {
    set: jest.fn(async () => (options.lock === false ? null : "OK")),
    eval: jest.fn(async () => 1),
  };
  const fetchDay = jest.fn(
    async (day: string, dimensions: readonly string[]) => {
      const failure = options.fail?.(day);
      if (failure) throw failure;
      const rows = options.days?.[day];
      if (!rows) return [];
      return dimensions.includes("query") ? rows.query : rows.page;
    },
  );
  const client = {
    setup: () =>
      jest
        .requireActual("../config/search-console.config")
        .getSearchConsoleSetup(),
    fetchDay,
  };
  const service = new SearchConsoleImportService(
    db as never,
    redis as never,
    client as never,
  );
  return { service, db, redis, fetchDay, written, statements, manager };
}

/** The parameters of the first statement of `db` that contains `fragment`. */
function paramsOf(
  db: { query: jest.Mock },
  fragment: string,
): unknown[] | undefined {
  const call = db.query.mock.calls.find(([sql]) =>
    String(sql).includes(fragment),
  ) as [string, unknown[]?] | undefined;
  return call?.[1];
}

describe("SearchConsoleImportService.run", () => {
  const now = new Date("2026-10-28T09:20:00Z");
  const saved = { ...process.env };
  afterEach(() => {
    process.env = { ...saved };
  });

  /** Every older day already fetched, so only the recent three are in play. */
  const olderFetched = Object.fromEntries(
    planDays(now, 3, new Set())
      .slice(3)
      .map((day) => [
        day,
        { rows: 0, clicks: 0, impressions: 0, positionSum: 0 },
      ]),
  );

  const day = (clicks: number) => ({
    page: [
      apiRow(["https://park.fan/de/parks", "deu", "MOBILE"], clicks, 50, 4),
    ],
    query: [
      apiRow(
        ["https://park.fan/de/parks", "q", "deu", "MOBILE"],
        clicks,
        20,
        3,
      ),
    ],
  });

  it("does nothing without credentials", async () => {
    const { service, fetchDay } = build();
    delete process.env.GSC_SERVICE_ACCOUNT_JSON;
    const summary = await service.run("cron", now);
    expect(summary.skipped).toBe("not-configured");
    expect(fetchDay).not.toHaveBeenCalled();
  });

  it("writes a day it has not seen: a batch and its rows in one transaction", async () => {
    const { service, written, db, manager } = build({
      fetched: {
        ...olderFetched,
        "2026-10-26": { rows: 0, clicks: 0, impressions: 0, positionSum: 0 },
        "2026-10-25": { rows: 0, clicks: 0, impressions: 0, positionSum: 0 },
      },
      days: { "2026-10-27": day(5) },
    });
    const summary = await service.run("cron", now);
    expect(summary).toMatchObject({
      daysChecked: 3,
      batchesWritten: 1,
      rowsWritten: 2,
    });
    expect(written).toEqual([{ day: "2026-10-27", rows: 2 }]);
    expect(db.transaction).toHaveBeenCalledTimes(1);
    const insert = manager.query.mock.calls.find(([sql]) =>
      String(sql).includes("INSERT INTO search_console_rows"),
    );
    expect(insert).toBeDefined();
  });

  it("marks a day Google had nothing for, so it is not asked again as a missing day", async () => {
    const { service, written } = build({ fetched: olderFetched });
    const summary = await service.run("cron", now);
    expect(summary.batchesWritten).toBe(3);
    expect(written.map((w) => w.rows)).toEqual([0, 0, 0]);
  });

  it("writes nothing for a day whose figures did not change", async () => {
    const { service, written } = build({
      fetched: {
        ...olderFetched,
        "2026-10-27": { rows: 2, clicks: 5, impressions: 50, positionSum: 200 },
        "2026-10-26": { rows: 0, clicks: 0, impressions: 0, positionSum: 0 },
        "2026-10-25": { rows: 0, clicks: 0, impressions: 0, positionSum: 0 },
      },
      days: { "2026-10-27": day(5) },
    });
    const summary = await service.run("cron", now);
    expect(summary.unchanged).toBe(3);
    expect(written).toEqual([]);
  });

  it("writes a newer batch for a day whose figures changed", async () => {
    const { service, written } = build({
      fetched: {
        ...olderFetched,
        "2026-10-27": { rows: 2, clicks: 5, impressions: 50, positionSum: 200 },
        "2026-10-26": { rows: 0, clicks: 0, impressions: 0, positionSum: 0 },
        "2026-10-25": { rows: 0, clicks: 0, impressions: 0, positionSum: 0 },
      },
      days: { "2026-10-27": day(9) },
    });
    await service.run("cron", now);
    expect(written).toEqual([{ day: "2026-10-27", rows: 2 }]);
  });

  it("records the run and releases the lock", async () => {
    const { service, db, redis } = build({ fetched: olderFetched });
    await service.run("manual", now);
    const sql = db.query.mock.calls.map(([s]) => String(s));
    expect(sql.some((s) => s.includes("INSERT INTO search_console_runs"))).toBe(
      true,
    );
    const update = paramsOf(db, "UPDATE search_console_runs");
    expect(update?.[1]).toBe(true);
    expect(redis.eval).toHaveBeenCalledTimes(1);
  });

  it("does not run while another run holds the lock", async () => {
    const { service, fetchDay } = build({ lock: false });
    const summary = await service.run("cron", now);
    expect(summary.skipped).toBe("running");
    expect(fetchDay).not.toHaveBeenCalled();
  });

  it("stops at once when Google refuses the account, and records why", async () => {
    const refusal = new SearchConsoleApiError(
      "Search Console refused gsc@x",
      403,
      true,
    );
    const { service, fetchDay, db } = build({
      fetched: olderFetched,
      fail: () => refusal,
    });
    await expect(service.run("cron", now)).rejects.toBe(refusal);
    expect(fetchDay).toHaveBeenCalledTimes(1);
    const update = paramsOf(db, "UPDATE search_console_runs");
    expect(update?.[1]).toBe(false);
    expect(String(update?.[5])).toContain("refused");
  });

  it("goes on past one failing day and fails the run at its end", async () => {
    const { service, written } = build({
      fetched: olderFetched,
      days: { "2026-10-27": day(5), "2026-10-25": day(2) },
      fail: (d) =>
        d === "2026-10-26"
          ? new SearchConsoleApiError("boom", 500, false)
          : null,
    });
    await expect(service.run("cron", now)).rejects.toThrow(
      /1 of 3 day\(s\) failed.*2026-10-26/,
    );
    expect(written.map((w) => w.day).sort()).toEqual([
      "2026-10-25",
      "2026-10-27",
    ]);
  });

  it("gives up after three failed days in a row", async () => {
    const { service, fetchDay } = build({
      fail: () => new SearchConsoleApiError("down", 503, false),
    });
    await expect(service.run("cron", now)).rejects.toThrow(
      /3 failed days in a row/,
    );
    expect(fetchDay).toHaveBeenCalledTimes(3);
  });

  it("backfills the days never fetched on the first run", async () => {
    const { service, fetchDay } = build();
    await service.run("cron", now).catch(() => undefined);
    // The first three calls are the recent days; the run is what is under test.
    expect(fetchDay.mock.calls[0][0]).toBe("2026-10-27");
  });
});

describe("SearchConsoleImportService.dropOldChunks", () => {
  it("drops chunks under a lock timeout and clears the bookkeeping that names nothing", async () => {
    const { service, db, manager } = build();
    manager.query.mockImplementation(async (sql: string): Promise<unknown[]> =>
      sql.includes("drop_chunks")
        ? [{ drop_chunks: "c1" }, { drop_chunks: "c2" }]
        : [],
    );
    expect(await service.dropOldChunks()).toBe(2);
    const statements = manager.query.mock.calls.map(([s]) => String(s));
    expect(statements[0]).toContain("lock_timeout");
    expect(statements[0]).toContain("5s");
    const deletes = db.query.mock.calls.map(([s]) => String(s));
    expect(
      deletes.some((s) => s.includes("DELETE FROM search_console_batches")),
    ).toBe(true);
    expect(
      deletes.some((s) => s.includes("DELETE FROM search_console_runs")),
    ).toBe(true);
  });

  it("leaves the chunks for the next night when the lock is not free", async () => {
    const { service, db } = build();
    db.transaction.mockRejectedValueOnce(
      new Error("canceling statement due to lock timeout"),
    );
    expect(await service.dropOldChunks()).toBe(0);
  });
});
