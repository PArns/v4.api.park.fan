import { Logger } from "@nestjs/common";
import { HYPERTABLES } from "../hypertables";
import { DbVacuumService, VacuumClient } from "./db-vacuum.service";
import {
  mayStartPcnReindex,
  PCN_REINDEX_CRON,
  PCN_REINDEX_TABLE,
  pcnReindexStatementTimeoutMs,
  TFT_FORECAST_RETENTION_DAYS,
  TFT_PRUNE_CRON,
  TFT_PRUNE_TABLES,
  tftPruneSql,
} from "./vacuum-plan";

const MIN = 60 * 1000;
// 2026-10-25 is a Sunday.
const sunday = (hhmm: string) => new Date(`2026-10-25T${hhmm}:00.000Z`);

interface Fake {
  /** Rows each successive DELETE reports, per table. */
  deletes?: Record<string, number[]>;
  missing?: string[];
  building?: boolean;
  vacuuming?: boolean;
  otherSession?: boolean;
  /** Invalid leftovers present before the REINDEX (and after it, if it fails). */
  leftovers?: string[];
  failReindex?: Error;
}

function fakeClient(opts: Fake = {}) {
  const statements: Array<{ sql: string; params?: unknown[] }> = [];
  const deletes = Object.fromEntries(
    Object.entries(opts.deletes ?? {}).map(([k, v]) => [k, [...v]]),
  );
  let leftovers = [...(opts.leftovers ?? [])];
  const client: VacuumClient & { statements: typeof statements } = {
    statements,
    connect: jest.fn().mockResolvedValue(undefined),
    end: jest.fn().mockResolvedValue(undefined),
    query: jest.fn(async (sql: string, params?: unknown[]) => {
      statements.push({ sql, params });
      if (sql.includes("to_regclass($1) IS NOT NULL")) {
        return {
          rows: [{ present: !opts.missing?.includes(params?.[0] as string) }],
        };
      }
      if (sql.startsWith("DELETE FROM")) {
        const table = sql.match(/^DELETE FROM "([^"]+)"/)![1];
        return { rows: [], rowCount: deletes[table]?.shift() ?? 0 };
      }
      if (sql.includes("pg_stat_progress_create_index")) {
        return { rows: opts.building ? [{ pid: 7, rel: "x_idx" }] : [] };
      }
      if (sql.includes("pg_stat_progress_vacuum")) {
        return { rows: opts.vacuuming ? [{ pid: 8 }] : [] };
      }
      if (sql.includes("pg_stat_activity")) {
        return { rows: opts.otherSession ? [{ pid: 9 }] : [] };
      }
      if (sql.includes("indisvalid")) {
        return { rows: leftovers.map((name) => ({ name })) };
      }
      if (sql.startsWith("DROP INDEX")) {
        leftovers = leftovers.filter((n) => !sql.includes(`"${n}"`));
      }
      if (sql.includes("pg_relation_size")) {
        const reindexed = statements.some((s) => s.sql.startsWith("REINDEX"));
        return { rows: [{ bytes: reindexed ? "5000000000" : "10000000000" }] };
      }
      if (sql.startsWith("REINDEX") && opts.failReindex) {
        leftovers = ["pcn_forecasts_pkey_ccnew"];
        throw opts.failReindex;
      }
      return { rows: [] };
    }),
  };
  return client;
}

function serviceWith(client: VacuumClient, now: Date) {
  const service = new DbVacuumService();
  jest
    .spyOn(
      service as unknown as { createClient(): VacuumClient },
      "createClient",
    )
    .mockReturnValue(client);
  jest.spyOn(service as unknown as { now(): Date }, "now").mockReturnValue(now);
  return service;
}

beforeEach(() => {
  jest.spyOn(Logger.prototype, "log").mockImplementation(() => undefined);
  jest.spyOn(Logger.prototype, "warn").mockImplementation(() => undefined);
  jest.spyOn(Logger.prototype, "error").mockImplementation(() => undefined);
});
afterEach(() => jest.restoreAllMocks());

describe("TFT forecast retention (PAR-1054)", () => {
  it("keeps 120 days, runs 04:35 UTC, covers both tables, none a hypertable", () => {
    expect(TFT_FORECAST_RETENTION_DAYS).toBe(120);
    expect(TFT_PRUNE_CRON).toBe("35 4 * * *");
    expect(TFT_PRUNE_TABLES).toEqual(["tft_forecasts", "tft_forecasts_shadow"]);
    // The ctid batching is only correct on a plain table.
    const hyper = HYPERTABLES.map((h) => h.table);
    for (const t of TFT_PRUNE_TABLES) expect(hyper).not.toContain(t);
  });

  it("deletes by forecast_date in ctid batches with no outer predicate", () => {
    const sql = tftPruneSql("tft_forecasts");
    expect(sql).toMatch(
      /^DELETE FROM "tft_forecasts"\s+WHERE ctid = ANY\(ARRAY\(/,
    );
    expect(sql).toContain(
      "WHERE forecast_date < (now() AT TIME ZONE 'UTC')::date - $1::int",
    );
    expect(sql).toContain("LIMIT $2");
  });

  it("loops until a short batch, per table, each DELETE its own statement", async () => {
    const client = fakeClient({
      deletes: { tft_forecasts: [100, 100, 40], tft_forecasts_shadow: [5] },
    });
    const results = await serviceWith(
      client,
      sunday("04:35"),
    ).pruneTftForecasts(TFT_PRUNE_TABLES, 120, 100, 10);

    expect(results).toEqual([
      { table: "tft_forecasts", deleted: 240, batches: 3, done: true },
      { table: "tft_forecasts_shadow", deleted: 5, batches: 1, done: true },
    ]);
    const dels = client.statements.filter((s) => s.sql.startsWith("DELETE"));
    expect(dels).toHaveLength(4);
    expect(dels[0].params).toEqual([120, 100]);
    expect(client.statements.map((s) => s.sql)).toContain(
      `SET statement_timeout = ${5 * MIN}`,
    );
    expect(
      client.statements.some((s) =>
        /^\s*(BEGIN|START TRANSACTION)/i.test(s.sql),
      ),
    ).toBe(false);
  });

  it("stops at the batch cap and says it is not done", async () => {
    const client = fakeClient({ deletes: { tft_forecasts: [100, 100, 100] } });
    const [r] = await serviceWith(client, sunday("04:35")).pruneTftForecasts(
      ["tft_forecasts"],
      120,
      100,
      2,
    );
    expect(r).toEqual({
      table: "tft_forecasts",
      deleted: 200,
      batches: 2,
      done: false,
    });
  });

  it("skips a table that does not exist yet", async () => {
    const client = fakeClient({ missing: ["tft_forecasts_shadow"] });
    const results = await serviceWith(
      client,
      sunday("04:35"),
    ).pruneTftForecasts();
    expect(results[1]).toEqual({
      table: "tft_forecasts_shadow",
      deleted: 0,
      batches: 0,
      done: true,
    });
    expect(
      client.statements.some((s) =>
        s.sql.startsWith('DELETE FROM "tft_forecasts_shadow"'),
      ),
    ).toBe(false);
  });
});

describe("weekly REINDEX of pcn_forecasts_pkey (PAR-1054)", () => {
  it("targets a plain table, Sundays 19:10 UTC", () => {
    expect(HYPERTABLES.map((h) => h.table)).not.toContain(PCN_REINDEX_TABLE);
    expect(PCN_REINDEX_CRON).toBe("10 19 * * 0");
  });

  it.each([
    [sunday("19:09"), false],
    [sunday("19:10"), true],
    [sunday("19:29"), true],
    [sunday("19:30"), false],
    [sunday("05:00"), false],
    [new Date("2026-10-26T19:15:00.000Z"), false], // Monday
  ])("may start at %s: %s", (now, expected) => {
    expect(mayStartPcnReindex(now)).toBe(expected);
  });

  it("caps the timeout to end by 20:03", () => {
    expect(pcnReindexStatementTimeoutMs(sunday("19:10"))).toBe(50 * MIN);
    expect(pcnReindexStatementTimeoutMs(sunday("19:29"))).toBe(34 * MIN);
  });

  it("reindexes concurrently with its lock and statement timeouts", async () => {
    const client = fakeClient();
    const r = await serviceWith(
      client,
      sunday("19:10"),
    ).reindexPcnForecastsPkey();

    expect(r).toMatchObject({
      outcome: "reindexed",
      bytesBefore: 10_000_000_000,
      bytesAfter: 5_000_000_000,
    });
    const sqls = client.statements.map((s) => s.sql);
    expect(sqls).toContain("SET lock_timeout = '60s'");
    expect(sqls).toContain(`SET statement_timeout = ${50 * MIN}`);
    expect(sqls).toContain('REINDEX INDEX CONCURRENTLY "pcn_forecasts_pkey"');
    expect(sqls.some((s) => /^\s*BEGIN/i.test(s))).toBe(false);
  });

  it.each([
    ["an index build", { building: true }],
    ["a vacuum of the table", { vacuuming: true }],
    ["another maintenance session", { otherSession: true }],
  ])("skips while %s is running", async (_label, opts) => {
    const client = fakeClient(opts);
    const r = await serviceWith(
      client,
      sunday("19:10"),
    ).reindexPcnForecastsPkey();
    expect(r.outcome).toBe("skipped_busy");
    expect(client.statements.some((s) => s.sql.startsWith("REINDEX"))).toBe(
      false,
    );
  });

  it("does not even connect outside its window", async () => {
    const client = fakeClient();
    const r = await serviceWith(
      client,
      sunday("06:00"),
    ).reindexPcnForecastsPkey();
    expect(r.outcome).toBe("skipped_window_closed");
    expect(client.connect).not.toHaveBeenCalled();
  });

  it("drops an earlier run's invalid copy before it starts", async () => {
    const client = fakeClient({ leftovers: ["pcn_forecasts_pkey_ccnew"] });
    await serviceWith(client, sunday("19:10")).reindexPcnForecastsPkey();
    const sqls = client.statements.map((s) => s.sql);
    const drop = sqls.indexOf(
      'DROP INDEX CONCURRENTLY IF EXISTS "pcn_forecasts_pkey_ccnew"',
    );
    expect(drop).toBeGreaterThan(-1);
    expect(drop).toBeLessThan(
      sqls.indexOf('REINDEX INDEX CONCURRENTLY "pcn_forecasts_pkey"'),
    );
  });

  it("cleans up its own invalid copy after a failure and never throws", async () => {
    const client = fakeClient({
      failReindex: new Error("canceling statement due to statement timeout"),
    });
    const r = await serviceWith(
      client,
      sunday("19:10"),
    ).reindexPcnForecastsPkey();
    expect(r.outcome).toBe("failed");
    const sqls = client.statements.map((s) => s.sql);
    expect(
      sqls.indexOf(
        'DROP INDEX CONCURRENTLY IF EXISTS "pcn_forecasts_pkey_ccnew"',
      ),
    ).toBeGreaterThan(
      sqls.indexOf('REINDEX INDEX CONCURRENTLY "pcn_forecasts_pkey"'),
    );
    expect(client.end).toHaveBeenCalled();
  });
});
