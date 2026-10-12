import { Logger } from "@nestjs/common";
import { DbVacuumService, VacuumClient } from "./db-vacuum.service";
import {
  DB_VACUUM_CRON,
  mayStartVacuum,
  quoteIdent,
  VACUUM_TARGETS,
  VacuumTarget,
  vacuumStatementTimeoutMs,
} from "./vacuum-plan";

const MIN = 60 * 1000;
const at = (hhmm: string) => new Date(`2026-10-13T${hhmm}:00.000Z`);

describe("vacuum plan (PAR-868)", () => {
  it("is scheduled at 04:50", () => {
    expect(DB_VACUUM_CRON).toBe("50 4 * * *");
  });

  it.each([
    ["04:49", false],
    ["04:50", true],
    ["05:30", true],
    ["05:49", true],
    ["05:50", false], // the hard stop: no table STARTS at or after 05:50
    ["06:00", false],
    ["06:15", false], // a late retry must not land in the training window
    ["12:00", false],
  ])("may start a table at %s UTC: %s", (hhmm, expected) => {
    expect(mayStartVacuum(at(hhmm))).toBe(expected);
  });

  it("uses the table's own timeout when the deadline is far", () => {
    expect(vacuumStatementTimeoutMs(at("04:50"), 30 * MIN)).toBe(30 * MIN);
  });

  it("caps the timeout so a VACUUM started late ends by 05:58", () => {
    // Started 05:49 with a 30-min bound → only 9 minutes are left.
    expect(vacuumStatementTimeoutMs(at("05:49"), 30 * MIN)).toBe(9 * MIN);
  });

  it("never returns 0, which Postgres reads as no limit", () => {
    expect(vacuumStatementTimeoutMs(at("06:30"), 30 * MIN)).toBe(1000);
  });

  it("starts with the worst offender and lists no hypertable", () => {
    expect(VACUUM_TARGETS[0].table).toBe("pcn_forecasts");
    const tables = VACUUM_TARGETS.map((t) => t.table);
    expect(tables).not.toContain("wait_time_predictions");
    expect(tables).not.toContain("queue_data");
  });

  it("quotes identifiers", () => {
    expect(quoteIdent("pcn_forecasts")).toBe('"pcn_forecasts"');
    expect(quoteIdent('a"b')).toBe('"a""b"');
  });
});

interface FakeOptions {
  inProgress?: string[];
  missing?: string[];
  failVacuum?: Record<string, Error>;
  /** Advances the fake clock by this many minutes per VACUUM. */
  vacuumMinutes?: number;
}

/** A pg client that records every statement and answers the catalogue reads. */
function fakeClient(clock: { now: Date }, opts: FakeOptions = {}) {
  const statements: string[] = [];
  const client: VacuumClient & { statements: string[]; ended: boolean } = {
    statements,
    ended: false,
    connect: jest.fn().mockResolvedValue(undefined),
    end: jest.fn(async () => {
      client.ended = true;
    }),
    query: jest.fn(async (sql: string, params?: unknown[]) => {
      statements.push(sql);
      const table = params?.[0] as string | undefined;
      if (sql.includes("to_regclass($1) IS NOT NULL")) {
        return { rows: [{ present: !opts.missing?.includes(table!) }] };
      }
      if (sql.includes("pg_stat_progress_vacuum")) {
        return {
          rows: opts.inProgress?.includes(table!) ? [{ pid: 4242 }] : [],
        };
      }
      if (sql.includes("n_dead_tup")) {
        // Before the VACUUM of this table: 1000; after: 0.
        const vacuumed = statements.some(
          (s) => s === `VACUUM (ANALYZE) ${quoteIdent(table!)}`,
        );
        return { rows: [{ dead: vacuumed ? "0" : "1000" }] };
      }
      if (sql.startsWith("VACUUM")) {
        const name = sql.match(/"([^"]+)"/)?.[1] ?? "";
        clock.now = new Date(
          clock.now.getTime() + (opts.vacuumMinutes ?? 1) * MIN,
        );
        const err = opts.failVacuum?.[name];
        if (err) throw err;
      }
      return { rows: [] };
    }),
  };
  return client;
}

function serviceWith(client: VacuumClient, clock: { now: Date }) {
  const service = new DbVacuumService();
  jest
    .spyOn(
      service as unknown as { createClient(): VacuumClient },
      "createClient",
    )
    .mockReturnValue(client);
  jest
    .spyOn(service as unknown as { now(): Date }, "now")
    .mockImplementation(() => clock.now);
  return service;
}

const targets: VacuumTarget[] = [
  { table: "pcn_forecasts", maxStatementMs: 30 * MIN, reason: "" },
  { table: "prediction_accuracy", maxStatementMs: 15 * MIN, reason: "" },
  { table: "holidays", maxStatementMs: 5 * MIN, reason: "" },
];

describe("DbVacuumService (PAR-868)", () => {
  beforeEach(() => {
    jest.spyOn(Logger.prototype, "log").mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, "warn").mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, "error").mockImplementation(() => undefined);
  });
  afterEach(() => jest.restoreAllMocks());

  it("vacuums every table, one at a time, and logs dead tuples before/after", async () => {
    const clock = { now: at("04:50") };
    const client = fakeClient(clock);
    const results = await serviceWith(client, clock).vacuumChurnTables(targets);

    expect(results.map((r) => [r.table, r.outcome])).toEqual([
      ["pcn_forecasts", "vacuumed"],
      ["prediction_accuracy", "vacuumed"],
      ["holidays", "vacuumed"],
    ]);
    expect(results[0]).toMatchObject({ deadBefore: 1000, deadAfter: 0 });
    const vacuums = client.statements.filter((s) => s.startsWith("VACUUM"));
    expect(vacuums).toEqual([
      'VACUUM (ANALYZE) "pcn_forecasts"',
      'VACUUM (ANALYZE) "prediction_accuracy"',
      'VACUUM (ANALYZE) "holidays"',
    ]);
    expect(client.ended).toBe(true);
  });

  it("sets lock_timeout, a 256MB work memory and a per-table statement_timeout", async () => {
    const clock = { now: at("04:50") };
    const client = fakeClient(clock);
    await serviceWith(client, clock).vacuumChurnTables(targets);

    expect(client.statements).toContain("SET lock_timeout = '5s'");
    expect(client.statements).toContain("SET maintenance_work_mem = '256MB'");
    // pcn_forecasts at 04:50 with a 30-min bound.
    expect(client.statements).toContain(`SET statement_timeout = ${30 * MIN}`);
    // Never VACUUM FULL.
    expect(client.statements.some((s) => /VACUUM\s*\(?\s*FULL/i.test(s))).toBe(
      false,
    );
  });

  it("never wraps a VACUUM in a transaction and sends it as its own statement", async () => {
    const clock = { now: at("04:50") };
    const client = fakeClient(clock);
    await serviceWith(client, clock).vacuumChurnTables(targets);

    expect(
      client.statements.some((s) =>
        /^\s*(BEGIN|START TRANSACTION|COMMIT|SAVEPOINT)/i.test(s),
      ),
    ).toBe(false);
    for (const s of client.statements.filter((x) => x.includes("VACUUM"))) {
      expect(s).not.toContain(";");
    }
  });

  it("starts no table at or after 05:50, and skips the rest", async () => {
    // Each VACUUM takes 25 minutes: 05:20 → 05:45 → 06:10.
    const clock = { now: at("05:20") };
    const client = fakeClient(clock, { vacuumMinutes: 25 });
    const results = await serviceWith(client, clock).vacuumChurnTables(targets);

    expect(results.map((r) => r.outcome)).toEqual([
      "vacuumed", // started 05:20
      "vacuumed", // started 05:45
      "skipped_window_closed", // would start 06:10
    ]);
    // The 05:45 start got the deadline-capped timeout: 13 minutes to 05:58.
    expect(client.statements).toContain(`SET statement_timeout = ${13 * MIN}`);
  });

  it("does nothing at all when the run arrives outside the window", async () => {
    const clock = { now: at("06:05") };
    const client = fakeClient(clock);
    const service = serviceWith(client, clock);
    const results = await service.vacuumChurnTables(targets);

    expect(results.every((r) => r.outcome === "skipped_window_closed")).toBe(
      true,
    );
    expect(client.connect).not.toHaveBeenCalled();
  });

  it("skips a table that already has a vacuum in progress", async () => {
    const clock = { now: at("04:50") };
    const client = fakeClient(clock, { inProgress: ["prediction_accuracy"] });
    const results = await serviceWith(client, clock).vacuumChurnTables(targets);

    expect(results[1]).toMatchObject({
      table: "prediction_accuracy",
      outcome: "skipped_in_progress",
    });
    expect(client.statements).not.toContain(
      'VACUUM (ANALYZE) "prediction_accuracy"',
    );
    expect(results[2].outcome).toBe("vacuumed");
  });

  it("skips a table that does not exist", async () => {
    const clock = { now: at("04:50") };
    const client = fakeClient(clock, { missing: ["holidays"] });
    const results = await serviceWith(client, clock).vacuumChurnTables(targets);
    expect(results[2].outcome).toBe("skipped_missing");
  });

  it("isolates a failing table: logs it, resets the timeout, carries on, never throws", async () => {
    const clock = { now: at("04:50") };
    const client = fakeClient(clock, {
      failVacuum: {
        pcn_forecasts: new Error("canceling statement due to lock timeout"),
      },
    });
    const results = await serviceWith(client, clock).vacuumChurnTables(targets);

    expect(results[0]).toMatchObject({
      table: "pcn_forecasts",
      outcome: "failed",
      error: "canceling statement due to lock timeout",
    });
    expect(results.slice(1).map((r) => r.outcome)).toEqual([
      "vacuumed",
      "vacuumed",
    ]);
    const failedAt = client.statements.indexOf(
      'VACUUM (ANALYZE) "pcn_forecasts"',
    );
    expect(client.statements[failedAt + 1]).toBe("RESET statement_timeout");
    expect(client.ended).toBe(true);
  });

  it("survives a catalogue query that throws", async () => {
    const clock = { now: at("04:50") };
    const client = fakeClient(clock);
    const original = client.query;
    let first = true;
    client.query = jest.fn(async (sql: string, params?: unknown[]) => {
      if (first && sql.includes("pg_stat_progress_vacuum")) {
        first = false;
        throw new Error("connection reset");
      }
      return original(sql, params);
    });
    const results = await serviceWith(client, clock).vacuumChurnTables(targets);
    expect(results.map((r) => r.outcome)).toEqual([
      "failed",
      "vacuumed",
      "vacuumed",
    ]);
  });

  it("returns without throwing when the connection cannot be opened", async () => {
    const clock = { now: at("04:50") };
    const client = fakeClient(clock);
    (client.connect as jest.Mock).mockRejectedValue(new Error("ECONNREFUSED"));
    await expect(
      serviceWith(client, clock).vacuumChurnTables(targets),
    ).resolves.toEqual([]);
  });
});
