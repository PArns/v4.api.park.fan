import { DataSource } from "typeorm";
import { queryWithLimits, statementLimitSql } from "./statement-limits.util";

describe("statementLimitSql", () => {
  it("emits the deadlines first, then the planner settings, as SET LOCAL", () => {
    expect(
      statementLimitSql({
        statementTimeoutMs: 600_000,
        lockTimeoutMs: 30_000,
        idleInTransactionTimeoutMs: 120_000,
        planner: { enable_nestloop: "off" },
      }),
    ).toEqual([
      "SET LOCAL statement_timeout = 600000",
      "SET LOCAL lock_timeout = 30000",
      "SET LOCAL idle_in_transaction_session_timeout = 120000",
      "SET LOCAL enable_nestloop = off",
    ]);
  });

  it("never emits a plain SET, which would outlive the transaction on a pooled connection", () => {
    for (const sql of statementLimitSql({
      statementTimeoutMs: 1,
      lockTimeoutMs: 1,
      planner: { work_mem: "64MB" },
    })) {
      expect(sql.startsWith("SET LOCAL ")).toBe(true);
    }
  });

  it("refuses a missing or non-positive deadline rather than turning it off", () => {
    // statement_timeout = 0 means NO limit in Postgres: the exact failure mode
    // this exists to prevent, so it must not be reachable by a bad constant.
    expect(() =>
      statementLimitSql({ statementTimeoutMs: 0, lockTimeoutMs: 1 }),
    ).toThrow();
    expect(() =>
      statementLimitSql({ statementTimeoutMs: NaN, lockTimeoutMs: 1 }),
    ).toThrow();
    expect(() =>
      statementLimitSql({ statementTimeoutMs: 1, lockTimeoutMs: -5 }),
    ).toThrow();
  });

  it("refuses planner settings that are not a plain name and value", () => {
    expect(() =>
      statementLimitSql({
        statementTimeoutMs: 1,
        lockTimeoutMs: 1,
        planner: { "enable_nestloop; DROP TABLE x": "off" },
      }),
    ).toThrow();
    expect(() =>
      statementLimitSql({
        statementTimeoutMs: 1,
        lockTimeoutMs: 1,
        planner: { enable_nestloop: "off; SELECT 1" },
      }),
    ).toThrow();
  });
});

describe("queryWithLimits", () => {
  it("applies the limits inside the transaction, before the statement, and returns its rows", async () => {
    const issued: string[] = [];
    const manager = {
      query: jest.fn(async (sql: string) => {
        issued.push(sql);
        return sql === "SELECT 1" ? [{ one: 1 }] : [];
      }),
    };
    const dataSource = {
      transaction: jest.fn(async (cb: (m: typeof manager) => unknown) =>
        cb(manager),
      ),
    } as unknown as DataSource;

    const rows = await queryWithLimits<Array<{ one: number }>>(
      dataSource,
      "SELECT 1",
      [],
      { statementTimeoutMs: 1000, lockTimeoutMs: 500 },
    );

    expect(rows).toEqual([{ one: 1 }]);
    expect(issued).toEqual([
      "SET LOCAL statement_timeout = 1000",
      "SET LOCAL lock_timeout = 500",
      "SELECT 1",
    ]);
  });
});
