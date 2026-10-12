import { Logger } from "@nestjs/common";
import { DataSource } from "typeorm";
import { TimescaleInitService } from "./timescale-init.service";

/**
 * The two boot steps that keep TimescaleDB jobs from queueing readers behind an
 * ACCESS EXCLUSIVE lock (db-health-runbook §0b): removing the retention policy
 * that locked `attractions`, and making compression give up its truncate lock
 * instead of waiting for it. Both run against production on every boot, so the
 * properties pinned here are the ones that make that safe: a bounded lock wait,
 * no write when nothing needs writing, and no error escaping into the boot.
 */
describe("TimescaleInitService — lock-safe boot steps", () => {
  type Query = { sql: string; params?: unknown[] };

  let outer: Query[];
  let inner: Query[];
  let policyRows: unknown[];
  let settingRows: Array<{ setconfig: string[] | null }>;
  let failOn: string | null;
  let service: TimescaleInitService;
  let warn: jest.SpyInstance;

  const run = (bucket: Query[]) =>
    jest.fn((sql: string, params?: unknown[]) => {
      bucket.push({ sql, params });
      if (failOn && sql.includes(failOn)) {
        return Promise.reject(new Error(`failed: ${failOn}`));
      }
      if (sql.includes("timescaledb_information.jobs")) {
        return Promise.resolve(policyRows);
      }
      if (sql.includes("pg_db_role_setting")) {
        return Promise.resolve(settingRows);
      }
      return Promise.resolve([]);
    });

  // Private steps, called directly: running initializeHypertables would also
  // create hypertables, which is not what this spec is about.
  const steps = () =>
    service as unknown as {
      setupRetentionPolicies(): Promise<void>;
      setupCompressTruncateBehaviour(): Promise<void>;
    };

  beforeEach(() => {
    outer = [];
    inner = [];
    policyRows = [];
    settingRows = [];
    failOn = null;

    const dataSource = {
      query: run(outer),
      transaction: jest.fn((cb: (em: unknown) => Promise<unknown>) =>
        cb({ query: run(inner) }),
      ),
    } as unknown as DataSource;

    service = new TimescaleInitService(dataSource);
    warn = jest
      .spyOn(Logger.prototype, "warn")
      .mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, "log").mockImplementation(() => undefined);
  });

  afterEach(() => jest.restoreAllMocks());

  describe("retention policy removal", () => {
    it("does nothing when no retention policy exists", async () => {
      await steps().setupRetentionPolicies();

      expect(outer).toHaveLength(1);
      expect(outer[0].sql).toContain("proc_name = 'policy_retention'");
      expect(outer[0].params).toEqual(["wait_time_predictions"]);
      expect(inner).toHaveLength(0);
    });

    it("removes an existing policy under a transaction-local lock_timeout", async () => {
      policyRows = [{ "?column?": 1 }];

      await steps().setupRetentionPolicies();

      expect(inner).toHaveLength(2);
      // The lock_timeout comes first and is local (third argument true).
      expect(inner[0].sql).toContain("set_config('lock_timeout', '5s', true)");
      expect(inner[1].sql).toContain("remove_retention_policy($1");
      expect(inner[1].sql).toContain("if_exists => true");
      expect(inner[1].params).toEqual(["wait_time_predictions"]);
      // And it never adds one back.
      const all = [...outer, ...inner].map((q) => q.sql).join("\n");
      expect(all).not.toContain("add_retention_policy");
    });

    it("only warns when the removal times out", async () => {
      policyRows = [{ "?column?": 1 }];
      failOn = "remove_retention_policy";

      await expect(steps().setupRetentionPolicies()).resolves.toBeUndefined();
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining("Could not remove the retention policy"),
      );
    });

    it("only warns when the policy lookup itself fails", async () => {
      failOn = "timescaledb_information.jobs";

      await expect(steps().setupRetentionPolicies()).resolves.toBeUndefined();
      expect(warn).toHaveBeenCalledTimes(1);
    });
  });

  describe("compress_truncate_behaviour", () => {
    const alters = () => outer.filter((q) => q.sql.includes("ALTER DATABASE"));

    it("reads the current database's own setting, not a role's", async () => {
      await steps().setupCompressTruncateBehaviour();

      expect(outer[0].sql).toContain("pg_db_role_setting");
      expect(outer[0].sql).toContain("d.datname = current_database()");
      expect(outer[0].sql).toContain("s.setrole = 0");
    });

    it("sets truncate_or_delete on the current database when it is missing", async () => {
      settingRows = [{ setconfig: ["pg_trgm.word_similarity_threshold=0.4"] }];

      await steps().setupCompressTruncateBehaviour();

      expect(alters()).toHaveLength(1);
      const sql = alters()[0].sql;
      expect(sql).toContain(
        "ALTER DATABASE %I SET timescaledb.compress_truncate_behaviour = %L",
      );
      expect(sql).toContain("current_database(), 'truncate_or_delete'");
    });

    it("sets it when the database has no settings row at all", async () => {
      settingRows = [];

      await steps().setupCompressTruncateBehaviour();

      expect(alters()).toHaveLength(1);
    });

    it("writes nothing when the setting is already in place", async () => {
      settingRows = [
        {
          setconfig: [
            "pg_trgm.word_similarity_threshold=0.4",
            "timescaledb.compress_truncate_behaviour=truncate_or_delete",
          ],
        },
      ];

      await steps().setupCompressTruncateBehaviour();

      expect(outer).toHaveLength(1);
      expect(alters()).toHaveLength(0);
    });

    it("overwrites a different value", async () => {
      settingRows = [
        {
          setconfig: ["timescaledb.compress_truncate_behaviour=truncate_only"],
        },
      ];

      await steps().setupCompressTruncateBehaviour();

      expect(alters()).toHaveLength(1);
    });

    it("only warns when the ALTER fails", async () => {
      failOn = "ALTER DATABASE";

      await expect(
        steps().setupCompressTruncateBehaviour(),
      ).resolves.toBeUndefined();
      expect(warn).toHaveBeenCalledWith(
        expect.stringContaining("Could not set compress_truncate_behaviour"),
      );
    });
  });
});
