import { isIP } from "net";
import {
  AdminAuditRetentionService,
  AUDIT_ADDRESS_RETENTION_DAYS,
  AUDIT_SWEEP_BATCH_SIZE,
} from "./admin-audit-retention.service";

const NOW = Date.parse("2026-10-28T01:15:00Z");
const DAY = 86_400_000;

interface Row {
  id: string;
  createdAt: number;
  actorIp: string | null;
}

/** The nth id in key order; the sweep walks the table by it. */
const id = (n: number) =>
  `00000000-0000-4000-8000-${n.toString().padStart(12, "0")}`;

/** A row `days` old. */
const row = (n: number, days: number, actorIp: string | null): Row => ({
  id: id(n),
  createdAt: NOW - days * DAY,
  actorIp,
});

/**
 * An in-memory `admin_audit_log` that answers the three statements the sweep
 * sends, told apart by what each one is for. It carries the same meaning as
 * the SQL (the retention cut-off, the key-order page, the update by id that
 * skips a cleared row); the e2e spec runs the SQL itself.
 */
function fakeTable(rows: Row[]) {
  const statements: Array<{ sql: string; params: unknown[] }> = [];
  const repository = {
    query: jest.fn(async (sql: string, params: unknown[] = []) => {
      statements.push({ sql, params });

      if (/created_at < now\(\)/.test(sql)) {
        const days = parseInt(String(params[0]), 10);
        const old = rows.filter(
          (r) => r.actorIp !== null && r.createdAt < NOW - days * DAY,
        );
        old.forEach((r) => (r.actorIp = null));
        return [{ cleared: old.length }];
      }

      if (/ORDER BY id/.test(sql)) {
        const [after, limit] = params as [string, number];
        return rows
          .filter((r) => r.actorIp !== null && r.id > after)
          .sort((a, b) => (a.id < b.id ? -1 : 1))
          .slice(0, limit)
          .map((r) => ({ id: r.id, actor_ip: r.actorIp }));
      }

      if (/unnest/.test(sql)) {
        const [ids, networks] = params as [string[], Array<string | null>];
        const changed: Array<{ id: string }> = [];
        ids.forEach((rowId, i) => {
          const target = rows.find((r) => r.id === rowId);
          if (target && target.actorIp !== null) {
            target.actorIp = networks[i];
            changed.push({ id: rowId });
          }
        });
        return changed;
      }

      throw new Error(`unexpected statement: ${sql}`);
    }),
  };
  const service = new AdminAuditRetentionService(repository as never);
  return {
    service,
    statements,
    updates: () => statements.filter((s) => /unnest/.test(s.sql)),
    selects: () => statements.filter((s) => /ORDER BY id/.test(s.sql)),
  };
}

describe("AdminAuditRetentionService.sweepAddresses", () => {
  it("keeps the address for 90 days, which is what the staff notice says", () => {
    expect(AUDIT_ADDRESS_RETENTION_DAYS).toBe(90);
  });

  it("clears the address of a row older than the retention and keeps the row", async () => {
    const rows = [
      row(1, AUDIT_ADDRESS_RETENTION_DAYS + 1, "203.0.113.0/24"),
      row(2, AUDIT_ADDRESS_RETENTION_DAYS + 30, "203.0.113.47"),
    ];
    const { service } = fakeTable(rows);

    const sweep = await service.sweepAddresses();

    expect(sweep).toEqual({ expired: 2, truncated: 0, unreadable: 0 });
    expect(rows.map((r) => r.actorIp)).toEqual([null, null]);
    expect(rows).toHaveLength(2);
  });

  it("keeps the network of a row inside the retention", async () => {
    const rows = [
      row(1, AUDIT_ADDRESS_RETENTION_DAYS - 1, "203.0.113.0/24"),
      row(2, 0, "2001:db8:1::/48"),
    ];
    const { service, updates } = fakeTable(rows);

    const sweep = await service.sweepAddresses();

    expect(sweep).toEqual({ expired: 0, truncated: 0, unreadable: 0 });
    expect(rows.map((r) => r.actorIp)).toEqual([
      "203.0.113.0/24",
      "2001:db8:1::/48",
    ]);
    expect(updates()).toHaveLength(0);
  });

  it("cuts a full address inside the retention to its network", async () => {
    const rows = [
      row(1, 3, "203.0.113.47"),
      row(2, 3, "::ffff:198.51.100.7"),
      row(3, 3, "2001:db8:1:2::9"),
      row(4, 3, "2001:0db8:85a3:0000:0000:8a2e:0370:7334"),
    ];
    const { service } = fakeTable(rows);

    const sweep = await service.sweepAddresses();

    expect(sweep).toEqual({ expired: 0, truncated: 4, unreadable: 0 });
    expect(rows.map((r) => r.actorIp)).toEqual([
      "203.0.113.0/24",
      "198.51.100.0/24",
      "2001:db8:1::/48",
      "2001:0db8:85a3::/48",
    ]);
  });

  it("leaves a network and a null untouched and cuts only the full address", async () => {
    const rows = [
      row(1, 3, "203.0.113.0/24"),
      row(2, 3, "2001:db8:0::/48"),
      row(3, 3, null),
      row(4, 3, "203.0.113.47"),
    ];
    const { service, updates } = fakeTable(rows);

    await service.sweepAddresses();

    expect(rows.map((r) => r.actorIp)).toEqual([
      "203.0.113.0/24",
      "2001:db8:0::/48",
      null,
      "203.0.113.0/24",
    ]);
    // One statement, naming the one row that was not yet a network.
    expect(updates()).toHaveLength(1);
    expect(updates()[0].params).toEqual([[id(4)], ["203.0.113.0/24"]]);
  });

  it("clears text that is neither an address nor a network", async () => {
    const rows = [row(1, 3, "unknown"), row(2, 3, "")];
    const { service } = fakeTable(rows);

    const sweep = await service.sweepAddresses();

    expect(sweep).toEqual({ expired: 0, truncated: 0, unreadable: 2 });
    expect(rows.map((r) => r.actorIp)).toEqual([null, null]);
  });

  it("does nothing the second time", async () => {
    const rows = [
      row(1, 200, "203.0.113.47"),
      row(2, 3, "203.0.113.47"),
      row(3, 3, "2001:db8:1:2::9"),
      row(4, 3, "unknown"),
      row(5, 3, "203.0.113.0/24"),
    ];
    const table = fakeTable(rows);

    expect(await table.service.sweepAddresses()).toEqual({
      expired: 1,
      truncated: 2,
      unreadable: 1,
    });
    const afterFirst = rows.map((r) => r.actorIp);
    table.statements.length = 0;

    expect(await table.service.sweepAddresses()).toEqual({
      expired: 0,
      truncated: 0,
      unreadable: 0,
    });
    expect(rows.map((r) => r.actorIp)).toEqual(afterFirst);
    expect(table.updates()).toHaveLength(0);
  });

  it("walks a table longer than one batch, a statement per page", async () => {
    const total = AUDIT_SWEEP_BATCH_SIZE * 2 + 1;
    const rows = Array.from({ length: total }, (_, i) =>
      row(i + 1, 1, `198.51.100.${(i % 250) + 1}`),
    );
    const table = fakeTable(rows);

    const sweep = await table.service.sweepAddresses();

    expect(sweep.truncated).toBe(total);
    expect(rows.every((r) => r.actorIp === "198.51.100.0/24")).toBe(true);
    expect(table.selects()).toHaveLength(3);
    expect(
      table.updates().map((u) => (u.params[0] as string[]).length),
    ).toEqual([AUDIT_SWEEP_BATCH_SIZE, AUDIT_SWEEP_BATCH_SIZE, 1]);
  });

  it("asks for no further page after a short one", async () => {
    const table = fakeTable([row(1, 1, "203.0.113.47")]);

    await table.service.sweepAddresses();

    expect(table.selects()).toHaveLength(1);
  });

  it("never binds an address to a statement", async () => {
    const rows = [
      row(1, 200, "203.0.113.47"),
      row(2, 3, "203.0.113.47"),
      row(3, 3, "2001:db8:1:2::9"),
    ];
    const table = fakeTable(rows);

    await table.service.sweepAddresses();

    const bound = table.statements
      .flatMap((s) => s.params)
      .flat()
      .filter((p): p is string => typeof p === "string");
    expect(bound.length).toBeGreaterThan(0);
    expect(bound.filter((p) => isIP(p) !== 0)).toEqual([]);
  });

  it("reads the cut-off from the database clock, not from a date it builds", async () => {
    const table = fakeTable([]);

    await table.service.sweepAddresses();

    const [clear] = table.statements;
    expect(clear.sql).toMatch(/created_at < now\(\) - \$1::interval/);
    expect(clear.params).toEqual([`${AUDIT_ADDRESS_RETENTION_DAYS} days`]);
  });

  it("leaves a failure to the job, which retries it", async () => {
    const repository = {
      query: jest.fn().mockRejectedValue(new Error("connection lost")),
    };
    const service = new AdminAuditRetentionService(repository as never);

    await expect(service.sweepAddresses()).rejects.toThrow("connection lost");
  });
});
