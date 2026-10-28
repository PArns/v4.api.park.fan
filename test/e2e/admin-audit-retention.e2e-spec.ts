import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { AdminAuditService } from "../../src/admin/auth/admin-audit.service";
import {
  AdminAuditRetentionService,
  AUDIT_ADDRESS_RETENTION_DAYS,
  AUDIT_SWEEP_BATCH_SIZE,
} from "../../src/admin/auth/admin-audit-retention.service";

/**
 * The audit log's address, against a real PostgreSQL: the SQL of the
 * retention sweep (the interval cut-off, the key-order paging, the update that
 * joins `unnest` arrays onto a uuid column) and the write path that stores a
 * network. The unit specs run the same logic on a fake table.
 */
describe("Audit log address retention (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let retention: AdminAuditRetentionService;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
    retention = app.get(AdminAuditRetentionService);
  });

  afterAll(async () => {
    await app.close();
  });

  /** Inserts one row `days` old (and a few minutes, so a boundary is not a tie). */
  const insert = async (
    label: string,
    days: number,
    actorIp: string | null,
  ): Promise<void> => {
    await dataSource.query(
      `INSERT INTO admin_audit_log (actor_email, action, entity_type, entity_label, actor_ip, created_at)
       VALUES ('editor@example.com', 'park.curate', 'park', $1, $2,
               now() - $3::interval - interval '5 minutes')`,
      [label, actorIp, `${days} days`],
    );
  };

  /** What the table holds for each label, with the row version Postgres gives it. */
  const table = async (): Promise<
    Record<string, { ip: string | null; xmin: string }>
  > => {
    const rows: Array<{ label: string; ip: string | null; xmin: string }> =
      await dataSource.query(
        `SELECT entity_label AS label, actor_ip AS ip, xmin::text AS xmin
           FROM admin_audit_log ORDER BY entity_label`,
      );
    return Object.fromEntries(
      rows.map((r) => [r.label, { ip: r.ip, xmin: r.xmin }]),
    );
  };

  it("keeps the column nullable text, as the entity has always declared it", async () => {
    const [column] = await dataSource.query(
      `SELECT data_type, is_nullable FROM information_schema.columns
        WHERE table_name = 'admin_audit_log' AND column_name = 'actor_ip'`,
    );
    expect(column).toEqual({ data_type: "text", is_nullable: "YES" });
  });

  it("clears what is past the retention, cuts what is not and touches nothing else", async () => {
    const edge = AUDIT_ADDRESS_RETENTION_DAYS;
    await insert("old-full", edge + 10, "203.0.113.47");
    await insert("old-network", edge + 10, "203.0.113.0/24");
    await insert("just-past", edge + 1, "198.51.100.7");
    await insert("just-inside", edge - 1, "198.51.100.7");
    await insert("recent-v4", 2, "203.0.113.47");
    await insert("recent-mapped", 2, "::ffff:203.0.113.47");
    await insert("recent-v6", 2, "2001:db8:1:2::9");
    await insert("recent-network", 2, "203.0.113.0/24");
    await insert("recent-network-v6", 2, "2001:db8:1::/48");
    await insert("recent-null", 2, null);
    await insert("recent-garbage", 2, "unknown");
    const before = await table();

    const sweep = await retention.sweepAddresses();

    expect(sweep).toEqual({ expired: 3, truncated: 4, unreadable: 1 });
    const after = await table();
    expect(Object.keys(after)).toHaveLength(11);
    expect(
      Object.fromEntries(Object.entries(after).map(([k, v]) => [k, v.ip])),
    ).toEqual({
      "old-full": null,
      "old-network": null,
      "just-past": null,
      "just-inside": "198.51.100.0/24",
      "recent-v4": "203.0.113.0/24",
      "recent-mapped": "203.0.113.0/24",
      "recent-v6": "2001:db8:1::/48",
      "recent-network": "203.0.113.0/24",
      "recent-network-v6": "2001:db8:1::/48",
      "recent-null": null,
      "recent-garbage": null,
    });
    // Not rewritten: already a network, already empty.
    for (const label of [
      "recent-network",
      "recent-network-v6",
      "recent-null",
    ]) {
      expect(after[label].xmin).toBe(before[label].xmin);
    }
  });

  it("finds nothing to do on a second run and rewrites no row", async () => {
    await insert("old", AUDIT_ADDRESS_RETENTION_DAYS + 5, "203.0.113.47");
    await insert("recent", 2, "203.0.113.47");
    await retention.sweepAddresses();
    const settled = await table();

    expect(await retention.sweepAddresses()).toEqual({
      expired: 0,
      truncated: 0,
      unreadable: 0,
    });
    expect(await table()).toEqual(settled);
  });

  it("walks a table of more than two batches", async () => {
    const total = AUDIT_SWEEP_BATCH_SIZE * 2 + 37;
    await dataSource.query(
      `INSERT INTO admin_audit_log (actor_email, action, entity_type, entity_label, actor_ip)
       SELECT 'editor@example.com', 'park.curate', 'park', 'bulk-' || n,
              CASE WHEN n % 2 = 0 THEN '198.51.100.' || (n % 250 + 1)
                   ELSE '2001:db8:' || to_hex(n) || '::1' END
         FROM generate_series(1, $1::int) AS n`,
      [total],
    );

    const sweep = await retention.sweepAddresses();

    expect(sweep).toEqual({ expired: 0, truncated: total, unreadable: 0 });
    const [left] = await dataSource.query(
      `SELECT count(*)::int AS n FROM admin_audit_log
        WHERE actor_ip NOT LIKE '%/24' AND actor_ip NOT LIKE '%/48'`,
    );
    expect(left.n).toBe(0);
  });

  it("stores the network when an admin action is audited", async () => {
    await app.get(AdminAuditService).record({
      actor: {
        userId: null,
        email: "legacy-pass",
        displayName: "legacy",
        role: "owner",
        sessionToken: null,
        legacy: true,
        ip: "203.0.113.47",
        mustChangePassword: false,
        mustEnrolTotp: false,
      },
      action: "park.curate",
      entityType: "park",
      entityLabel: "written",
    });

    expect((await table()).written.ip).toBe("203.0.113.0/24");
  });
});
