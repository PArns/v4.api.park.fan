import {
  HEARTBEAT_SOURCE,
  MAX_CARRIED_HEARTBEATS,
  OUTAGE_QUEUE_TYPE,
  TRAILING_OUTAGE_LOOKBACK_DAYS,
  TRAILING_OUTAGE_START_SQL,
  outageRunBreaks,
  rowBreaksOutageRun,
} from "./outage-rows.sql";
import { RECONCILIATION_SOURCE } from "./source-absent-status.util";

/** Collapses the SQL's formatting so assertions can match on wording alone. */
const flat = (sql: string) => sql.replace(/\s+/g, " ");

/**
 * The SQL half and the TypeScript half are hand-written twins, and a twin
 * drifts. The agreement test below is the point of this file: it walks every
 * combination of status and source, evaluates the TypeScript rule, and derives
 * what the SQL expression must say for the same row. A change to one half that
 * is not made to the other fails here rather than in production, where the
 * symptom would be a downtime figure that is wrong in a direction nobody looks.
 */
describe("outageRunBreaks / rowBreaksOutageRun", () => {
  const STATUSES = ["OPERATING", "CLOSED", "DOWN", "REFURBISHMENT"] as const;
  const SOURCES = [
    "themeparks-wiki",
    "queue-times",
    "wartezeiten-app",
    RECONCILIATION_SOURCE,
    HEARTBEAT_SOURCE,
    "",
  ] as const;

  /**
   * A tiny evaluator for the one expression shape this file emits. It is not a
   * SQL engine: it reads the emitted string rather than trusting a second copy
   * of the rule, so a reworded predicate is caught instead of silently passing.
   */
  function evaluateSql(status: string, source: string): boolean {
    const sql = flat(outageRunBreaks("qd"));
    const synthetic = [...sql.matchAll(/'([a-z-]+)'/g)]
      .map((m) => m[1])
      .filter((value) => value !== "DOWN");
    expect(sql).toContain("qd.status <> 'DOWN'");
    expect(sql).toContain("COALESCE(qd.data_source, '')");
    return status !== "DOWN" || synthetic.includes(source);
  }

  it("agrees with its TypeScript twin on every status and source", () => {
    const disagreements: string[] = [];
    for (const status of STATUSES) {
      for (const source of SOURCES) {
        const ts = rowBreaksOutageRun({ status, dataSource: source });
        const sql = evaluateSql(status, source);
        if (ts !== sql)
          disagreements.push(`${status}/${source}: ${ts} vs ${sql}`);
      }
    }
    expect(disagreements).toEqual([]);
  });

  it("lets an observed DOWN continue the run", () => {
    expect(
      rowBreaksOutageRun({ status: "DOWN", dataSource: "themeparks-wiki" }),
    ).toBe(false);
  });

  it("ends the run on a reconciliation row rather than dropping it", () => {
    // Dropping it would join the DOWN before a twelve-day silence to the DOWN
    // after it and report one twelve-day outage.
    expect(
      rowBreaksOutageRun({ status: "DOWN", dataSource: RECONCILIATION_SOURCE }),
    ).toBe(true);
    expect(
      rowBreaksOutageRun({
        status: "CLOSED",
        dataSource: RECONCILIATION_SOURCE,
      }),
    ).toBe(true);
  });

  it("ends the run on the heartbeat's own invented row", () => {
    // status=CLOSED, waitTime=0 for a ride with no STANDBY row in two days is
    // our bookkeeping, not a recovery.
    expect(
      rowBreaksOutageRun({ status: "CLOSED", dataSource: HEARTBEAT_SOURCE }),
    ).toBe(true);
  });

  it("treats a null data_source as a real source rather than swallowing the row", () => {
    // A NULL inside a bare IN would make the whole expression NULL, and a NULL
    // passes neither `breaks` nor `NOT breaks`: the row would leave both the run
    // and the boundary.
    expect(rowBreaksOutageRun({ status: "DOWN", dataSource: null })).toBe(
      false,
    );
    expect(flat(outageRunBreaks("qd"))).toContain(
      "COALESCE(qd.data_source, '')",
    );
  });

  it("counts REFURBISHMENT as an end, not as downtime", () => {
    expect(
      rowBreaksOutageRun({
        status: "REFURBISHMENT",
        dataSource: "themeparks-wiki",
      }),
    ).toBe(true);
  });
});

describe("TRAILING_OUTAGE_START_SQL", () => {
  const sql = flat(TRAILING_OUTAGE_START_SQL);

  it("joins on the uuid column without a text cast", () => {
    // `a.id::text = qd."attractionId"` raises `operator does not exist:
    // text = uuid`, and the catch above it turns that into an empty set on
    // every park, silently. See plan-day.service.ts:857-869.
    expect(sql).toContain(`qd."attractionId" = ANY($1::uuid[])`);
    expect(sql).not.toContain("::text");
  });

  it("bounds the timestamp as a half-open range on the raw column", () => {
    // A cast hides the column from TimescaleDB's chunk exclusion: measured at
    // 254 chunks and 35,967 buffers to find nine rows.
    expect(sql).toContain("qd.timestamp >= $2");
    expect(sql).toContain("qd.timestamp < $3");
    expect(sql).not.toMatch(/qd\.timestamp AT TIME ZONE/);
  });

  it("reads STANDBY only", () => {
    expect(sql).toContain(`qd."queueType" = '${OUTAGE_QUEUE_TYPE}'`);
  });

  it("reports a run that already covered the oldest row as unobserved", () => {
    // `last_break IS NULL` means no transition into DOWN was seen inside the
    // window, so the start is older than the window and must not be rendered as
    // an onset.
    expect(sql).toContain(`(b.last_break IS NOT NULL) AS "startObserved"`);
    expect(sql).toContain("b.last_break IS NULL OR m.ts > b.last_break");
  });

  it("takes the run's first row as the start", () => {
    expect(sql).toContain(`MIN(m.ts) AS "startedAt"`);
    expect(sql).toContain("WHERE NOT m.breaks");
  });
});

describe("the constants the reconstruction will share", () => {
  it("looks back seven days, the same bound a works period is called at", () => {
    expect(TRAILING_OUTAGE_LOOKBACK_DAYS).toBe(7);
  });

  it("stops believing a carried state after four heartbeats", () => {
    // About four hours, long enough that a real recovery — which is a real
    // delta write — lands inside.
    expect(MAX_CARRIED_HEARTBEATS).toBe(4);
  });
});
