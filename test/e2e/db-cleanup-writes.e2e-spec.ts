import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { HolidaysService } from "../../src/holidays/holidays.service";
import { buildLeadSnapshotScoreUpdate } from "../../src/ml/services/prediction-lead-snapshot.service";

/**
 * PAR-872, against a real PostgreSQL: the parts of the DB clean-up that are
 * strings to the compiler.
 *
 * - The schema `synchronize` builds from the entities carries no duplicate
 *   index on the five tables whose duplicates were dropped by hand in
 *   production — so a deploy finds nothing left to drop at boot.
 * - The holiday upsert leaves an unchanged row physically untouched (same
 *   `xmin`) and still writes a changed one.
 * - The set-based lead-snapshot UPDATE types a chunk whose first row is all
 *   NULL, and writes exactly the rows it names.
 */
describe("DB clean-up writes (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
  });

  afterAll(async () => {
    await app.close();
  });

  describe("indexes", () => {
    /** Every index of a table: its key columns in order, predicate, uniqueness. */
    const indexesOf = async (
      table: string,
    ): Promise<
      Array<{
        name: string;
        cols: string;
        pred: string | null;
        am: string;
        uniq: boolean;
      }>
    > =>
      dataSource.query(
        `SELECT i.relname AS name,
                (SELECT string_agg(a.attname, ',' ORDER BY k.n)
                   FROM unnest(x.indkey) WITH ORDINALITY AS k(attnum, n)
                   JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = k.attnum) AS cols,
                pg_get_expr(x.indpred, x.indrelid) AS pred,
                am.amname AS am,
                x.indisunique AS uniq
           FROM pg_index x
           JOIN pg_class i ON i.oid = x.indexrelid
           JOIN pg_am am ON am.oid = i.relam
          WHERE x.indrelid = $1::regclass`,
        [`public.${table}`],
      );

    it.each([
      "queue_data",
      "weather_data",
      "destinations",
      "restaurants",
      "shows",
    ])("%s has no two indexes over the same columns", async (table) => {
      const idx = await indexesOf(table);
      const seen = new Map<string, string>();
      for (const i of idx) {
        const key = `${i.am}|${i.cols}|${i.pred ?? ""}`;
        expect({ table, duplicate: seen.get(key) ?? null, of: i.name }).toEqual(
          { table, duplicate: null, of: i.name },
        );
        seen.set(key, i.name);
      }
    });

    it("queue_data keeps the covering index and drops its two-column prefix", async () => {
      const idx = await indexesOf("queue_data");
      expect(idx.map((i) => i.name)).toContain("idx_queue_data_covering");
      expect(
        idx.filter(
          (i) => i.cols === "attractionId,timestamp" && i.pred === null,
        ),
      ).toEqual([]);
    });

    it("the unique columns keep their constraint-backed index", async () => {
      for (const table of ["destinations", "restaurants", "shows"]) {
        const idx = await indexesOf(table);
        const onExternalId = idx.filter((i) => i.cols === "externalId");
        expect({
          table,
          n: onExternalId.length,
          uniq: onExternalId[0]?.uniq,
        }).toEqual({
          table,
          n: 1,
          uniq: true,
        });
      }
    });
  });

  describe("holiday upsert", () => {
    const externalId = "e2e-par-872:DE:2026-10-03";
    const input = {
      externalId,
      date: new Date("2026-10-03T12:00:00Z"),
      name: "Tag der Deutschen Einheit",
      localName: "Tag der Deutschen Einheit",
      country: "DE",
      region: null,
      holidayType: "public" as const,
      isNationwide: true,
    };
    const rowVersion = async (): Promise<{ xmin: string; name: string }> => {
      const [row] = await dataSource.query(
        `SELECT xmin::text AS xmin, name FROM holidays WHERE "externalId" = $1`,
        [externalId],
      );
      return row;
    };

    afterAll(async () => {
      await dataSource.query(`DELETE FROM holidays WHERE "externalId" = $1`, [
        externalId,
      ]);
    });

    it("leaves an unchanged row untouched and still writes a changed one", async () => {
      const holidays = app.get(HolidaysService);
      // The same object on every call, on purpose: TypeORM writes the
      // generated id and timestamps back into what it was handed, and a
      // reused input then carried a millisecond-rounded `createdAt` that read
      // as a change against the stored microseconds. saveRawHolidays copies
      // its input so that can never decide whether a row is rewritten.
      await holidays.saveRawHolidays([input as never]);
      const first = await rowVersion();

      await holidays.saveRawHolidays([input as never]);
      expect(await rowVersion()).toEqual(first);

      await holidays.saveRawHolidays([
        { ...input, name: "Day of German Unity" } as never,
      ]);
      const changed = await rowVersion();
      expect(changed.name).toBe("Day of German Unity");
      expect(changed.xmin).not.toBe(first.xmin);
    });
  });

  describe("lead-snapshot scoring update", () => {
    const a = "00000000-0000-4000-8000-000000000872";
    const b = "00000000-0000-4000-8000-000000000873";

    afterAll(async () => {
      await dataSource.query(
        `DELETE FROM prediction_lead_snapshots WHERE attraction_id = ANY($1::uuid[])`,
        [[a, b]],
      );
    });

    it("writes exactly the named rows, a NULL first row included", async () => {
      for (const [id, lead] of [
        [a, 1],
        [a, 7],
        [b, 1],
      ] as const) {
        await dataSource.query(
          `INSERT INTO prediction_lead_snapshots
             (attraction_id, target_date, lead_days, predicted_wait_time, model_version, source)
           VALUES ($1, '2026-10-01', $2, 40, 'e2e', 'tft')`,
          [id, lead],
        );
      }

      const scoredAt = new Date("2026-10-03T03:00:00Z");
      const { sql, params } = buildLeadSnapshotScoreUpdate(
        [
          {
            attractionId: a,
            targetDate: "2026-10-01",
            leadDays: 1,
            actualWaitTime: null,
            absoluteError: null,
          },
          {
            attractionId: b,
            targetDate: "2026-10-01",
            leadDays: 1,
            actualWaitTime: 55,
            absoluteError: 15,
          },
        ],
        scoredAt,
      );
      await dataSource.query(sql, params);

      const rows: Array<{
        attraction_id: string;
        lead_days: number;
        actual_wait_time: number | null;
        absolute_error: number | null;
        scored: boolean;
      }> = await dataSource.query(
        `SELECT attraction_id, lead_days, actual_wait_time, absolute_error,
                scored_at = $2 AS scored
           FROM prediction_lead_snapshots
          WHERE attraction_id = ANY($1::uuid[])
          ORDER BY attraction_id, lead_days`,
        [[a, b], scoredAt],
      );
      expect(rows).toEqual([
        {
          attraction_id: a,
          lead_days: 1,
          actual_wait_time: null,
          absolute_error: null,
          scored: true,
        },
        // Not named: untouched.
        {
          attraction_id: a,
          lead_days: 7,
          actual_wait_time: null,
          absolute_error: null,
          scored: null,
        },
        {
          attraction_id: b,
          lead_days: 1,
          actual_wait_time: 55,
          absolute_error: 15,
          scored: true,
        },
      ]);
    });
  });
});
