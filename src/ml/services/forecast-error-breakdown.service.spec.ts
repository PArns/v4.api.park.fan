import { ForecastErrorBreakdownService } from "./forecast-error-breakdown.service";

const PARK = "11111111-1111-4111-8111-111111111111";

function build(rows: unknown[]) {
  const query = jest.fn(async (sql: string, _params?: unknown[]) =>
    sql.startsWith("SET LOCAL") ? [] : rows,
  );
  const dataSource = {
    transaction: async (cb: (m: unknown) => unknown) => cb({ query }),
  };
  const redis = {
    get: jest.fn(async () => null),
    set: jest.fn(async () => "OK"),
  };
  return {
    service: new ForecastErrorBreakdownService(
      dataSource as never,
      redis as never,
    ),
    query,
  };
}

const sqlRow = (recent: boolean, over = {}) => ({
  id: "p1",
  name: "Park",
  model: "catboost",
  recent,
  n: "100",
  sae: recent ? "830" : "710",
  sbias: "50",
  nBase: "80",
  saeOnBase: "600",
  saeBase: "800",
  ...over,
});

describe("ForecastErrorBreakdownService", () => {
  const now = new Date("2026-10-29T10:30:00Z");

  it("reports MAE, bias and the change against the window before", async () => {
    const { service } = build([sqlRow(true), sqlRow(false)]);
    const out = await service.breakdown({ days: 7 }, now);
    expect(out.grain).toBe("park");
    expect(out.rows[0].recent).toMatchObject({
      n: 100,
      mae: 8.3,
      bias: 0.5,
      baselineMae: 10,
      skill: 0.75,
    });
    expect(out.rows[0].maeChange).toBe(1.2);
    expect(out.totals[0].maeChange).toBe(1.2);
    expect(out.window.until).toBe("2026-10-29T10:00:00.000Z");
  });

  it("binds the park as a uuid and the model as parameters", async () => {
    const { service, query } = build([]);
    const out = await service.breakdown(
      { days: 7, parkId: PARK, model: "pcn" },
      now,
    );
    expect(out.grain).toBe("ride");
    const call = query.mock.calls.find(
      ([sql]) => !sql.startsWith("SET LOCAL"),
    )!;
    expect(call[0]).toContain('att."parkId" = $2::uuid');
    expect(call[0]).toContain("m.model = $3");
    expect(call[0]).not.toContain("::text =");
    expect(call[1]!).toHaveLength(3);
    expect(call[1]![1]).toBe(PARK);
  });

  it("refuses an id that is not a uuid before it reaches SQL", async () => {
    const { service } = build([]);
    await expect(
      service.breakdown({ days: 7, parkId: "x'; drop table" }, now),
    ).rejects.toThrow(/uuid/);
  });

  it("clamps days so both windows and the baseline stay inside the retention", async () => {
    const { service } = build([]);
    expect((await service.breakdown({ days: 99 }, now)).days).toBe(31);
  });
});
