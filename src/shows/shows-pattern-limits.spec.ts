import { ShowsService } from "./shows.service";

describe("ShowsService.rebuildSchedulePatterns read deadline (PAR-837)", () => {
  it("runs the 56-day pattern read under a 180 s statement_timeout, before the statement", async () => {
    const order: string[] = [];
    const manager = {
      query: (sql: string) => {
        order.push(sql.startsWith("SET LOCAL") ? sql : "pattern-read");
        return Promise.resolve([]);
      },
      transaction: jest.fn().mockResolvedValue(undefined),
    };
    const connection = {
      transaction: (cb: (m: unknown) => Promise<unknown>) => cb(manager),
    };
    const liveRepo = { manager: { ...manager, connection } };
    const patternRepo = { manager: { transaction: manager.transaction } };
    const service = new ShowsService(
      {} as never,
      liveRepo as never,
      patternRepo as never,
    );

    await service.rebuildSchedulePatterns();

    expect(order[0]).toBe("SET LOCAL statement_timeout = 180000");
    expect(order).toContain("pattern-read");
    expect(order.indexOf("pattern-read")).toBeGreaterThan(0);
  });
});
