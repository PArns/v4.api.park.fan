import { Test, TestingModule } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { AttractionOutageService } from "./attraction-outage.service";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { DowntimeRecoveryCurve } from "../../analytics/entities/downtime-recovery-curve.entity";

/**
 * The service had no spec, and the closure path is the one that needs one.
 *
 * `addClosureGaps` swallows its own failures on purpose — a line under a badge
 * is a nicety, the park page is not — which is exactly why nothing here is
 * self-reporting. The statement's own restriction to blind parks was believed
 * to make it cheap outside them for months while it was 79 % of the database,
 * and a wrong parameter list would be just as silent.
 *
 * What the regime gate is NOT: it does not live here. It is an uncorrelated
 * EXISTS in the statement, where the answer's owner is one join away, and
 * `closure-gap.sql.spec.ts` pins its shape.
 */
describe("AttractionOutageService — the closure path", () => {
  const PARK = {
    id: "11111111-1111-4111-8111-111111111111",
    timezone: "Europe/Berlin",
    wikiEntityId: "wiki-1",
  };
  const OPEN_RIDE = { id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" };
  const DOWN_RIDE = {
    id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
    effectiveStatus: "DOWN",
  };

  let service: AttractionOutageService;
  let query: jest.Mock;
  let curves: { find: jest.Mock };

  const build = async () => {
    query = jest.fn().mockResolvedValue([]);
    curves = { find: jest.fn().mockResolvedValue([]) };
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AttractionOutageService,
        {
          provide: getRepositoryToken(QueueData),
          useValue: { manager: { query } },
        },
        {
          provide: getRepositoryToken(DowntimeRecoveryCurve),
          useValue: curves,
        },
      ],
    }).compile();
    service = module.get(AttractionOutageService);
  };

  beforeEach(build);

  it("asks the closure statement for the whole roster, the zone and the park", async () => {
    // All four, and the fourth is the one that gets forgotten. The statement
    // counts how many rides in the PARK shut in the same minute, so over the
    // candidate list alone that count is whatever the caller happened to pass
    // and the filter separating a park-wide closing from a fault never fires.
    await service.getCurrentOutages(PARK, [OPEN_RIDE]);

    expect(query).toHaveBeenCalledTimes(1);
    expect(query.mock.calls[0][1]).toEqual([
      [OPEN_RIDE.id],
      PARK.timezone,
      expect.any(Date),
      PARK.id,
    ]);
  });

  it("asks it for EVERY ride the DOWN query did not place, the DOWN one included", async () => {
    // The regression this file exists for, in both its halves.
    //
    // It returned early when the DOWN query came back empty, which is the NOTE
    // reached by a different road — so every other ride in the park lost its
    // closure line. And the ride that reads DOWN was filtered out of the
    // candidates on the strength of a status whose query had just answered
    // nothing, so the one ride actually standing still was the only ride with
    // no line at all.
    //
    // Both queries take four parameters; the closure one is the query whose
    // fourth is the park id.
    query.mockResolvedValue([]);

    await service.getCurrentOutages(PARK, [OPEN_RIDE, DOWN_RIDE]);

    const closureCall = query.mock.calls.find((c) => c[1][3] === PARK.id);
    expect(closureCall).toBeDefined();
    expect(closureCall![1][0]).toEqual([OPEN_RIDE.id, DOWN_RIDE.id]);
  });

  it("does not re-ask for a ride the DOWN query already placed", async () => {
    query.mockImplementation((_sql: string, params: unknown[]) =>
      Array.isArray(params) && params[3] === PARK.id
        ? Promise.resolve([])
        : Promise.resolve([
            {
              attractionId: DOWN_RIDE.id,
              startedAt: new Date("2026-09-08T09:00:00Z"),
              startObserved: true,
              rowsInRun: 3,
              elapsedOperatingMinutes: 45,
              hasWindows: true,
            },
          ]),
    );

    const out = await service.getCurrentOutages(PARK, [OPEN_RIDE, DOWN_RIDE]);

    expect(out.get(DOWN_RIDE.id)?.signal).toBe("down");
    const closureCall = query.mock.calls.find((c) => c[1][3] === PARK.id);
    expect(closureCall![1][0]).toEqual([OPEN_RIDE.id]);
  });

  it("a failing closure query costs the line, never the page", async () => {
    query.mockRejectedValue(new Error("statement timeout"));

    await expect(
      service.getCurrentOutages(PARK, [OPEN_RIDE]),
    ).resolves.toBeInstanceOf(Map);
  });

  it("a failed DOWN query does not hand its rides to the closure statement", async () => {
    // The distinction between "found nothing" and "could not look", and it is a
    // wording question. On a timeout `out` is empty, so a filter of "everything
    // out does not hold" would send every DOWN ride to a statement that answers
    // `closed_gap` — and that signal may not be worded as reported. A query
    // timing out would restate what the operator told us as something we merely
    // noticed.
    query.mockImplementation((_sql: string, params: unknown[]) =>
      Array.isArray(params) && params[3] === PARK.id
        ? Promise.resolve([])
        : Promise.reject(new Error("statement timeout")),
    );

    await service.getCurrentOutages(PARK, [OPEN_RIDE, DOWN_RIDE]);

    const closureCall = query.mock.calls.find((c) => c[1][3] === PARK.id);
    expect(closureCall).toBeDefined();
    expect(closureCall![1][0]).toEqual([OPEN_RIDE.id]);
  });

  it("a failed curve read costs the estimate, not the outage line", async () => {
    // The same guard reached from inside the try: letting loadCurves() escape
    // would drop the rows the DOWN query just placed and, with `out` empty,
    // hand those rides to the closure statement after all.
    curves.find.mockRejectedValue(new Error("statement timeout"));
    query.mockImplementation((_sql: string, params: unknown[]) =>
      Array.isArray(params) && params[3] === PARK.id
        ? Promise.resolve([])
        : Promise.resolve([
            {
              attractionId: DOWN_RIDE.id,
              startedAt: new Date("2026-09-08T09:00:00Z"),
              startObserved: true,
              rowsInRun: 3,
              elapsedOperatingMinutes: 45,
              hasWindows: true,
            },
          ]),
    );

    const out = await service.getCurrentOutages(PARK, [OPEN_RIDE, DOWN_RIDE]);

    expect(out.get(DOWN_RIDE.id)?.signal).toBe("down");
    expect(out.get(DOWN_RIDE.id)?.estimate).toBeUndefined();
    const closureCall = query.mock.calls.find((c) => c[1][3] === PARK.id);
    expect(closureCall![1][0]).toEqual([OPEN_RIDE.id]);
  });

  it("an inferred closure is never dressed as a reported one", async () => {
    // The load-bearing line, and nothing exercised it: every other test in this
    // file resolves the closure query to []. `signal` travels to the page, and
    // `closed_gap` must read „steht still" rather than „gemeldet". `estimate`
    // must stay absent too — the curve for this signal would be fit on a
    // population defined by having recovered, so it can only err towards "it
    // will be back soon", and the estimate copy has no signal variant.
    curves.find.mockResolvedValue([
      { parkId: null, signal: "down", elapsedMinutes: 0, medianRemaining: 30 },
      {
        parkId: null,
        signal: "closed_gap",
        elapsedMinutes: 0,
        medianRemaining: 30,
      },
    ]);
    query.mockImplementation((_sql: string, params: unknown[]) =>
      Array.isArray(params) && params[3] === PARK.id
        ? Promise.resolve([
            {
              attractionId: OPEN_RIDE.id,
              startedAt: new Date("2026-09-08T11:20:00Z"),
            },
          ])
        : Promise.resolve([]),
    );

    const out = await service.getCurrentOutages(PARK, [OPEN_RIDE]);

    const row = out.get(OPEN_RIDE.id);
    expect(row?.signal).toBe("closed_gap");
    expect(row?.startObserved).toBe(true);
    expect(row?.estimate).toBeUndefined();
  });

  it("a closure never overwrites a reported outage already placed", async () => {
    // The closure statement is only asked about rides the DOWN query did not
    // place, so this should be unreachable — but `out.set` would overwrite
    // silently if it ever were, turning a reported outage into an inferred one.
    query.mockImplementation((_sql: string, params: unknown[]) =>
      Array.isArray(params) && params[3] === PARK.id
        ? Promise.resolve([
            {
              attractionId: DOWN_RIDE.id,
              startedAt: new Date("2026-09-08T11:20:00Z"),
            },
          ])
        : Promise.resolve([
            {
              attractionId: DOWN_RIDE.id,
              startedAt: new Date("2026-09-08T09:00:00Z"),
              startObserved: true,
              rowsInRun: 3,
              elapsedOperatingMinutes: 45,
              hasWindows: true,
            },
          ]),
    );

    const out = await service.getCurrentOutages(PARK, [OPEN_RIDE, DOWN_RIDE]);

    expect(out.get(DOWN_RIDE.id)?.signal).toBe("down");
  });

  describe("the recovery window", () => {
    // The curve at 45 elapsed minutes, as measured: p25 25, p75 192.
    const CURVE = [
      {
        parkId: null,
        signal: "down",
        elapsedMinutes: 45,
        atRisk: 44653,
        recoveryWithin30: "0.317",
        recoveryWithin60: "0.504",
        remainingP25: 25,
        remainingMedian: 60,
        remainingP75: 192,
      },
    ];
    const ASOF = new Date("2026-09-09T13:00:00.000Z");
    const DOWN_ROW = {
      attractionId: DOWN_RIDE.id,
      startedAt: new Date("2026-09-09T11:00:00Z"),
      startObserved: true,
      rowsInRun: 3,
      elapsedOperatingMinutes: 45,
      hasWindows: true,
    };
    /** The window query is the only one that selects `opensAt`. */
    const isWindowQuery = (sql: string) => sql.includes('AS "opensAt"');

    const route = (windowRows: unknown[] | Error) =>
      query.mockImplementation((sql: string, params: unknown[]) => {
        if (isWindowQuery(sql)) {
          return windowRows instanceof Error
            ? Promise.reject(windowRows)
            : Promise.resolve(windowRows);
        }
        if (Array.isArray(params) && params[3] === PARK.id) {
          return Promise.resolve([]); // closure statement
        }
        return Promise.resolve([DOWN_ROW]);
      });

    const OPEN_TODAY = {
      opensAt: new Date("2026-09-09T08:00:00.000Z"),
      closesAt: new Date("2026-09-09T18:00:00.000Z"),
    };

    beforeEach(() => curves.find.mockResolvedValue(CURVE));

    it("asks the calendar for the park, from now, and answers in instants", async () => {
      route([OPEN_TODAY]);

      const out = await service.getCurrentOutages(
        PARK,
        [OPEN_RIDE, DOWN_RIDE],
        ASOF,
      );

      const windowCall = query.mock.calls.find((c) => isWindowQuery(c[0]));
      expect(windowCall).toBeDefined();
      expect(windowCall![1][0]).toEqual([PARK.id]);
      expect(windowCall![1][1]).toEqual(ASOF);
      // The far end is a horizon, not another "now": a park that opens only at
      // weekends needs days of calendar to place a quartile at all.
      expect((windowCall![1][2] as Date).getTime()).toBeGreaterThan(
        ASOF.getTime() + 7 * 24 * 60 * 60 * 1000,
      );

      expect(out.get(DOWN_RIDE.id)?.estimate?.recoveryWindow).toEqual({
        from: "2026-09-09T13:25:00.000Z",
        to: "2026-09-09T16:12:00.000Z",
      });
    });

    it("keeps the operating-minute pair beside it, unchanged", async () => {
      // The frontend renders `remaining` until it moves over, and this feature
      // may not take that away while it is the only thing being rendered.
      route([OPEN_TODAY]);

      const out = await service.getCurrentOutages(PARK, [DOWN_RIDE], ASOF);

      expect(out.get(DOWN_RIDE.id)?.estimate?.remaining).toEqual({
        p25: 25,
        median: 60,
        p75: 192,
      });
    });

    it("a failing calendar read costs the window, not the estimate or the line", async () => {
      // Same posture as the curve read one test up: this is an addition on top
      // of an addition, and it may not cost the sentence underneath it.
      route(new Error("statement timeout"));

      const out = await service.getCurrentOutages(PARK, [DOWN_RIDE], ASOF);

      const outage = out.get(DOWN_RIDE.id);
      expect(outage?.signal).toBe("down");
      expect(outage?.estimate?.recoveryWithin30).toBeCloseTo(0.317);
      expect(outage?.estimate?.recoveryWindow).toBeUndefined();
    });

    it("does not ask the calendar for a park that publishes no hours", async () => {
      // `hasWindows` false means there is no schedule row to find, so the query
      // would be a round trip for an empty result on every one of those parks.
      query.mockImplementation((sql: string, params: unknown[]) => {
        if (isWindowQuery(sql)) return Promise.resolve([]);
        if (Array.isArray(params) && params[3] === PARK.id) {
          return Promise.resolve([]);
        }
        return Promise.resolve([{ ...DOWN_ROW, hasWindows: false }]);
      });

      const out = await service.getCurrentOutages(PARK, [DOWN_RIDE], ASOF);

      expect(query.mock.calls.some((c) => isWindowQuery(c[0]))).toBe(false);
      expect(out.get(DOWN_RIDE.id)?.estimate).toBeUndefined();
    });

    it("reads the calendar once for two renders of the same park", async () => {
      route([OPEN_TODAY]);

      await service.getCurrentOutages(PARK, [DOWN_RIDE], ASOF);
      await service.getCurrentOutages(
        PARK,
        [DOWN_RIDE],
        new Date(ASOF.getTime() + 60_000),
      );

      expect(query.mock.calls.filter((c) => isWindowQuery(c[0]))).toHaveLength(
        1,
      );
    });

    it("re-reads it for an `asOf` outside the cached list's reach", async () => {
      // A spec pinning a fixed instant, or a run that outlived the TTL. The
      // cache is keyed on the park, so without this check the second answer
      // would be a calendar fetched for a different day.
      route([OPEN_TODAY]);

      await service.getCurrentOutages(PARK, [DOWN_RIDE], ASOF);
      await service.getCurrentOutages(
        PARK,
        [DOWN_RIDE],
        new Date(ASOF.getTime() + 6 * 60 * 60 * 1000),
      );

      expect(query.mock.calls.filter((c) => isWindowQuery(c[0]))).toHaveLength(
        2,
      );
    });
  });

  it("asks nothing at all for a park that cannot emit DOWN", async () => {
    // From configuration, not from the outcome: no wiki_entity_id means no
    // source here produces the status, so there is nothing to ask about.
    await service.getCurrentOutages({ ...PARK, wikiEntityId: null }, [
      OPEN_RIDE,
    ]);

    expect(query).not.toHaveBeenCalled();
  });
});
