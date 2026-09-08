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

  it("asks nothing at all for a park that cannot emit DOWN", async () => {
    // From configuration, not from the outcome: no wiki_entity_id means no
    // source here produces the status, so there is nothing to ask about.
    await service.getCurrentOutages({ ...PARK, wikiEntityId: null }, [
      OPEN_RIDE,
    ]);

    expect(query).not.toHaveBeenCalled();
  });
});
