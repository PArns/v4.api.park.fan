import { Test } from "@nestjs/testing";
import { getRepositoryToken } from "@nestjs/typeorm";
import { FindOperator } from "typeorm";
import {
  RideAlertsService,
  type ParkForRideAlertCheck,
} from "./ride-alerts.service";
import { RideAlert } from "./entities/ride-alert.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { QueueDataService } from "../queue-data/queue-data.service";
import { PushService } from "../push/push.service";
import {
  LiveStatus,
  QueueType,
} from "../external-apis/themeparks/themeparks.types";

/**
 * `checkAndNotify` is where every rule this feature depends on actually
 * meets — the no-live-wait-times gate, the seasonal `!== false` rule, the
 * heartbeat/staleness exclusion, the edge-trigger armed/disarm and its
 * race guard, and the day-boundary re-arm. None of it was exercised before:
 * `wait-times.processor.spec.ts` mocks this service out entirely, and
 * `diffRideAlerts`'s own tests know nothing about the database or the
 * season/wait-time gates around it.
 */
describe("RideAlertsService", () => {
  const PHANTASIALAND: ParkForRideAlertCheck = {
    name: "Phantasialand",
    slug: "phantasialand",
    citySlug: "bruehl",
    countrySlug: "germany",
    continentSlug: "europe",
    timezone: "Europe/Berlin",
  };

  // 2026-06-15 12:00 UTC — 14:00 in Berlin (CEST), well inside a June day.
  const NOW_MS = Date.parse("2026-06-15T12:00:00.000Z");

  function matchWhere(
    row: Record<string, unknown>,
    where: Record<string, unknown>,
  ): boolean {
    return Object.entries(where).every(([key, condition]) => {
      if (condition instanceof FindOperator) {
        if (condition.type === "in") {
          return (condition.value as unknown[]).includes(row[key]);
        }
        return true;
      }
      return row[key] === condition;
    });
  }

  let alertRows: Map<string, RideAlert>;
  let attractionRows: Map<string, Attraction>;
  let alertRepo: {
    find: jest.Mock;
    findOne: jest.Mock;
    update: jest.Mock;
    upsert: jest.Mock;
    create: jest.Mock;
    save: jest.Mock;
  };
  let queueDataService: { findCurrentStatusByAttractionIds: jest.Mock };
  let pushService: { findByIds: jest.Mock; send: jest.Mock };
  let service: RideAlertsService;

  const alert = (overrides: Partial<RideAlert> = {}): RideAlert =>
    ({
      id: "alert-1",
      subscriptionId: "sub-1",
      attractionId: "ride-1",
      thresholdMinutes: 20,
      armed: true,
      lastTriggeredAt: null,
      createdAt: new Date("2026-06-01T00:00:00.000Z"),
      updatedAt: new Date("2026-06-01T00:00:00.000Z"),
      ...overrides,
    }) as RideAlert;

  const attraction = (overrides: Partial<Attraction> = {}): Attraction =>
    ({
      id: "ride-1",
      name: "Taron",
      slug: "taron",
      isSeasonal: false,
      seasonMonths: null,
      seasonOutSince: null,
      ...overrides,
    }) as unknown as Attraction;

  beforeEach(async () => {
    alertRows = new Map();
    attractionRows = new Map([["ride-1", attraction()]]);

    alertRepo = {
      find: jest.fn(async (opts: { where: Record<string, unknown> }) =>
        [...alertRows.values()].filter((row) =>
          matchWhere(row as unknown as Record<string, unknown>, opts.where),
        ),
      ),
      findOne: jest.fn(async (opts: { where: Record<string, unknown> }) => {
        for (const row of alertRows.values()) {
          if (matchWhere(row as unknown as Record<string, unknown>, opts.where))
            return row;
        }
        return null;
      }),
      update: jest.fn(
        async (
          criteria: string | Record<string, unknown>,
          partial: Partial<RideAlert>,
        ) => {
          const matched =
            typeof criteria === "string"
              ? [alertRows.get(criteria)].filter((r): r is RideAlert => !!r)
              : [...alertRows.values()].filter((row) =>
                  matchWhere(
                    row as unknown as Record<string, unknown>,
                    criteria,
                  ),
                );
          for (const row of matched) Object.assign(row, partial);
          return { affected: matched.length };
        },
      ),
      create: jest.fn((data: Partial<RideAlert>) => ({ ...data }) as RideAlert),
      save: jest.fn(async (row: RideAlert) => {
        const stored = { ...row, id: row.id ?? `alert-${alertRows.size + 1}` };
        alertRows.set(stored.id, stored as RideAlert);
        return stored;
      }),
      // A stand-in for `INSERT ... ON CONFLICT (conflictPaths) DO UPDATE`:
      // finds the row by the conflict columns and merges the given fields
      // into it (real Postgres only touches columns actually present in the
      // entity, which is exactly what `Object.assign` does here), or creates
      // a fresh one.
      upsert: jest.fn(
        async (
          entity: Partial<RideAlert>,
          options: { conflictPaths: string[] },
        ) => {
          const existing = [...alertRows.values()].find((row) =>
            options.conflictPaths.every(
              (key) =>
                (row as unknown as Record<string, unknown>)[key] ===
                (entity as unknown as Record<string, unknown>)[key],
            ),
          );
          if (existing) {
            Object.assign(existing, entity);
          } else {
            const stored = {
              id: `alert-${alertRows.size + 1}`,
              lastTriggeredAt: null,
              createdAt: new Date(),
              ...entity,
            };
            alertRows.set(stored.id, stored as RideAlert);
          }
          return { raw: [], identifiers: [], generatedMaps: [] };
        },
      ),
    };

    const attractionRepo = {
      find: jest.fn(async (opts: { where: Record<string, unknown> }) =>
        [...attractionRows.values()].filter((row) =>
          matchWhere(row as unknown as Record<string, unknown>, opts.where),
        ),
      ),
    };

    queueDataService = {
      findCurrentStatusByAttractionIds: jest.fn().mockResolvedValue(new Map()),
    };
    pushService = {
      findByIds: jest.fn().mockResolvedValue(new Map()),
      send: jest.fn().mockResolvedValue(true),
    };

    const moduleRef = await Test.createTestingModule({
      providers: [
        RideAlertsService,
        { provide: getRepositoryToken(RideAlert), useValue: alertRepo },
        { provide: getRepositoryToken(Attraction), useValue: attractionRepo },
        { provide: QueueDataService, useValue: queueDataService },
        { provide: PushService, useValue: pushService },
      ],
    }).compile();

    service = moduleRef.get(RideAlertsService);
  });

  const standbyReading = (
    overrides: Partial<{
      status: LiveStatus;
      waitTime: number | null;
      isHeartbeat: boolean | null;
    }> = {},
  ) => [
    {
      queueType: QueueType.STANDBY,
      status: LiveStatus.OPERATING,
      waitTime: 10,
      isHeartbeat: false,
      ...overrides,
    },
  ];

  describe("checkAndNotify", () => {
    it("does nothing when no attractions were polled", async () => {
      await service.checkAndNotify(PHANTASIALAND, [], NOW_MS);
      expect(
        queueDataService.findCurrentStatusByAttractionIds,
      ).not.toHaveBeenCalled();
    });

    it("never reads a park with no live wait-time source", async () => {
      alertRows.set("alert-1", alert());
      const hansaPark: ParkForRideAlertCheck = {
        name: "Hansa-Park",
        slug: "hansa-park",
        citySlug: "sierksdorf",
        countrySlug: "germany",
        continentSlug: "europe",
        timezone: "Europe/Berlin",
      };
      await service.checkAndNotify(hansaPark, ["ride-1"], NOW_MS);
      expect(
        queueDataService.findCurrentStatusByAttractionIds,
      ).not.toHaveBeenCalled();
      expect(pushService.send).not.toHaveBeenCalled();
    });

    it("does nothing when none of the polled rides have an alert", async () => {
      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);
      expect(
        queueDataService.findCurrentStatusByAttractionIds,
      ).not.toHaveBeenCalled();
    });

    it("sends and disarms when the wait drops below the threshold", async () => {
      alertRows.set("alert-1", alert({ thresholdMinutes: 20, armed: true }));
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([["ride-1", standbyReading({ waitTime: 12 })]]),
      );
      pushService.findByIds.mockResolvedValueOnce(
        new Map([["sub-1", { id: "sub-1", locale: "de" }]]),
      );

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);

      expect(pushService.send).toHaveBeenCalledTimes(1);
      expect(alertRows.get("alert-1")!.armed).toBe(false);
      expect(alertRows.get("alert-1")!.lastTriggeredAt).not.toBeNull();
    });

    it("does not re-send while the wait stays below an already-fired alert", async () => {
      alertRows.set(
        "alert-1",
        alert({
          thresholdMinutes: 20,
          armed: false,
          lastTriggeredAt: NOW_MS ? new Date(NOW_MS) : null,
        }),
      );
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValue(
        new Map([["ride-1", standbyReading({ waitTime: 10 })]]),
      );

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);
      expect(pushService.send).not.toHaveBeenCalled();
    });

    it("re-arms without sending once the wait recovers above the threshold", async () => {
      alertRows.set(
        "alert-1",
        alert({
          thresholdMinutes: 20,
          armed: false,
          lastTriggeredAt: new Date(NOW_MS),
        }),
      );
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([["ride-1", standbyReading({ waitTime: 25 })]]),
      );

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);
      expect(pushService.send).not.toHaveBeenCalled();
      expect(alertRows.get("alert-1")!.armed).toBe(true);
    });

    it("skips a heartbeat row rather than treating it as a fresh observation", async () => {
      alertRows.set("alert-1", alert({ thresholdMinutes: 20, armed: true }));
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([
          ["ride-1", standbyReading({ waitTime: 5, isHeartbeat: true })],
        ]),
      );

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);
      expect(pushService.send).not.toHaveBeenCalled();
    });

    it("skips an attraction the detector has confirmed is out of season (=== false)", async () => {
      attractionRows.set(
        "ride-1",
        attraction({
          isSeasonal: true,
          seasonMonths: [11, 12, 1], // winter only; NOW_MS is June
        }),
      );
      alertRows.set("alert-1", alert({ thresholdMinutes: 20, armed: true }));
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([["ride-1", standbyReading({ waitTime: 5 })]]),
      );

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);
      expect(pushService.send).not.toHaveBeenCalled();
    });

    it("does NOT skip when the season verdict is unknown (null) — never === true", async () => {
      // Seasonal, but the detector has not named any months yet (< 330
      // observed days) and no `seasonOutSince` — isCurrentlyInSeason
      // returns null here, and the rule is `!== false`, not `=== true`.
      attractionRows.set(
        "ride-1",
        attraction({
          isSeasonal: true,
          seasonMonths: null,
          seasonOutSince: null,
        }),
      );
      alertRows.set("alert-1", alert({ thresholdMinutes: 20, armed: true }));
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([["ride-1", standbyReading({ waitTime: 5 })]]),
      );
      pushService.findByIds.mockResolvedValueOnce(
        new Map([["sub-1", { id: "sub-1", locale: "de" }]]),
      );

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);
      expect(pushService.send).toHaveBeenCalledTimes(1);
    });

    it("re-arms across a day boundary even though the reading is still below threshold", async () => {
      // Fired yesterday evening; the ride is a walk-on all day today and
      // never again reaches the threshold — without the day-boundary
      // re-arm this alert would never fire again.
      alertRows.set(
        "alert-1",
        alert({
          thresholdMinutes: 20,
          armed: false,
          lastTriggeredAt: new Date("2026-06-14T20:00:00.000Z"),
        }),
      );
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([["ride-1", standbyReading({ waitTime: 5 })]]),
      );
      pushService.findByIds.mockResolvedValueOnce(
        new Map([["sub-1", { id: "sub-1", locale: "de" }]]),
      );

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);

      // Re-armed, saw a below-threshold reading in the SAME cycle, and
      // fired again — the correct behaviour for a genuinely new day.
      expect(pushService.send).toHaveBeenCalledTimes(1);
      expect(alertRows.get("alert-1")!.armed).toBe(false);
    });

    it("leaves an alert triggered earlier TODAY disarmed", async () => {
      alertRows.set(
        "alert-1",
        alert({
          thresholdMinutes: 20,
          armed: false,
          // Same park-local day as NOW_MS (2026-06-15 14:00 Berlin).
          lastTriggeredAt: new Date("2026-06-15T10:00:00.000Z"),
        }),
      );
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValue(
        new Map([["ride-1", standbyReading({ waitTime: 5 })]]),
      );

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);
      expect(pushService.send).not.toHaveBeenCalled();
      expect(alertRows.get("alert-1")!.armed).toBe(false);
    });

    it("skips sending when another sweep already disarmed the alert (the race the guard closes)", async () => {
      // `alerts` is read once and handed to `diffRideAlerts` as a snapshot;
      // simulate a second, overlapping sweep having already disarmed the
      // SAME alert (and written it to the store) between that read and this
      // call's own attempt to disarm-and-send.
      alertRows.set("alert-1", alert({ thresholdMinutes: 20, armed: true }));
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([["ride-1", standbyReading({ waitTime: 12 })]]),
      );
      alertRepo.find.mockImplementationOnce(async (opts) => {
        const rows = [...alertRows.values()].filter((row) =>
          matchWhere(row as unknown as Record<string, unknown>, opts.where),
        );
        const snapshot = rows.map((r) => ({ ...r }));
        // The "other sweep" wins the race right after we took our snapshot.
        alertRows.get("alert-1")!.armed = false;
        return snapshot;
      });

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);
      expect(pushService.send).not.toHaveBeenCalled();
    });

    it("re-arms rather than silently dropping the alert when delivery fails", async () => {
      alertRows.set("alert-1", alert({ thresholdMinutes: 20, armed: true }));
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([["ride-1", standbyReading({ waitTime: 12 })]]),
      );
      pushService.findByIds.mockResolvedValueOnce(
        new Map([["sub-1", { id: "sub-1", locale: "de" }]]),
      );
      pushService.send.mockResolvedValueOnce(false);

      await service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS);

      expect(alertRows.get("alert-1")!.armed).toBe(true);
    });

    it("never throws — a bad reading must not fail the park's poll cycle", async () => {
      alertRows.set("alert-1", alert());
      queueDataService.findCurrentStatusByAttractionIds.mockRejectedValueOnce(
        new Error("redis down"),
      );
      await expect(
        service.checkAndNotify(PHANTASIALAND, ["ride-1"], NOW_MS),
      ).resolves.toBeUndefined();
    });
  });

  describe("upsert", () => {
    it("arms a fresh alert when the current wait is already at or above the threshold", async () => {
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([["ride-1", standbyReading({ waitTime: 25 })]]),
      );
      const result = await service.upsert("sub-1", "ride-1", 20);
      expect(result.armed).toBe(true);
    });

    it("does NOT arm a fresh alert whose threshold the current wait already satisfies", async () => {
      // Setting "under 20 min" while the ride already reads 10 must not fire
      // on the very next poll about the number just seen on screen.
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([["ride-1", standbyReading({ waitTime: 10 })]]),
      );
      const result = await service.upsert("sub-1", "ride-1", 20);
      expect(result.armed).toBe(false);
    });

    it("arms when there is no current reading to compare against", async () => {
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map(),
      );
      const result = await service.upsert("sub-1", "ride-1", 20);
      expect(result.armed).toBe(true);
    });

    it("does not arm against a heartbeat row", async () => {
      queueDataService.findCurrentStatusByAttractionIds.mockResolvedValueOnce(
        new Map([
          ["ride-1", standbyReading({ waitTime: 10, isHeartbeat: true })],
        ]),
      );
      const result = await service.upsert("sub-1", "ride-1", 20);
      expect(result.armed).toBe(true);
    });

    it("never 500s on two concurrent upserts of the same ride alert (a double-tap)", async () => {
      // The old read-then-write shape had both calls pass `findOne` before
      // either wrote, so the second `save()` hit the unique index. The real
      // `ON CONFLICT DO UPDATE` this now issues makes both calls land safely
      // regardless of ordering.
      const [first, second] = await Promise.all([
        service.upsert("sub-1", "ride-1", 20),
        service.upsert("sub-1", "ride-1", 25),
      ]);
      expect(first.attractionId).toBe("ride-1");
      expect(second.attractionId).toBe("ride-1");
      expect(alertRows.size).toBe(1);
    });
  });
});
