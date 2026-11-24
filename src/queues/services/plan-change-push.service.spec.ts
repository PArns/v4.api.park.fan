import { PlanChangePushService } from "./plan-change-push.service";

let apnsOn = true;
jest.mock("../../push/apns", () => ({
  ...jest.requireActual("../../push/apns"),
  isApnsConfigured: () => apnsOn,
}));

// 2026-10-17 12:00 Europe/Berlin.
const NOW = Date.parse("2026-10-17T10:00:00Z");
const at = (hhmm: string) => new Date(`2026-10-17T${hhmm}:00+02:00`);

const PAYLOAD = {
  version: 1,
  parks: {
    phantasialand: {
      slug: "phantasialand",
      name: "Phantasialand",
      timezone: "Europe/Berlin",
      geo: { continent: "europe", country: "germany", city: "bruehl" },
      days: {
        "2026-10-17": {
          date: "2026-10-17",
          entries: [{ id: "e1", startMinute: 900, custom: { label: "Pause" } }],
        },
      },
    },
  },
};

describe("PlanChangePushService", () => {
  let store: Map<string, string>;
  let schedule: Array<Record<string, unknown>>;
  let devices: Array<Record<string, unknown>>;
  let send: jest.Mock;
  let parksService: Record<string, jest.Mock>;
  let service: PlanChangePushService;

  beforeEach(() => {
    apnsOn = true;
    store = new Map();
    schedule = [
      {
        scheduleType: "OPERATING",
        openingTime: at("09:00"),
        closingTime: at("18:00"),
      },
    ];
    devices = [
      {
        id: "d1",
        endpoint: "apns:1",
        apnsToken: "ab",
        tripId: "t1",
        topics: ["plan-change"],
        locale: "de",
        timezone: "Europe/Berlin",
      },
    ];
    send = jest.fn().mockResolvedValue(true);
    parksService = {
      findByGeographicPath: jest.fn(async () => ({
        id: "p1",
        timezone: "Europe/Berlin",
      })),
      getScheduleForDate: jest.fn(async () => schedule),
    };
    service = new PlanChangePushService(
      {
        devicesWithTripAndKind: jest.fn(async () => devices),
        send,
      } as never,
      { find: jest.fn(async () => ({ payload: PAYLOAD })) } as never,
      parksService as never,
      {} as never,
      { findCurrentStatusByAttractionIds: jest.fn() } as never,
      {
        get: jest.fn(async (k: string) => store.get(k) ?? null),
        set: jest.fn(async (k: string, v: string) => store.set(k, v)),
      } as never,
    );
  });

  const closeEarly = () => {
    schedule = [{ ...schedule[0], closingTime: at("17:00") }];
  };

  it("does nothing without a device that chose it", async () => {
    devices = [];
    await expect(service.run(NOW)).resolves.toBe(0);
    expect(parksService.findByGeographicPath).not.toHaveBeenCalled();
  });

  it("starts a baseline in silence, then sends one digest per change", async () => {
    await expect(service.run(NOW)).resolves.toBe(0);
    closeEarly();
    await expect(service.run(NOW + 5 * 60_000)).resolves.toBe(1);
    expect(send).toHaveBeenCalledWith(
      devices[0],
      expect.objectContaining({
        kind: "plan-change",
        tag: "plan-change:phantasialand:2026-10-17",
        body: expect.stringContaining("09:00–17:00"),
        url: "/trip-planner?park=europe%2Fgermany%2Fbruehl%2Fphantasialand&date=2026-10-17",
      }),
    );
    await expect(service.run(NOW + 60 * 60_000)).resolves.toBe(0);
  });

  it("holds a change back in quiet hours and sends it after", async () => {
    devices[0].timezone = "Pacific/Auckland"; // 23:00 there
    await service.run(NOW);
    closeEarly();
    await expect(service.run(NOW + 5 * 60_000)).resolves.toBe(0);
    devices[0].timezone = "Europe/Berlin";
    await expect(service.run(NOW + 10 * 60_000)).resolves.toBe(1);
  });

  it("reads nothing while APNs is not configured", async () => {
    apnsOn = false;
    await expect(service.run(NOW)).resolves.toBe(0);
    expect(parksService.findByGeographicPath).not.toHaveBeenCalled();
  });

  it("gives one trip at most its share of the tick", async () => {
    const days: Record<string, unknown> = {};
    for (let i = 0; i < 60; i++) {
      const date = new Date(NOW + i * 86_400_000).toISOString().slice(0, 10);
      days[date] = { entries: [{ id: "e1", startMinute: 900 }] };
    }
    const park = PAYLOAD.parks.phantasialand;
    (service as unknown as { tripsService: unknown }).tripsService = {
      find: async () => ({ payload: { parks: { p: { ...park, days } } } }),
    };
    await service.run(NOW);
    expect(parksService.getScheduleForDate).toHaveBeenCalledTimes(
      PlanChangePushService.MAX_PLAN_DAYS_PER_TRIP,
    );
  });

  it("stops when the tick's time is spent", async () => {
    const realNow = Date.now;
    let clock = realNow();
    jest.spyOn(Date, "now").mockImplementation(() => clock);
    parksService.getScheduleForDate.mockImplementation(async () => {
      clock += PlanChangePushService.MAX_RUN_MS + 1;
      return schedule;
    });
    devices.push({ ...devices[0], id: "d2", tripId: "t2" });
    try {
      await service.run(NOW);
    } finally {
      jest.restoreAllMocks();
    }
    expect(parksService.getScheduleForDate).toHaveBeenCalledTimes(1);
  });

  it("isolates a failing day", async () => {
    parksService.getScheduleForDate.mockRejectedValue(new Error("db"));
    await expect(service.run(NOW)).resolves.toBe(0);
  });
});
