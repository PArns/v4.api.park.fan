import { WartezeitenScheduleProcessor } from "./wartezeiten-schedule.processor";

/**
 * `fillScheduleGaps` writes an UNKNOWN placeholder for today, so "today has a
 * row" is not "the Wiki published today's hours". Counting the placeholder as
 * Wiki data skipped the Wartezeiten fallback for exactly the parks it exists for.
 */
describe("WartezeitenScheduleProcessor — Wiki-park fallback", () => {
  const park = {
    id: "park-1",
    name: "Park",
    wikiEntityId: "wiki-1",
    wartezeitenEntityId: "wz-1",
  };

  const build = (todayRows: Array<{ scheduleType: string }>) => {
    const parksService = {
      getTodaySchedule: jest.fn().mockResolvedValue(todayRows),
      saveScheduleData: jest.fn().mockResolvedValue(1),
      recordScheduleVersions: jest.fn().mockResolvedValue(0),
    };
    const client = {
      getOpeningTimes: jest.fn().mockResolvedValue([
        {
          opened_today: true,
          open_from: "2026-10-20T09:00:00+02:00",
          closed_from: "2026-10-20T18:00:00+02:00",
        },
      ]),
    };
    const parkRepository = {
      createQueryBuilder: () => ({
        where: () => ({ getMany: async () => [park] }),
      }),
    };
    const processor = new WartezeitenScheduleProcessor(
      client as never,
      parksService as never,
      parkRepository as never,
    );
    return { processor, parksService };
  };

  it("writes Wartezeiten hours when today holds only the gap-fill's UNKNOWN row", async () => {
    const { processor, parksService } = build([{ scheduleType: "UNKNOWN" }]);

    await processor.handleFetchOpeningTimes({} as never);

    expect(parksService.saveScheduleData).toHaveBeenCalledTimes(1);
    expect(parksService.recordScheduleVersions).toHaveBeenCalledWith(
      park.id,
      "wartezeiten",
    );
  });

  it("leaves a park alone whose Wiki schedule already answered today", async () => {
    const { processor, parksService } = build([{ scheduleType: "OPERATING" }]);

    await processor.handleFetchOpeningTimes({} as never);

    expect(parksService.saveScheduleData).not.toHaveBeenCalled();
  });
});
