import { Test, TestingModule } from "@nestjs/testing";
import { Job } from "bull";
import { ParksService } from "../../parks/parks.service";
import { ParkDayOperationService } from "../../parks/services/park-day-operation.service";
import { ParkDayOperationProcessor } from "./park-day-operation.processor";

/**
 * PAR-697. The processor decides WHICH days get judged, and both of its
 * decisions are easy to get wrong in a way no reader would notice: yesterday in
 * the park's own timezone rather than in UTC, and one park's failure costing
 * only that park its row.
 */
describe("ParkDayOperationProcessor", () => {
  let processor: ParkDayOperationProcessor;
  let parksService: { findAll: jest.Mock; findById: jest.Mock };
  let verdicts: { computeRange: jest.Mock };

  const job = {} as Job;

  /** Two parks a day apart in local time at most UTC hours. */
  const parks = [
    { id: "p-nz", slug: "rainbows-end", timezone: "Pacific/Auckland" },
    { id: "p-la", slug: "knotts-berry-farm", timezone: "America/Los_Angeles" },
  ];

  beforeEach(async () => {
    parksService = {
      findAll: jest.fn().mockResolvedValue(parks),
      findById: jest.fn(),
    };
    verdicts = {
      computeRange: jest
        .fn()
        .mockResolvedValue({ daysJudged: 1, operatingDays: 0 }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        ParkDayOperationProcessor,
        { provide: ParksService, useValue: parksService },
        { provide: ParkDayOperationService, useValue: verdicts },
      ],
    }).compile();

    processor = module.get(ParkDayOperationProcessor);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("asks each park for its own yesterday, not for a shared UTC one", async () => {
    // 2026-10-07 11:00 UTC: already the 8th in Auckland (+13), still the 7th in
    // Los Angeles (−7). So their last finished days are different dates.
    jest.useFakeTimers().setSystemTime(new Date("2026-10-07T11:00:00Z"));

    await processor.handleYesterday(job);

    expect(verdicts.computeRange.mock.calls).toEqual([
      [parks[0], "2026-10-07", "2026-10-07"],
      [parks[1], "2026-10-06", "2026-10-06"],
    ]);
  });

  it("keeps going when one park's aggregate fails", async () => {
    verdicts.computeRange
      .mockRejectedValueOnce(new Error("canceling statement due to timeout"))
      .mockResolvedValueOnce({ daysJudged: 1, operatingDays: 1 });

    await expect(processor.handleYesterday(job)).resolves.toBeUndefined();

    expect(verdicts.computeRange).toHaveBeenCalledTimes(2);
  });

  it("fills an explicit range for every park", async () => {
    await processor.handleBackfill({
      data: { fromDate: "2026-04-01", toDate: "2026-04-30" },
    } as Job<{ parkId?: string; fromDate: string; toDate: string }>);

    expect(verdicts.computeRange.mock.calls).toEqual([
      [parks[0], "2026-04-01", "2026-04-30"],
      [parks[1], "2026-04-01", "2026-04-30"],
    ]);
  });

  it("fills one park when the job names one", async () => {
    parksService.findById.mockResolvedValue(parks[1]);

    await processor.handleBackfill({
      data: { parkId: "p-la", fromDate: "2026-04-01", toDate: "2026-04-02" },
    } as Job<{ parkId?: string; fromDate: string; toDate: string }>);

    expect(parksService.findAll).not.toHaveBeenCalled();
    expect(verdicts.computeRange.mock.calls).toEqual([
      [parks[1], "2026-04-01", "2026-04-02"],
    ]);
  });

  it("writes nothing when the named park does not exist", async () => {
    parksService.findById.mockResolvedValue(null);

    await processor.handleBackfill({
      data: { parkId: "gone", fromDate: "2026-04-01", toDate: "2026-04-02" },
    } as Job<{ parkId?: string; fromDate: string; toDate: string }>);

    expect(verdicts.computeRange).not.toHaveBeenCalled();
  });
});
