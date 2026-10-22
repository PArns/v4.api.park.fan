import { BadRequestException } from "@nestjs/common";
import { ParkSeasonsController } from "./park-seasons.controller";

describe("ParkSeasonsController", () => {
  const path = ["europe", "germany", "bruehl", "phantasialand"] as const;

  function build(timezone = "Europe/Berlin") {
    const parks = {
      findOne: jest.fn(async () => ({ id: "park-1", timezone })),
    };
    const seasons = { forParkInRange: jest.fn(async () => []) };
    const controller = new ParkSeasonsController(
      parks as never,
      seasons as never,
    );
    return { controller, seasons };
  }

  afterEach(() => jest.useRealTimers());

  it("answers 400 for a day the calendar does not have, without calling the service", async () => {
    const { controller, seasons } = build();
    await expect(
      controller.forPark(...path, "2026-02-31"),
    ).rejects.toBeInstanceOf(BadRequestException);
    await expect(
      controller.forPark(...path, undefined, "2026-13-01"),
    ).rejects.toBeInstanceOf(BadRequestException);
    expect(seasons.forParkInRange).not.toHaveBeenCalled();
  });

  it("answers 400 for year 0 and for a repeated parameter", async () => {
    const { controller } = build();
    await expect(
      controller.forPark(...path, "0000-01-01"),
    ).rejects.toBeInstanceOf(BadRequestException);
    await expect(
      controller.forPark(...path, ["2026-01-01", "2026-01-02"] as never),
    ).rejects.toBeInstanceOf(BadRequestException);
  });

  it("treats an empty parameter as absent", async () => {
    const { controller, seasons } = build();
    await controller.forPark(...path, "", "");
    expect(seasons.forParkInRange).toHaveBeenCalledTimes(1);
  });

  it("starts the default range on the park's day, not the UTC day", async () => {
    jest.useFakeTimers().setSystemTime(new Date("2026-10-22T20:00:00Z"));
    const { controller, seasons } = build("Asia/Tokyo");
    const result = await controller.forPark(...path);
    expect(seasons.forParkInRange).toHaveBeenCalledWith(
      "park-1",
      "2026-10-23",
      "2027-10-23",
    );
    expect(result.from).toBe("2026-10-23");
  });

  it("keeps an explicit range as given", async () => {
    const { controller, seasons } = build();
    await controller.forPark(...path, "2026-03-01", "2026-03-31");
    expect(seasons.forParkInRange).toHaveBeenCalledWith(
      "park-1",
      "2026-03-01",
      "2026-03-31",
    );
  });
});
