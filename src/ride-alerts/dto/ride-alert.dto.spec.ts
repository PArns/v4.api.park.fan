import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import { CreateRideAlertDto, DeleteRideAlertDto } from "./ride-alert.dto";

/**
 * `attractionId` used to be `@IsString() @IsNotEmpty()` only, so a value like
 * "gone" passed the DTO layer and reached `Attraction.id` (a `uuid` column) —
 * Postgres answers `22P02 invalid input syntax for type uuid` for that, which
 * is not an `HttpException`, so it fell through to the global exception
 * filter's generic 500 instead of the controller's own 404. A controller unit
 * test that mocks `findAttractionForAlert` never exercises this: the mock
 * returns before any DTO-shaped input reaches a real pipe. This is the layer
 * that actually runs the decorator, the same way a request does.
 */
describe("CreateRideAlertDto", () => {
  const VALID_UUID = "3f3e6a10-3b1a-4c1f-9e2e-2b6a2f6c9d10";

  it("rejects a non-UUID attractionId", async () => {
    const dto = plainToInstance(CreateRideAlertDto, {
      endpoint: "https://fcm.googleapis.com/fcm/send/e1",
      attractionId: "gone",
      thresholdMinutes: 20,
    });
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === "attractionId")).toBe(true);
  });

  it("accepts a well-formed UUID attractionId", async () => {
    const dto = plainToInstance(CreateRideAlertDto, {
      endpoint: "https://fcm.googleapis.com/fcm/send/e1",
      attractionId: VALID_UUID,
      thresholdMinutes: 20,
    });
    const errors = await validate(dto);
    expect(errors).toHaveLength(0);
  });
});

describe("CreateRideAlertDto kind", () => {
  const base = {
    endpoint: "https://fcm.googleapis.com/fcm/send/e1",
    attractionId: "3f3e6a10-3b1a-4c1f-9e2e-2b6a2f6c9d10",
  };

  it("accepts a reopen alert without a threshold", async () => {
    const dto = plainToInstance(CreateRideAlertDto, {
      ...base,
      kind: "reopen",
    });
    expect(await validate(dto)).toHaveLength(0);
  });

  it("still requires a threshold when no kind is given", async () => {
    const dto = plainToInstance(CreateRideAlertDto, base);
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === "thresholdMinutes")).toBe(true);
  });

  it("rejects an unknown kind", async () => {
    const dto = plainToInstance(CreateRideAlertDto, {
      ...base,
      kind: "rain",
      thresholdMinutes: 20,
    });
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === "kind")).toBe(true);
  });
});

describe("DeleteRideAlertDto", () => {
  it("rejects a non-UUID attractionId", async () => {
    const dto = plainToInstance(DeleteRideAlertDto, {
      endpoint: "https://fcm.googleapis.com/fcm/send/e1",
      attractionId: "gone",
    });
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === "attractionId")).toBe(true);
  });
});
