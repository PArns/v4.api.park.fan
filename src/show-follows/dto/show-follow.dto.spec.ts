import { plainToInstance } from "class-transformer";
import { validate } from "class-validator";
import { CreateShowFollowDto, DeleteShowFollowDto } from "./show-follow.dto";

/**
 * Same reasoning as `ride-alert.dto.spec.ts`: `showId` reaches `Show.id`, a
 * `uuid` column, so a non-UUID value must be refused at the DTO layer rather
 * than reach Postgres and surface as a generic 500.
 */
describe("CreateShowFollowDto", () => {
  const VALID_UUID = "3f3e6a10-3b1a-4c1f-9e2e-2b6a2f6c9d10";

  it("rejects a non-UUID showId", async () => {
    const dto = plainToInstance(CreateShowFollowDto, {
      endpoint: "https://fcm.googleapis.com/fcm/send/e1",
      showId: "gone",
    });
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === "showId")).toBe(true);
  });

  it("accepts a well-formed UUID showId", async () => {
    const dto = plainToInstance(CreateShowFollowDto, {
      endpoint: "https://fcm.googleapis.com/fcm/send/e1",
      showId: VALID_UUID,
    });
    const errors = await validate(dto);
    expect(errors).toHaveLength(0);
  });
});

describe("DeleteShowFollowDto", () => {
  it("rejects a non-UUID showId", async () => {
    const dto = plainToInstance(DeleteShowFollowDto, {
      endpoint: "https://fcm.googleapis.com/fcm/send/e1",
      showId: "gone",
    });
    const errors = await validate(dto);
    expect(errors.some((e) => e.property === "showId")).toBe(true);
  });
});
