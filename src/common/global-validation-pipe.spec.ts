import { ValidationPipe, ArgumentMetadata } from "@nestjs/common";
import { TripWriteDto } from "../trips/dto/trip.dto";
import { PushSubscribeDto, PushUnsubscribeDto } from "../push/dto/push.dto";

/**
 * Every request body has to survive the pipe that production actually mounts.
 *
 * `main.ts` uses `whitelist: true` (keep only decorated properties) together
 * with `forbidNonWhitelisted: true` (400 on the rest). A DTO class with no
 * class-validator decorators therefore rejects **its own** fields: the pipe
 * whitelists nothing and answers `["property <x> should not exist"]`. That is
 * not a subtle failure — the endpoint is simply dead — and nothing else in the
 * suite sees it, because controller specs call the handler methods directly and
 * the e2e specs cover other controllers.
 *
 * So this file mounts the real pipe with the real options and pushes a valid
 * body through each write DTO. It is deliberately generic: a new body DTO
 * belongs in the table below on the day it is written.
 */
describe("the global ValidationPipe vs. every request body", () => {
  // Copied from `main.ts` on purpose rather than imported: the point is to
  // notice when the two drift, and a shared constant would hide that. Keep in
  // step with `app.useGlobalPipes(...)`.
  const pipe = new ValidationPipe({
    whitelist: true,
    forbidNonWhitelisted: true,
    transform: true,
    transformOptions: { enableImplicitConversion: true },
  });

  const asBody = (
    metatype: ArgumentMetadata["metatype"],
  ): ArgumentMetadata => ({
    type: "body",
    metatype,
  });

  const bodies: Array<[string, ArgumentMetadata["metatype"], object]> = [
    [
      "POST /v1/trips + PUT /v1/trips/:id",
      TripWriteDto,
      { payload: { version: 1, parks: {} } },
    ],
    [
      "POST /v1/push/subscriptions",
      PushSubscribeDto,
      {
        endpoint: "https://fcm.googleapis.com/fcm/send/abc123",
        p256dh: "BN4GvZ...",
        auth: "k9Qb...",
        tripId: "n7Qk2Fd3Xb9pLmZa",
        locale: "de-AT",
        timezone: "Europe/Berlin",
        topics: ["next-up"],
      },
    ],
    [
      "DELETE /v1/push/subscriptions",
      PushUnsubscribeDto,
      { endpoint: "https://fcm.googleapis.com/fcm/send/abc123" },
    ],
  ];

  it.each(bodies)("accepts a valid body for %s", async (_route, dto, body) => {
    const out = await pipe.transform(body, asBody(dto));
    // Not just "did not throw": `whitelist` silently DROPS an undecorated
    // property, so a body that survives with its fields stripped would reach
    // the handler as `{}` and fail there instead.
    for (const key of Object.keys(body)) {
      expect(out).toHaveProperty(key);
    }
  });

  it("still refuses a body carrying an unknown property", async () => {
    await expect(
      pipe.transform(
        { payload: { version: 1, parks: {} }, admin: true },
        asBody(TripWriteDto),
      ),
    ).rejects.toThrow();
  });

  it("still refuses a payload that is not an object", async () => {
    await expect(
      pipe.transform({ payload: "a string" }, asBody(TripWriteDto)),
    ).rejects.toThrow();
  });

  it("refuses a subscription with a missing key rather than storing half of one", async () => {
    await expect(
      pipe.transform(
        { endpoint: "https://fcm.googleapis.com/fcm/send/abc123" },
        asBody(PushSubscribeDto),
      ),
    ).rejects.toThrow();
  });
});
