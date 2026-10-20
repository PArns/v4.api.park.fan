import * as webpush from "web-push";
import { Repository } from "typeorm";
import { PushService } from "./push.service";
import { PushSubscription } from "./entities/push-subscription.entity";

jest.mock("web-push", () => ({ sendNotification: jest.fn() }));

/**
 * `send` runs on a row loaded at the start of the cycle. A browser that
 * unsubscribes while the cycle is sending must stay unsubscribed: writing the
 * outcome back has to touch the live row by id, never `save` the stale copy
 * (which would re-insert it, with whatever keys and topics it had then).
 */
describe("PushService.send — writes the outcome without resurrecting the row", () => {
  const sendNotification = webpush.sendNotification as jest.Mock;
  const ENV = process.env;
  let live: Map<string, { failureCount: number }>;
  let repository: Record<string, jest.Mock>;
  let service: PushService;

  const stale = {
    id: "sub-1",
    endpoint: "https://fcm.googleapis.com/fcm/send/e1",
    p256dh: "key",
    auth: "secret",
    failureCount: 7,
  } as PushSubscription;

  beforeEach(() => {
    process.env = {
      ...ENV,
      VAPID_PUBLIC_KEY: "pub",
      VAPID_PRIVATE_KEY: "priv",
      VAPID_SUBJECT: "mailto:hello@park.fan",
    };
    live = new Map();
    const affect = (
      id: string,
      apply: (row: { failureCount: number }) => void,
    ) => {
      const row = live.get(id);
      if (row) apply(row);
      return { affected: row ? 1 : 0 };
    };
    repository = {
      save: jest.fn(),
      update: jest.fn(async ({ id }, partial) =>
        affect(id, (row) => Object.assign(row, partial)),
      ),
      increment: jest.fn(async ({ id }, _column, by: number) =>
        affect(id, (row) => (row.failureCount += by)),
      ),
      // Only the `MoreThanOrEqual(MAX)` shape is ever sent here.
      delete: jest.fn(async ({ id, failureCount }) => {
        const row = live.get(id);
        if (!row || (failureCount && row.failureCount < failureCount.value)) {
          return { affected: 0 };
        }
        live.delete(id);
        return { affected: 1 };
      }),
    };
    service = new PushService(
      repository as unknown as Repository<PushSubscription>,
    );
  });
  afterAll(() => {
    process.env = ENV;
  });

  it("a row deleted between load and a successful send stays deleted", async () => {
    sendNotification.mockResolvedValue(undefined);
    await expect(service.send(stale, {} as never)).resolves.toBe(true);
    expect(repository.save).not.toHaveBeenCalled();
    expect(live.has("sub-1")).toBe(false);
  });

  it("a row deleted between load and a failed send stays deleted", async () => {
    sendNotification.mockRejectedValue({ statusCode: 500 });
    await expect(service.send(stale, {} as never)).resolves.toBe(false);
    expect(repository.save).not.toHaveBeenCalled();
    expect(live.has("sub-1")).toBe(false);
  });

  it("counts failures on the live row and drops it at the limit", async () => {
    sendNotification.mockRejectedValue({ statusCode: 500 });
    live.set("sub-1", { failureCount: 6 });
    await service.send(stale, {} as never);
    expect(live.get("sub-1")?.failureCount).toBe(7);
    await service.send(stale, {} as never);
    expect(live.has("sub-1")).toBe(false);
  });
});
