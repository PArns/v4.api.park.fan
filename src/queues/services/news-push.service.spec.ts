import { generateKeyPairSync } from "crypto";
import { Redis } from "ioredis";
import { PushService } from "../../push/push.service";
import { PushSubscription } from "../../push/entities/push-subscription.entity";
import {
  NewsPushService,
  newsLocaleOf,
  wantsNewsPost,
  writeNewsMessage,
} from "./news-push.service";

const PEM = generateKeyPairSync("ec", { namedCurve: "prime256v1" })
  .privateKey.export({ type: "pkcs8", format: "pem" })
  .toString();

const NOW = Date.parse("2026-11-24T10:00:00Z");
const POST = {
  slug: "europa-park-wintersaison-2026",
  title: "Europa-Park startet in den Winter",
  teaser: "Ab dem 28. November.",
  publishedAt: "2026-11-24T08:37:00+01:00",
  parks: ["europe/germany/rust/europa-park"],
  parkSlugs: ["europa-park"],
};

describe("news push", () => {
  it("writes the post as it stands in the reader's export, with a deep link", () => {
    expect(writeNewsMessage(POST)).toEqual({
      title: POST.title,
      body: POST.teaser,
      url: `/news/${POST.slug}`,
      tag: `news:${POST.slug}`,
      kind: "news",
    });
  });

  it("narrows by park geo path, or by slug when a post has no paths", () => {
    expect(wantsNewsPost(null, POST)).toBe(true);
    expect(wantsNewsPost(["europe/germany/rust/europa-park"], POST)).toBe(true);
    expect(wantsNewsPost(["europe/germany/bruehl/phantasialand"], POST)).toBe(
      false,
    );
    expect(
      wantsNewsPost(["europe/germany/rust/europa-park"], {
        ...POST,
        parks: [],
      }),
    ).toBe(true);
    expect(wantsNewsPost([], POST)).toBe(false);
  });

  it("reads a regional tag as its language, anything else as English", () => {
    expect(newsLocaleOf("de-AT")).toBe("de");
    expect(newsLocaleOf("pt-BR")).toBe("en");
  });

  describe("NewsPushService.run", () => {
    const ENV = process.env;
    let seen: Set<string> | null;
    let redis: Partial<Record<keyof Redis, jest.Mock>>;
    let send: jest.Mock;
    let devices: PushSubscription[];
    let service: NewsPushService;
    const device = (patch: Partial<PushSubscription>) =>
      ({
        id: "d",
        apnsToken: "ab".repeat(32),
        locale: "de",
        timezone: "Europe/Berlin",
        topics: ["news"],
        newsParks: null,
        ...patch,
      }) as PushSubscription;

    beforeEach(() => {
      process.env = {
        ...ENV,
        APNS_KEY_ID: "K",
        APNS_TEAM_ID: "T",
        APNS_BUNDLE_ID: "fan.park.app",
        APNS_KEY: PEM,
      };
      seen = new Set(["older-post"]);
      redis = {
        exists: jest.fn(async () => (seen ? 1 : 0)),
        sadd: jest.fn(async (_key: string, ...slugs: string[]) => {
          seen ??= new Set();
          let added = 0;
          for (const s of slugs) if (!seen.has(s) && seen.add(s)) added++;
          return added;
        }),
      };
      send = jest.fn().mockResolvedValue(true);
      devices = [device({ id: "all" })];
      service = new NewsPushService(
        {
          send,
          devicesWithNews: async () => devices,
        } as unknown as PushService,
        redis as unknown as Redis,
      );
      global.fetch = jest.fn(async () => ({
        ok: true,
        json: async () => ({ posts: [POST, { ...POST, slug: "older-post" }] }),
      })) as unknown as typeof fetch;
    });
    afterAll(() => {
      process.env = ENV;
    });

    it("sends a new post once, never twice", async () => {
      expect(await service.run(NOW)).toBe(1);
      expect(send).toHaveBeenCalledWith(devices[0], writeNewsMessage(POST));
      expect(await service.run(NOW)).toBe(0);
    });

    it("fills a missing seen-set silently instead of announcing the archive", async () => {
      seen = null;
      expect(await service.run(NOW)).toBe(0);
      expect(send).not.toHaveBeenCalled();
      expect(seen).toContain(POST.slug);
    });

    it("keeps an old post quiet even when it is new to the set", async () => {
      expect(await service.run(NOW + 7 * 60 * 60_000)).toBe(0);
    });

    it("skips devices narrowed to other parks", async () => {
      devices = [
        device({ newsParks: ["europe/germany/bruehl/phantasialand"] }),
      ];
      expect(await service.run(NOW)).toBe(0);
      expect(send).not.toHaveBeenCalled();
    });

    it("does nothing without an APNs key", async () => {
      process.env = { ...ENV, APNS_KEY_ID: "" };
      expect(await service.run(NOW)).toBe(0);
      expect(global.fetch).not.toHaveBeenCalled();
    });
  });
});
