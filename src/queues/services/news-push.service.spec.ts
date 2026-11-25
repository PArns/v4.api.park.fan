import { generateKeyPairSync } from "crypto";
import { Redis } from "ioredis";
import { PushService } from "../../push/push.service";
import { PushSubscription } from "../../push/entities/push-subscription.entity";
import {
  NewsPushService,
  newsLocaleOf,
  wantsNewsPost,
  POST_FEEDS,
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

const GUIDE = {
  ...POST,
  slug: "freizeitpark-packliste",
  title: "Die Freizeitpark-Packliste",
  teaser: "Was in den Rucksack gehört.",
  parks: [],
  parkSlugs: [],
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
    let guidesSeen: Set<string> | null;
    let guides: (typeof POST)[];
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
      guidesSeen = new Set([GUIDE.slug]);
      guides = [GUIDE];
      const isGuides = (key: string) => key.startsWith("push:guides:seen:");
      redis = {
        exists: jest.fn(async (key: string) =>
          (isGuides(key) ? guidesSeen : seen) ? 1 : 0,
        ),
        sadd: jest.fn(async (key: string, ...slugs: string[]) => {
          const set = isGuides(key)
            ? (guidesSeen ??= new Set())
            : (seen ??= new Set());
          let added = 0;
          for (const s of slugs) if (!set.has(s) && set.add(s)) added++;
          return added;
        }),
      };
      send = jest.fn().mockResolvedValue(true);
      devices = [device({ id: "all" })];
      service = new NewsPushService(
        {
          send,
          devicesWithKind: async (kind: string) =>
            devices.filter((d) => d.topics.includes(kind)),
        } as unknown as PushService,
        redis as unknown as Redis,
      );
      global.fetch = jest.fn(async (url: string) => ({
        ok: true,
        json: async () => ({
          posts: url.includes("/guides/")
            ? guides
            : [POST, { ...POST, slug: "older-post" }],
        }),
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

    describe("guides", () => {
      const NEW_GUIDE = { ...GUIDE, slug: "phantasialand-tipps" };
      beforeEach(() => {
        devices = [device({ id: "g", topics: ["guides"] })];
      });

      it("fills its own missing seen-set silently, all existing guides seen", async () => {
        guidesSeen = null;
        guides = [GUIDE, NEW_GUIDE];
        expect(await service.run(NOW)).toBe(0);
        expect(send).not.toHaveBeenCalled();
        expect([...guidesSeen!]).toEqual([GUIDE.slug, NEW_GUIDE.slug]);
        expect(redis.sadd).toHaveBeenCalledWith(
          "push:guides:seen:de",
          GUIDE.slug,
          NEW_GUIDE.slug,
        );
      });

      it("announces a new guide once, opening /blog/<slug>", async () => {
        guides = [NEW_GUIDE, GUIDE];
        expect(await service.run(NOW)).toBe(1);
        expect(send).toHaveBeenCalledWith(devices[0], {
          title: NEW_GUIDE.title,
          // What tells a guide from a news headline in a stack.
          subtitle: "Neuer Guide",
          body: NEW_GUIDE.teaser,
          url: "/blog/phantasialand-tipps",
          tag: "guides:phantasialand-tipps",
          kind: "guides",
        });
        expect(await service.run(NOW)).toBe(0);
      });

      it("carries the cover as the photo, when the post has one", () => {
        expect(
          writeNewsMessage(
            { ...NEW_GUIDE, cover: "phantasialand/background" },
            POST_FEEDS[1],
          ),
        ).toMatchObject({
          photo: "phantasialand/background",
          subtitle: "New guide",
        });
        const news = writeNewsMessage({ ...NEW_GUIDE, cover: null });
        expect(news).not.toHaveProperty("photo");
        expect(news).not.toHaveProperty("subtitle");
      });

      it("keeps an edited guide quiet, and one that left the list and came back", async () => {
        guides = [
          { ...GUIDE, updatedAt: "2026-11-24T09:55:00+01:00" } as never,
        ];
        expect(await service.run(NOW)).toBe(0);
        guides = [NEW_GUIDE];
        expect(await service.run(NOW)).toBe(1);
        guides = [GUIDE];
        expect(await service.run(NOW)).toBe(0);
        guides = [
          GUIDE,
          { ...NEW_GUIDE, updatedAt: POST.publishedAt } as never,
        ];
        expect(await service.run(NOW)).toBe(0);
        expect(send).toHaveBeenCalledTimes(1);
      });

      it("keeps an old guide quiet even when its slug is new", async () => {
        guides = [{ ...NEW_GUIDE, publishedAt: "2025-03-01T10:00:00+01:00" }];
        expect(await service.run(NOW)).toBe(0);
      });

      it("goes only to devices that asked for guides, and news only to news", async () => {
        const newsOnly = device({ id: "n" });
        const both = device({ id: "b", topics: ["news", "guides"] });
        devices = [newsOnly, both];
        guides = [NEW_GUIDE];
        // The post and the guide of one run are two notifications.
        expect(await service.run(NOW)).toBe(3);
        const got = (d: PushSubscription) =>
          send.mock.calls.filter((c) => c[0] === d).map((c) => c[1].kind);
        expect(got(newsOnly)).toEqual(["news"]);
        expect(got(both)).toEqual(["news", "guides"]);
      });

      it("is not narrowed by newsParks", async () => {
        devices = [
          device({
            topics: ["guides"],
            newsParks: ["europe/germany/bruehl/phantasialand"],
          }),
        ];
        guides = [NEW_GUIDE];
        expect(await service.run(NOW)).toBe(1);
      });

      it("reads the English export for a language without one", async () => {
        devices = [device({ topics: ["guides"], locale: "pt-BR" })];
        guides = [NEW_GUIDE];
        expect(await service.run(NOW)).toBe(1);
        expect(global.fetch).toHaveBeenCalledWith(
          "https://media.park.fan/guides/en.json",
          expect.anything(),
        );
        expect(redis.sadd).toHaveBeenCalledWith(
          "push:guides:seen:en",
          NEW_GUIDE.slug,
        );
      });
    });

    it("does nothing without an APNs key", async () => {
      process.env = { ...ENV, APNS_KEY_ID: "" };
      expect(await service.run(NOW)).toBe(0);
      expect(global.fetch).not.toHaveBeenCalled();
    });
  });
});
