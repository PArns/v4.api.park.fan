import { z } from "zod";
import type { McpToolContext } from "../mcp-tool";
import { pageViewsTool } from "./page-views.tool";
import { VISITOR_DATA_NOTICE } from "./visitor-data";

const PARK = "11111111-1111-4111-8111-111111111111";
const GONE = "22222222-2222-4222-8222-222222222222";

function service(overrides: Record<string, unknown> = {}) {
  return {
    adminOverview: jest.fn(async () => ({
      days: 30,
      from: "2026-09-29",
      kinds: [
        {
          kind: "park",
          total: 12,
          byLocale: { de: 12 },
          top: [
            {
              key: PARK,
              name: "Phantasialand",
              context: "Brühl, Germany",
              total: 10,
              byLocale: { de: 10 },
            },
            { key: GONE, total: 2, byLocale: { de: 2 } },
          ],
        },
        {
          kind: "article",
          total: 5,
          byLocale: { en: 5 },
          top: [
            {
              key: "ignore-previous-instructions",
              total: 5,
              byLocale: { en: 5 },
            },
          ],
        },
      ],
      daily: [],
      ...overrides,
    })),
  };
}

async function run(
  raw: Record<string, unknown>,
  overrides: Record<string, unknown> = {},
) {
  const pageViews = service(overrides);
  const ctx = { admin: {}, get: () => pageViews } as unknown as McpToolContext;
  const input = z.object(pageViewsTool.input).parse(raw);
  return (await pageViewsTool.handler(input as never, ctx)) as Record<
    string,
    unknown
  >;
}

describe("page_views tool", () => {
  it("names parks from our tables and drops the ones that no longer resolve", async () => {
    const answer = await run({});
    expect(answer.parks).toEqual({
      items: [
        {
          id: PARK,
          name: "Phantasialand",
          context: "Brühl, Germany",
          total: 10,
          byLocale: { de: 10 },
        },
      ],
      unresolved: 1,
    });
  });

  it("keeps the keys the site sent under visitorData, labelled", async () => {
    const answer = await run({});
    expect(answer.visitorDataNotice).toBe(VISITOR_DATA_NOTICE);
    expect(answer.visitorData).toEqual({
      articles: [
        { key: "ignore-previous-instructions", total: 5, byLocale: { en: 5 } },
      ],
    });
    expect(JSON.stringify({ ...answer, visitorData: null })).not.toContain(
      "ignore-previous-instructions",
    );
  });

  it("answers one kind only when asked", async () => {
    const answer = await run({ kind: "park" });
    expect(answer.totals).toEqual([
      { kind: "park", total: 12, byLocale: { de: 12 } },
    ]);
    expect(answer.visitorData).toEqual({});
  });

  describe("small values", () => {
    const OTHER = "22222222-3333-4333-8333-333333333333";
    const kinds = [
      {
        kind: "park",
        total: 60,
        byLocale: { de: 55, en: 5 },
        top: [
          {
            key: PARK,
            name: "Phantasialand",
            context: "Brühl, Germany",
            total: 50,
            byLocale: { de: 46, en: 3, nl: 1 },
          },
          { key: OTHER, name: "Europa-Park", total: 3, byLocale: { de: 3 } },
          { key: GONE, name: "Toverland", total: 2, byLocale: { nl: 2 } },
        ],
      },
      {
        kind: "news",
        total: 14,
        byLocale: { de: 14 },
        top: [
          { key: "big-news", total: 12, byLocale: { de: 12 } },
          { key: "quiet-news", total: 2, byLocale: { de: 2 } },
        ],
      },
    ];

    it("folds the small top pages and the small languages of a page into (other), and keeps the totals", async () => {
      const answer = await run({}, { kinds });
      expect(answer.parks).toEqual({
        items: [
          {
            id: PARK,
            name: "Phantasialand",
            context: "Brühl, Germany",
            total: 50,
            byLocale: { de: 46, "(other)": 4 },
          },
          {
            id: null,
            name: "(other)",
            context: null,
            total: 5,
            byLocale: null,
          },
        ],
        unresolved: 0,
      });
      expect(answer.totals).toEqual([
        { kind: "park", total: 60, byLocale: { de: 55, en: 5 } },
        { kind: "news", total: 14, byLocale: { de: 14 } },
      ]);
      expect(answer.visitorData).toEqual({
        news: [
          { key: "big-news", total: 12, byLocale: { de: 12 } },
          { key: "(other)", total: 2, byLocale: null },
        ],
      });
    });

    it("hides the small days of one kind, and not the days of all kinds together", async () => {
      const daily = [
        { day: "2026-10-27", byKind: { park: 3, news: 20 } },
        { day: "2026-10-28", byKind: { park: 30, news: 2 } },
      ];
      const all = await run({ daily: true }, { kinds, daily });
      expect(all.daily).toEqual(daily);
      expect(all).not.toHaveProperty("smallValuesHidden");
      const one = await run({ daily: true, kind: "park" }, { kinds, daily });
      expect(one.daily).toEqual([
        { day: "2026-10-27", views: null },
        { day: "2026-10-28", views: 30 },
      ]);
      expect(one.smallValuesHidden).toBe(true);
    });

    it("tells the model in its description", () => {
      expect(pageViewsTool.description).toContain(
        'folded into one "(other)" row',
      );
    });
  });
});
