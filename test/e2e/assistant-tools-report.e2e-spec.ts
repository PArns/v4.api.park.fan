import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import {
  AssistantRecordsService,
  type ToolStatsQuery,
} from "../../src/assistant/assistant-records.service";
import { SiteAnalyticsAssistantService } from "../../src/site-analytics/site-analytics-assistant.service";
import { addIsoDays } from "../../src/common/utils/date.util";
import { utcDay } from "../../src/common/utils/reader-hash.util";

/**
 * Parky's tool report against a real PostgreSQL: the calls read out of the
 * answers' `toolCalls` jsonb (outcomes, regions, prompt versions, the refused
 * paths, the dropped keys, change_trip's keys, the interview's ops), the
 * prune keeping their sums, a report that reads the same before and after
 * it, and his site events' report over the statistic's rows.
 */
describe("Parky's tool report (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let records: AssistantRecordsService;
  const today = utcDay(new Date());

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    app = moduleFixture.createNestApplication();
    await app.init();
    dataSource = app.get(DataSource);
    records = app.get(AssistantRecordsService);
  });

  afterAll(async () => {
    await app.close();
  });

  /** One chat `days` old with one answer whose tool calls are `calls`. */
  const chat = async (days: number, calls: unknown[], promptVersion = 53) => {
    const [row]: Array<{ id: string }> = await dataSource.query(
      `INSERT INTO assistant_conversations
         (locale, "startPath", "messageCount", "totalTokens", "costUsd", "createdAt")
       VALUES ('de', '/de', 2, 1000, 0.01, now() - $1::interval - interval '5 minutes')
       RETURNING id`,
      [`${days} days`],
    );
    await dataSource.query(
      `INSERT INTO assistant_messages ("conversationId", role, text, "createdAt")
       VALUES ($1::uuid, 'user', 'Wann nach Orlando?', now() - $2::interval - interval '5 minutes')`,
      [row.id, `${days} days`],
    );
    await dataSource.query(
      `INSERT INTO assistant_messages
         ("conversationId", role, text, "toolCalls", "promptVersion", "createdAt")
       VALUES ($1::uuid, 'assistant', 'Im März.', $2::jsonb, $3,
               now() - $4::interval - interval '5 minutes')`,
      [row.id, JSON.stringify(calls), promptVersion, `${days} days`],
    );
    return row.id;
  };

  const query = (over: Partial<ToolStatsQuery> = {}): ToolStatsQuery => ({
    from: addIsoDays(today, -29),
    to: today,
    tools: ["plan_vacation", "change_trip", "interview", "get_park_live"],
    regions: ["orlando", "japan"],
    ...over,
  });

  it("reads every call's outcome, region and prompt version, and keeps the sums the prune deletes", async () => {
    // Ten days old: past the retention.
    await chat(10, [
      { name: "plan_vacation", input: { region: "orlando" } },
      {
        name: "plan_vacation",
        input: { region: "orlando" },
        error: true,
        errorKind: "refused",
      },
      // A failure stored before the kinds were.
      { name: "change_trip", input: {}, error: true },
    ]);
    await chat(1, [
      {
        name: "change_trip",
        input: { region: "orlando", bases: "tokyo:3" },
        ignoredKeys: ["bases"],
      },
      {
        name: "change_trip",
        input: { region: "orlando", dates: { from: "x" } },
        error: true,
        errorKind: "invalid_input",
        issues: ["dates.from"],
      },
      {
        name: "interview",
        input: { op: "start", kind: "trip", region: "atlantis" },
      },
      { name: "interview", input: { op: "end", outcome: "done" } },
      {
        name: "made_up_tool",
        input: {},
        error: true,
        errorKind: "unknown_tool",
      },
    ]);
    await chat(0, [], 54);

    const cells = (rows: Awaited<ReturnType<typeof records.toolStats>>) =>
      rows
        .map(
          (r) =>
            `${r.tool}|${r.outcome}|${r.region}|${r.promptVersion}|${r.calls}`,
        )
        .sort();
    const before = await records.toolStats(query());
    expect(cells(before)).toEqual(
      [
        "(unknown)|unknown_tool||53|1",
        "change_trip|error||53|1",
        "change_trip|invalid_input|orlando|53|1",
        "change_trip|ok|orlando|53|1",
        // An input's region no region has is none.
        "interview|ok||53|2",
        "plan_vacation|ok|orlando|53|1",
        "plan_vacation|refused|orlando|53|1",
      ].sort(),
    );

    const inputs = await records.toolInputStats(query());
    expect(inputs).toMatchObject({
      answers: 3,
      answersWithTools: 2,
      calls: 8,
      issues: [{ tool: "change_trip", path: "dates.from", count: 1 }],
      ignoredKeys: [{ tool: "change_trip", key: "bases", count: 1 }],
      interviewOps: expect.arrayContaining([
        { op: "start", outcome: "", count: 1 },
        { op: "end", outcome: "done", count: 1 },
      ]),
    });
    expect(inputs.changeTripKeys).toEqual(
      expect.arrayContaining([
        { key: "region", count: 2 },
        { key: "bases", count: 1 },
        { key: "dates", count: 1 },
      ]),
    );

    // The prune deletes the old chat and keeps its calls' sums.
    expect(await records.pruneConversations()).toBe(1);
    const kept: Array<{ tool: string; outcome: string; calls: number }> =
      await dataSource.query(
        `SELECT tool, outcome, calls FROM assistant_tool_day_stats ORDER BY tool, outcome`,
      );
    expect(kept).toEqual([
      { tool: "change_trip", outcome: "error", calls: 1 },
      { tool: "plan_vacation", outcome: "ok", calls: 1 },
      { tool: "plan_vacation", outcome: "refused", calls: 1 },
    ]);
    // The report reads the same before and after.
    expect(cells(await records.toolStats(query()))).toEqual(cells(before));
    // Narrowed to one tool and one prompt version, both halves alike.
    expect(
      cells(
        await records.toolStats(
          query({ tool: "plan_vacation", promptVersion: 53 }),
        ),
      ),
    ).toEqual([
      "plan_vacation|ok|orlando|53|1",
      "plan_vacation|refused|orlando|53|1",
    ]);
    expect(await records.toolStats(query({ promptVersion: 54 }))).toEqual([]);
    // The inputs of a deleted chat are gone with it.
    expect((await records.toolInputStats(query())).answers).toBe(2);
  });

  it("reads Parky's site events: chips shown and pressed, and the interview", async () => {
    const bucket = new Date();
    bucket.setUTCMinutes(0, 0, 0);
    for (const [name, props, count] of [
      ["assistant_question", { kind: "chip" }, 6],
      ["assistant_question", { kind: "typed" }, 10],
      ["assistant_replies_shown", { count: "3" }, 8],
      ["assistant_interview_started", { kind: "trip", source: "planner" }, 5],
      [
        "assistant_interview_ended",
        { kind: "trip", outcome: "done", steps: "4" },
        3,
      ],
    ] as const) {
      await dataSource.query(
        `INSERT INTO site_events (bucket, name, locale, path, page_type, device, browser, os,
                                  country, network, props_key, props, count)
         VALUES ($1, $2, 'de', '/de/orlando', 'trip_hub', 'mobile', 'safari', 'ios', 'de',
                 'other', $3, $4::jsonb, $5)`,
        [
          bucket,
          name,
          Object.keys(props)
            .sort()
            .map((k) => `${k}=${(props as Record<string, string>)[k]}`)
            .join("&"),
          JSON.stringify(props),
          count,
        ],
      );
    }
    const report = await app.get(SiteAnalyticsAssistantService).report({
      days: 7,
      from: null,
      to: null,
      interval: null,
      locale: null,
      filter: [],
      datacenter: null,
    });
    expect(report.totals).toMatchObject({
      questions: 16,
      chipsPressed: 6,
      chipsShown: 8,
      chipsDrawn: 24,
      tripInterviews: 5,
      tripInterviewsDone: 3,
    });
    expect(report.lists.questionsByKind).toEqual([
      { kind: "typed", count: 10 },
      { kind: "chip", count: 6 },
    ]);
  });
});
