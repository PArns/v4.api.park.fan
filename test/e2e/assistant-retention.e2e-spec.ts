import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import {
  AssistantRecordsService,
  CONVERSATION_RETENTION_DAYS,
} from "../../src/assistant/assistant-records.service";
import { AssistantChatService } from "../../src/assistant/assistant-chat.service";
import { AssistantSettingsService } from "../../src/assistant/assistant-settings.service";

/**
 * Parky's chat retention against a real PostgreSQL: the cut-off, the cascade to the messages, the
 * daily sums kept for the statistic, and that a statistic read before and after the prune agrees.
 */
describe("Assistant chat retention (e2e)", () => {
  let app: INestApplication;
  let dataSource: DataSource;
  let records: AssistantRecordsService;

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

  /** One chat `days` old with two answers, one rated `rating`; returns its id. */
  const chat = async (
    days: number,
    rating: number | null,
    ended = false,
  ): Promise<string> => {
    const [row]: Array<{ id: string }> = await dataSource.query(
      `INSERT INTO assistant_conversations
         (locale, "startPath", "messageCount", "totalTokens", "costUsd", "endedAt", "createdAt")
       VALUES ('de', '/de', 2, 1000, 0.01, $2, now() - $1::interval - interval '5 minutes')
       RETURNING id`,
      [`${days} days`, ended ? new Date() : null],
    );
    for (const value of [rating, null]) {
      await dataSource.query(
        `INSERT INTO assistant_messages ("conversationId", role, text, rating, "createdAt")
         VALUES ($1::uuid, 'assistant', 'Hallo', $2, now() - $3::interval - interval '5 minutes')`,
        [row.id, value, `${days} days`],
      );
    }
    return row.id;
  };

  it("deletes chats past seven days with their messages, ended ones too, and keeps their sums", async () => {
    expect(CONVERSATION_RETENTION_DAYS).toBe(7);
    const old = await chat(20, 1);
    const oldEnded = await chat(8, -1, true);
    const fresh = await chat(6, 1);
    const before = await records.stats();

    expect(await records.pruneConversations()).toBe(2);

    const left: Array<{ id: string }> = await dataSource.query(
      `SELECT id FROM assistant_conversations`,
    );
    expect(left.map((row) => row.id)).toEqual([fresh]);
    const messages: Array<{ n: number }> = await dataSource.query(
      `SELECT count(*)::int AS n FROM assistant_messages
        WHERE "conversationId" = ANY($1::uuid[])`,
      [[old, oldEnded]],
    );
    expect(messages[0].n).toBe(0);

    // The statistic reads the same before and after: the deleted chats now come from the sums.
    expect(await records.stats()).toEqual(before);
    expect(before.ratings).toEqual({ up: 2, down: 1 });
    expect(
      before.days.reduce((n: number, d: { chats: number }) => n + d.chats, 0),
    ).toBe(3);

    // A second run finds nothing and counts nothing twice.
    expect(await records.pruneConversations()).toBe(0);
    expect(await records.stats()).toEqual(before);
  });

  it("sums tokens per day and source, the same before and after the prune, and names who spent most", async () => {
    const [key]: Array<{ id: string }> = await dataSource.query(
      `INSERT INTO api_keys (name, prefix, key_hash) VALUES ('park.fan App', 'pf_app', 'hash-app') RETURNING id`,
    );
    /** One chat `days` old with one answer of 700 in, 200 out, 100 cache-read tokens times `factor`. */
    const spend = async (
      days: number,
      source: "web" | "app",
      visitor: string,
      factor = 1,
    ): Promise<string> => {
      const [row]: Array<{ id: string }> = await dataSource.query(
        `INSERT INTO assistant_conversations
           (locale, "startPath", source, "apiKeyId", "visitorKey", "messageCount",
            "totalTokens", "costUsd", "createdAt")
         VALUES ('de', '/de', $2, $3, $4, 2, $5, $6,
                 now() - $1::interval - interval '5 minutes')
         RETURNING id`,
        [
          `${days} days`,
          source,
          source === "app" ? key.id : null,
          visitor,
          1000 * factor,
          0.01 * factor,
        ],
      );
      await dataSource.query(
        `INSERT INTO assistant_messages
           ("conversationId", role, text, "inputTokens", "outputTokens", "cacheReadTokens")
         VALUES ($1::uuid, 'user', 'Frage', 0, 0, 0),
                ($1::uuid, 'assistant', 'Antwort', $2, $3, $4)`,
        [row.id, 700 * factor, 200 * factor, 100 * factor],
      );
      return row.id;
    };
    await spend(20, "web", "v-old");
    await spend(20, "app", "v-old-app", 2);
    await spend(1, "web", "v-web");
    await spend(1, "web", "v-web");
    const dearest = await spend(1, "app", "v-app", 5);
    // A day pruned before the source was kept: only the old table has it, total tokens alone.
    await dataSource.query(
      `INSERT INTO assistant_day_stats (day, chats, messages, tokens, "costUsd")
       VALUES ((now() AT TIME ZONE 'UTC')::date - 25, 3, 6, 3000, 0.03)`,
    );

    const today = new Date().toISOString().slice(0, 10);
    const from = new Date(Date.now() - 29 * 86_400_000)
      .toISOString()
      .slice(0, 10);
    const before = await records.tokenStats({ from, to: today });

    expect(before.totals.web).toEqual({
      chats: 6,
      messages: 12,
      inputTokens: 2100,
      outputTokens: 600,
      cacheReadTokens: 300,
      totalTokens: 6000,
      costUsd: 0.06,
    });
    expect(before.totals.app).toEqual({
      chats: 2,
      messages: 4,
      inputTokens: 4900,
      outputTokens: 1400,
      cacheReadTokens: 700,
      totalTokens: 7000,
      costUsd: 0.07,
    });
    expect(before.totals.all.totalTokens).toBe(13000);
    expect(before.days.map((row) => row.source)).toEqual([
      "web",
      "app",
      "web",
      "app",
      "web",
    ]);
    expect(before.topVisitors[0]).toMatchObject({
      visitorKey: "v-app",
      source: "app",
      chats: 1,
      totalTokens: 5000,
    });
    expect(before.topVisitors[1]).toMatchObject({
      visitorKey: "v-old-app",
      totalTokens: 2000,
    });
    expect(before.topVisitors[2]).toMatchObject({
      visitorKey: "v-web",
      chats: 2,
      totalTokens: 2000,
    });
    expect(before.topApiKeys).toEqual([
      {
        apiKeyId: key.id,
        name: "park.fan App",
        chats: 2,
        messages: 4,
        totalTokens: 7000,
        costUsd: 0.07,
      },
    ]);
    expect(before.topConversations[0]).toMatchObject({
      id: dearest,
      source: "app",
      apiKeyId: key.id,
      totalTokens: 5000,
    });
    // Numbers and ids only.
    expect(JSON.stringify(before)).not.toContain("Antwort");

    expect(await records.pruneConversations()).toBe(2);
    const after = await records.tokenStats({ from, to: today });
    expect(after.days).toEqual(before.days);
    expect(after.totals).toEqual(before.totals);
    // A key's sums outlive the prune; the top visitors and chats read the kept chats only.
    expect(after.topApiKeys).toEqual(before.topApiKeys);
    expect(after.topVisitors.map((v) => v.visitorKey)).toEqual([
      "v-app",
      "v-web",
    ]);
    // The 30-day statistic beside it still counts every chat once.
    const stats = await records.stats();
    expect(
      stats.days.reduce((n: number, d: { chats: number }) => n + d.chats, 0),
    ).toBe(8);

    const list = await records.listConversations({ page: 1, source: "app" });
    expect(list.items.map((item) => item.id)).toEqual([dearest]);
    expect(list.items[0]).toMatchObject({ source: "app", apiKeyId: key.id });
    expect(await records.conversation(dearest)).toMatchObject({
      source: "app",
      apiKeyName: "park.fan App",
    });
  });

  it("files a new chat under the source its session was opened with", async () => {
    const chats = app.get(AssistantChatService);
    const settings = app.get(AssistantSettingsService);
    jest.spyOn(settings, "apiKey").mockResolvedValue("sk-ant-test");
    const [key]: Array<{ id: string }> = await dataSource.query(
      `INSERT INTO api_keys (name, prefix, key_hash) VALUES ('park.fan App', 'pf_app', 'hash-app') RETURNING id`,
    );
    const request = {
      locale: "de",
      pagePath: "/de",
      message: "Wie voll ist es heute?",
      sessionToken: "unused",
    } as never;
    const web = await chats.prepare(request, "203.0.113.9", "nonce-web");
    const fromApp = await chats.prepare(
      request,
      "203.0.113.9",
      `nonce-app~${key.id}`,
    );
    if (typeof web === "string" || typeof fromApp === "string") {
      throw new Error("refused");
    }
    expect(web.conversation).toMatchObject({ source: "web", apiKeyId: null });
    expect(fromApp.conversation).toMatchObject({
      source: "app",
      apiKeyId: key.id,
    });
    // A row written without the column, as before it existed, reads as the website's.
    const [legacy]: Array<{ source: string }> = await dataSource.query(
      `INSERT INTO assistant_conversations (locale, "startPath") VALUES ('de', '/de')
       RETURNING source`,
    );
    expect(legacy.source).toBe("web");
  });
});
