import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import {
  AssistantRecordsService,
  CONVERSATION_RETENTION_DAYS,
} from "../../src/assistant/assistant-records.service";

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
});
