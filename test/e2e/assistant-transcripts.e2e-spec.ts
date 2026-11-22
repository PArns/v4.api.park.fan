import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { DataSource } from "typeorm";
import { AppModule } from "../../src/app.module";
import { AssistantRecordsService } from "../../src/assistant/assistant-records.service";

/**
 * The admin MCP's `assistant_transcripts` against a real PostgreSQL: whole chats a page at a time,
 * kept by the prompt version any of their answers ran on, while the chat list still filters by
 * the latest answer's.
 */
describe("Assistant transcripts (e2e)", () => {
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
    await dataSource.query(`DELETE FROM assistant_conversations`);
  });

  afterAll(async () => {
    await app.close();
  });

  /**
   * A chat `minutesAgo` old whose answers ran on `versions` in turn, each after its question;
   * the chat keeps the last answer's version, as the chat service stores it.
   */
  const chat = async (
    minutesAgo: number,
    locale: string,
    versions: number[],
  ): Promise<string> => {
    const [row]: Array<{ id: string }> = await dataSource.query(
      `INSERT INTO assistant_conversations
         (locale, "startPath", "messageCount", "totalTokens", "costUsd", "promptVersion", "createdAt")
       VALUES ($1, $2, $3, 1000, 0.01, $4, now() - $5::interval)
       RETURNING id`,
      [
        locale,
        `/${locale}`,
        versions.length * 2,
        versions[versions.length - 1],
        `${minutesAgo} minutes`,
      ],
    );
    for (const [i, version] of versions.entries()) {
      // Seconds apart, oldest first, so the order the messages come back in is checked too.
      const at = `${minutesAgo * 60 - i * 10} seconds`;
      await dataSource.query(
        `INSERT INTO assistant_messages ("conversationId", role, text, "createdAt")
         VALUES ($1::uuid, 'user', $2, now() - $3::interval)`,
        [row.id, `question ${i + 1}`, at],
      );
      await dataSource.query(
        `INSERT INTO assistant_messages
           ("conversationId", role, text, "promptVersion", "toolCalls", "createdAt")
         VALUES ($1::uuid, 'assistant', $2, $3, $4::jsonb, now() - $5::interval + interval '5 seconds')`,
        [
          row.id,
          `answer ${i + 1}`,
          version,
          JSON.stringify([{ name: "get_days", input: { park: "Efteling" } }]),
          at,
        ],
      );
    }
    return row.id;
  };

  it("keeps a chat by any answer's prompt version, whole and in order", async () => {
    const spansDeploy = await chat(30, "de", [62, 63]);
    const onlyNew = await chat(20, "en", [63]);
    const onlyOld = await chat(10, "de", [62]);

    const on62 = await records.transcripts({
      promptVersion: 62,
      page: 1,
      pageSize: 10,
    });
    expect(on62.total).toBe(2);
    expect(on62.items.map((c) => c.id)).toEqual([onlyOld, spansDeploy]);
    const spanning = on62.items[1];
    expect(spanning.messages.map((m) => m.text)).toEqual([
      "question 1",
      "answer 1",
      "question 2",
      "answer 2",
    ]);
    expect(spanning.messages.map((m) => m.promptVersion)).toEqual([
      null,
      62,
      null,
      63,
    ]);
    expect(spanning.messages[1].toolCalls).toEqual([
      { name: "get_days", input: { park: "Efteling" } },
    ]);

    // The chat list still goes by the latest answer: the chat across the deploy reads 63 there.
    const listed = await records.listConversations({
      promptVersion: 62,
      page: 1,
      pageSize: 10,
    });
    expect(listed.items.map((c) => c.id)).toEqual([onlyOld]);

    const on63German = await records.transcripts({
      promptVersion: 63,
      locale: "de",
      page: 1,
      pageSize: 10,
    });
    expect(on63German.items.map((c) => c.id)).toEqual([spansDeploy]);

    const pageTwo = await records.transcripts({ page: 2, pageSize: 2 });
    expect(pageTwo.total).toBe(3);
    expect(pageTwo.items.map((c) => c.id)).toEqual([spansDeploy]);
    expect(pageTwo.items[0].messages).toHaveLength(4);

    const onlyNewByWord = await records.transcripts({
      q: "ANSWER 1",
      locale: "en",
      page: 1,
      pageSize: 10,
    });
    expect(onlyNewByWord.items.map((c) => c.id)).toEqual([onlyNew]);
  });

  it("returns an empty page without reading messages", async () => {
    const none = await records.transcripts({
      promptVersion: 1,
      page: 1,
      pageSize: 10,
    });
    expect(none).toEqual({ total: 0, page: 1, pageSize: 10, items: [] });
  });
});
