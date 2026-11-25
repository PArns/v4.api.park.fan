import type { Repository } from "typeorm";
import { AssistantRecordsService } from "./assistant-records.service";
import type { AssistantConversation } from "./entities/assistant-conversation.entity";
import type { AssistantMessage } from "./entities/assistant-message.entity";
import type { SiteFeedback } from "./entities/site-feedback.entity";

describe("AssistantRecordsService.deleteConversation", () => {
  const id = "00000000-0000-4000-8000-000000000001";

  function service(affected: number) {
    const calls: string[] = [];
    const conversations = {
      delete: jest.fn(() => {
        calls.push("delete chat");
        return Promise.resolve({ affected });
      }),
    } as unknown as Repository<AssistantConversation>;
    const feedback = {
      update: jest.fn(() => {
        calls.push("unlink feedback");
        return Promise.resolve({ affected: 1 });
      }),
    } as unknown as Repository<SiteFeedback>;
    const records = new AssistantRecordsService(
      conversations,
      {} as Repository<AssistantMessage>,
      feedback,
    );
    return { records, conversations, feedback, calls };
  }

  it("keeps the chat's feedback, unlinked, and deletes the chat", async () => {
    const { records, feedback, conversations, calls } = service(1);
    await expect(records.deleteConversation(id)).resolves.toBe(true);
    expect(feedback.update).toHaveBeenCalledWith(
      { conversationId: id },
      { conversationId: null },
    );
    expect(conversations.delete).toHaveBeenCalledWith({ id });
    expect(calls).toEqual(["unlink feedback", "delete chat"]);
  });

  it("says so when there was no such chat", async () => {
    await expect(service(0).records.deleteConversation(id)).resolves.toBe(
      false,
    );
  });
});

describe("AssistantRecordsService.tokenStats", () => {
  const KEY = "0b0e7a52-6d1f-4c7e-9a43-2f6f0c1d9e11";
  const window = { from: "2026-11-01", to: "2026-11-03" };

  /** The service over the four queries' rows, in the order it asks: days, visitors, keys, chats. */
  function service(...results: Array<Array<Record<string, unknown>>>) {
    const query = jest.fn();
    for (const rows of results) query.mockResolvedValueOnce(rows);
    query.mockResolvedValue([]);
    const records = new AssistantRecordsService(
      { query } as unknown as Repository<AssistantConversation>,
      {} as Repository<AssistantMessage>,
      {} as Repository<SiteFeedback>,
    );
    return { records, query };
  }

  it("answers a window without chats with zeros and empty lists", async () => {
    const { records, query } = service();
    const zero = {
      chats: 0,
      messages: 0,
      inputTokens: 0,
      outputTokens: 0,
      cacheReadTokens: 0,
      totalTokens: 0,
      costUsd: 0,
    };
    await expect(records.tokenStats(window)).resolves.toEqual({
      ...window,
      keptDays: 7,
      days: [],
      totals: { all: zero, web: zero, app: zero },
      topVisitors: [],
      topApiKeys: [],
      topConversations: [],
    });
    // The window reaches the database as two bound UTC days, never as text in the statement.
    for (const [sql, params] of query.mock.calls as Array<[string, string[]]>) {
      expect(params).toEqual([
        "2026-11-01T00:00:00.000Z",
        "2026-11-03T00:00:00.000Z",
      ]);
      expect(sql).not.toContain("2026-11");
    }
  });

  it("sums the days per source from what Postgres hands over as text", async () => {
    const day = (name: string, source: string, factor: number) => ({
      day: name,
      source,
      chats: String(factor),
      messages: String(2 * factor),
      inputTokens: String(700 * factor),
      outputTokens: String(200 * factor),
      cacheReadTokens: String(100 * factor),
      totalTokens: String(1000 * factor),
      costUsd: (0.01 * factor).toFixed(6),
    });
    const { records } = service(
      [
        day("2026-11-01", "web", 1),
        day("2026-11-02", "app", 5),
        day("2026-11-02", "web", 2),
      ],
      [],
      [
        {
          apiKeyId: KEY,
          name: null,
          chats: "5",
          messages: "10",
          totalTokens: "5000",
          costUsd: "0.050000",
        },
      ],
    );
    const stats = await records.tokenStats(window);
    expect(stats.days[1]).toEqual({
      day: "2026-11-02",
      source: "app",
      chats: 5,
      messages: 10,
      inputTokens: 3500,
      outputTokens: 1000,
      cacheReadTokens: 500,
      totalTokens: 5000,
      costUsd: 0.05,
    });
    expect(stats.totals.web).toMatchObject({
      chats: 3,
      totalTokens: 3000,
      costUsd: 0.03,
    });
    expect(stats.totals.app).toMatchObject({ chats: 5, totalTokens: 5000 });
    expect(stats.totals.all).toMatchObject({
      chats: 8,
      messages: 16,
      inputTokens: 5600,
      totalTokens: 8000,
      costUsd: 0.08,
    });
    // A deleted key keeps its sums, without a name.
    expect(stats.topApiKeys).toEqual([
      {
        apiKeyId: KEY,
        name: null,
        chats: 5,
        messages: 10,
        totalTokens: 5000,
        costUsd: 0.05,
      },
    ]);
  });
});
