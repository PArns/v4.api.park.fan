import type { McpToolContext } from "../mcp-tool";
import { AssistantRecordsService } from "../../assistant/assistant-records.service";
import { assistantTokensReportTool } from "./assistant-tokens-report.tool";

describe("assistant_tokens_report", () => {
  it("hands a model the report without any visitor hash, each chat with its admin link", async () => {
    const tokenStats = jest.fn().mockResolvedValue({
      from: "2026-11-01",
      to: "2026-11-30",
      keptDays: 7,
      days: [],
      totals: {},
      topVisitors: [
        { visitorKey: "hash-1", source: "web", day: "2026-11-29", chats: 2 },
      ],
      topApiKeys: [],
      topConversations: [
        { id: "chat-1", visitorKey: "hash-1", source: "web", totalTokens: 9 },
      ],
    });
    const ctx = {
      get: (token: unknown) =>
        token === AssistantRecordsService ? { tokenStats } : undefined,
    } as unknown as McpToolContext;
    const report = (await assistantTokensReportTool.handler(
      { days: 30 } as never,
      ctx,
    )) as {
      topVisitors: unknown[];
      topConversations: Array<{ adminUrl: string }>;
    };
    expect(JSON.stringify(report)).not.toContain("hash-1");
    expect(report.topVisitors).toEqual([
      { source: "web", day: "2026-11-29", chats: 2 },
    ]);
    expect(report.topConversations[0].adminUrl).toContain("chat-1");
    expect(tokenStats).toHaveBeenCalledWith({
      from: expect.stringMatching(/^\d{4}-\d{2}-\d{2}$/),
      to: expect.stringMatching(/^\d{4}-\d{2}-\d{2}$/),
    });
  });
});
