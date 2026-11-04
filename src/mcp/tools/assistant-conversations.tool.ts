import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import {
  AssistantRecordsService,
  CONVERSATION_RETENTION_DAYS,
} from "../../assistant/assistant-records.service";
import { chatSummary } from "./assistant-records";
import {
  CHAT_DATA_DESCRIPTION,
  CHAT_DATA_NOTICE,
  withVisitorData,
} from "./visitor-data";

export const assistantConversationsTool = defineMcpTool({
  name: "assistant_conversations",
  title: "Parky's chats",
  description:
    "Chats visitors had with Parky, park.fan's chat assistant, newest first: when, the site " +
    "language, how many messages, tokens and cost in USD, the rating (-1 when any answer got a " +
    "thumbs down, 1 when only thumbs up, 0 unrated), the page the chat was opened on and the " +
    "first question. Filter by rating or by a word in any message. Read a whole chat with " +
    `assistant_conversation and its id. Chats are kept ${CONVERSATION_RETENTION_DAYS} days. ` +
    CHAT_DATA_DESCRIPTION,
  input: {
    rating: z
      .union([z.literal(-1), z.literal(0), z.literal(1)])
      .optional()
      .describe(
        "-1 for chats with a thumbs down, 1 for only thumbs up, 0 for unrated",
      ),
    q: z
      .string()
      .trim()
      .min(2)
      .max(100)
      .optional()
      .describe(
        "A word or phrase any message of the chat contains (case-insensitive)",
      ),
    page: z.number().int().min(1).max(1000).default(1).describe("Page, from 1"),
    pageSize: z
      .number()
      .int()
      .min(1)
      .max(50)
      .default(20)
      .describe("Chats per page"),
  },
  async handler({ rating, q, page, pageSize }, ctx) {
    const list = await ctx
      .get(AssistantRecordsService)
      .listConversations({ rating, q, page, pageSize });
    return withVisitorData(
      { total: list.total, page: list.page, pageSize: list.pageSize },
      {
        chats: list.items.map((chat) => ({
          ...chatSummary(chat),
          startPath: chat.startPath,
          firstQuestion: chat.firstQuestion,
        })),
      },
      CHAT_DATA_NOTICE,
    );
  },
});
