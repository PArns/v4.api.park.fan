import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import {
  AssistantRecordsService,
  CONVERSATION_RETENTION_DAYS,
} from "../../assistant/assistant-records.service";
import { ASSISTANT_LOCALES } from "../../assistant/assistant-locales";
import { SYSTEM_PROMPT_VERSION } from "../../assistant/system-prompt";
import {
  chatSummary,
  feedbackEntry,
  transcriptMessage,
} from "./assistant-records";
import {
  CHAT_DATA_DESCRIPTION,
  CHAT_DATA_NOTICE,
  withVisitorData,
} from "./visitor-data";

/** Whole chats per page: a long chat runs to a few thousand words. */
const MAX_PAGE_SIZE = 20;

export const assistantTranscriptsTool = defineMcpTool({
  name: "assistant_transcripts",
  title: "Parky's chats in full",
  description:
    "Many chats with Parky, park.fan's chat assistant, in full at once, newest first: for " +
    "reading how a version of his prompt answers without opening the chats one by one. Each " +
    "chat carries its summary (as assistant_conversations lists it), the page it was opened on, " +
    "every question and answer in order with the tools Parky called and their input (error true " +
    "when one failed), the prompt version and the rating per answer, and the feedback sent from " +
    "it. promptVersion keeps every chat with at least one answer on that version (a chat that " +
    "spans a deploy keeps its earlier answers, each marked with its own version); without it, " +
    `every chat. currentPromptVersion is the version answering now. Tool results are not stored, ` +
    `only the calls. Chats are kept ${CONVERSATION_RETENTION_DAYS} days. ` +
    CHAT_DATA_DESCRIPTION,
  input: {
    promptVersion: z
      .number()
      .int()
      .min(1)
      .max(32767)
      .optional()
      .describe(
        "Only chats with at least one answer written on this version of Parky's prompt",
      ),
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
    locale: z
      .enum(ASSISTANT_LOCALES)
      .optional()
      .describe("Only chats opened on this language of the site"),
    page: z.number().int().min(1).max(1000).default(1).describe("Page, from 1"),
    pageSize: z
      .number()
      .int()
      .min(1)
      .max(MAX_PAGE_SIZE)
      .default(10)
      .describe(`Chats per page, at most ${MAX_PAGE_SIZE}`),
  },
  async handler({ promptVersion, rating, q, locale, page, pageSize }, ctx) {
    const list = await ctx
      .get(AssistantRecordsService)
      .transcripts({ promptVersion, rating, q, locale, page, pageSize });
    return withVisitorData(
      {
        total: list.total,
        page: list.page,
        pageSize: list.pageSize,
        nextPage: list.page * list.pageSize < list.total ? list.page + 1 : null,
        currentPromptVersion: SYSTEM_PROMPT_VERSION,
      },
      {
        chats: list.items.map((chat) => ({
          ...chatSummary(chat),
          startPath: chat.startPath,
          messages: chat.messages.map(transcriptMessage),
          feedback: chat.feedback.map(feedbackEntry),
        })),
      },
      CHAT_DATA_NOTICE,
    );
  },
});
