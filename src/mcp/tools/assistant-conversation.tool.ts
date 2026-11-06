import { defineMcpTool, McpToolError, uuidInput } from "../mcp-tool";
import {
  AssistantRecordsService,
  CONVERSATION_RETENTION_DAYS,
} from "../../assistant/assistant-records.service";
import { chatMessage, chatSummary, feedbackEntry } from "./assistant-records";
import {
  CHAT_DATA_DESCRIPTION,
  CHAT_DATA_NOTICE,
  withVisitorData,
} from "./visitor-data";

export const assistantConversationTool = defineMcpTool({
  name: "assistant_conversation",
  title: "One Parky chat",
  description:
    "One chat with Parky in full, in order: every question and answer with its time, the tools " +
    "Parky called for an answer with their input (error true when one failed), tokens, cost and " +
    "the version of Parky's prompt per answer, the thumbs up (1) or down (-1) a visitor gave it, " +
    "and the feedback sent from the " +
    "chat. Ids come from assistant_conversations or a feedback's conversationId; adminUrl opens " +
    "the chat in the admin. Tool results are not stored, only the calls. " +
    CHAT_DATA_DESCRIPTION,
  input: {
    id: uuidInput().describe("The chat's id"),
  },
  async handler({ id }, ctx) {
    const chat = await ctx
      .get(AssistantRecordsService)
      .conversation(id.toLowerCase());
    if (!chat) {
      throw new McpToolError(
        `No chat with that id. Chats are kept ${CONVERSATION_RETENTION_DAYS} days, and an admin can delete one.`,
      );
    }
    return withVisitorData(
      chatSummary(chat),
      {
        startPath: chat.startPath,
        messages: chat.messages.map(chatMessage),
        feedback: chat.feedback.map(feedbackEntry),
      },
      CHAT_DATA_NOTICE,
    );
  },
});
