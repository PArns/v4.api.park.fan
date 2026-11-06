import type { AssistantConversation } from "../../assistant/entities/assistant-conversation.entity";
import type { AssistantMessage } from "../../assistant/entities/assistant-message.entity";
import type { SiteFeedback } from "../../assistant/entities/site-feedback.entity";
import { adminConversationUrl } from "./links";

/**
 * How Parky's stored chats and the feedback reach the model, shared by the
 * `assistant_*` and `site_feedback` tools. Left out on purpose: the hashes of
 * the visitor and the session, which identify a browser for a day, and the
 * feedback's email address, which is personal data the admin shows a person
 * but a model has no use for; `wantsReply` says whether there is one.
 */

/** A chat as a row of the list: numbers and ids only, the text goes under visitorData. */
export function chatSummary(chat: AssistantConversation) {
  return {
    id: chat.id,
    at: chat.createdAt,
    locale: chat.locale,
    messages: chat.messageCount,
    tokens: chat.totalTokens,
    costUsd: Number(chat.costUsd),
    rating: chat.rating,
    // Absent for chats from before the version was stored.
    ...(chat.promptVersion !== null && { promptVersion: chat.promptVersion }),
    // Absent unless Parky closed the chat for abuse.
    ...(chat.endedAt && {
      endedByParky: { at: chat.endedAt, reason: chat.endedReason },
    }),
    adminUrl: adminConversationUrl(chat.id),
  };
}

/** One question or answer, with what Parky looked up for it. */
export function chatMessage(message: AssistantMessage) {
  return {
    id: message.id,
    at: message.createdAt,
    role: message.role,
    text: message.text,
    ...(message.toolCalls?.length && {
      toolCalls: message.toolCalls.map((call) => ({
        name: call.name,
        input: call.input,
        ...(call.error && { error: true }),
      })),
    }),
    ...(message.role === "assistant" && {
      tokens: {
        input: message.inputTokens,
        output: message.outputTokens,
        cacheRead: message.cacheReadTokens,
      },
      costUsd: Number(message.costUsd),
      ...(message.promptVersion !== null && {
        promptVersion: message.promptVersion,
      }),
    }),
    ...(message.rating !== null && { rating: message.rating }),
  };
}

/**
 * One piece of feedback, without the email address. `source` says who filed it: the visitor in
 * the form, or Parky from the chat, whose `message` is then the visitor's own words.
 */
export function feedbackEntry(entry: SiteFeedback) {
  return {
    id: entry.id,
    at: entry.createdAt,
    status: entry.status,
    source: entry.source,
    ...(entry.kind && { kind: entry.kind }),
    locale: entry.locale,
    pagePath: entry.pagePath,
    message: entry.message,
    wantsReply: entry.email !== null,
    ...(entry.conversationId && {
      conversationId: entry.conversationId,
      conversationAdminUrl: adminConversationUrl(entry.conversationId),
    }),
  };
}
