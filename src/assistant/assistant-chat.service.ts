import { Injectable, Logger } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { InjectRepository } from "@nestjs/typeorm";
import Anthropic from "@anthropic-ai/sdk";
import { Repository } from "typeorm";
import { hashRateLimitKey } from "../common/redis/rate-limit.util";
import { AssistantConversation } from "./entities/assistant-conversation.entity";
import {
  AssistantMessage,
  type AssistantToolCall,
} from "./entities/assistant-message.entity";
import { AssistantSettingsService } from "./assistant-settings.service";
import { chatContext, SYSTEM_PROMPT } from "./system-prompt";
import { nowIn, userTimeZone } from "./assistant-time";
import type { ChatRequest } from "./assistant.schemas";
import {
  ASSISTANT_TOOL_PARAMS,
  runAssistantTool,
  toolStatus,
  type AssistantToolStatus,
} from "./tools/assistant-tools";
import type { ClientAction } from "./tools/assistant-actions";

export const ASSISTANT_MODEL = "claude-haiku-5-5";

/** Rounds of tool calls per answer; the last round must answer in text. */
const MAX_ROUNDS = 4;
/** Earlier messages sent along: five questions and their answers. */
const HISTORY_MESSAGES = 10;
/** Thinking counts against it too; an answer is meant to be a few sentences. */
const MAX_TOKENS = 2048;

/** US dollars per million tokens for Claude Haiku 5.5, up to 100K tokens of prompt. */
const PRICE_PER_MTOK = {
  input: 0.1,
  output: 0.5,
  cacheRead: 0.01,
  cacheWrite: 0.125,
};

/** What the widget receives over the stream. */
export type ChatEvent =
  | { type: "start"; conversationId: string }
  | { type: "status"; tool: AssistantToolStatus }
  | { type: "action"; action: ClientAction }
  | { type: "delta"; text: string }
  | { type: "done"; conversationId: string; messageId: string }
  | { type: "error"; code: "upstream" | "off" };

/** A chat that passed every check and may be answered. */
export interface PreparedChat {
  apiKey: string;
  conversation: AssistantConversation;
  history: Anthropic.MessageParam[];
  request: ChatRequest;
  /** The session's hash, which its token budget is counted under. */
  sessionHash: string;
}

/** Why a chat is refused before anything is streamed. */
export type ChatRefusal = "off" | "not-found" | "limit";

interface Usage {
  input: number;
  output: number;
  cacheRead: number;
  cacheWrite: number;
}

/**
 * Parky's answer loop: the question, the stored history and the tools go to
 * Claude, tool calls run here, and the text streams back as it is written.
 */
@Injectable()
export class AssistantChatService {
  private readonly logger = new Logger(AssistantChatService.name);

  constructor(
    private readonly settings: AssistantSettingsService,
    private readonly moduleRef: ModuleRef,
    @InjectRepository(AssistantConversation)
    private readonly conversations: Repository<AssistantConversation>,
    @InjectRepository(AssistantMessage)
    private readonly messages: Repository<AssistantMessage>,
  ) {}

  /**
   * @param sessionHash A hash of the caller's session token: a chat can only be continued from
   *   the session that started it, so a leaked chat id shows nobody else's history.
   */
  async prepare(
    request: ChatRequest,
    ip: string | null,
    sessionHash: string,
  ): Promise<PreparedChat | ChatRefusal> {
    // The switch only decides who is shown the widget: admins and `?assistant=on` testers chat
    // while it is off. What stops every chat is no key or a spent budget.
    const apiKey = await this.settings.apiKey();
    if (!apiKey || (await this.settings.budgetSpent())) return "off";
    // One visitor may not spend the day's budget that every visitor shares.
    if (await this.settings.visitorOverBudget(sessionHash)) return "limit";

    let conversation: AssistantConversation | null;
    let history: Anthropic.MessageParam[] = [];
    if (request.conversationId) {
      conversation = await this.conversations.findOneBy({
        id: request.conversationId,
      });
      if (!conversation || conversation.sessionHash !== sessionHash) {
        return "not-found";
      }
      const earlier = await this.messages.find({
        where: { conversationId: conversation.id },
        order: { createdAt: "DESC" },
        take: HISTORY_MESSAGES,
      });
      history = historyOf(earlier.reverse());
    } else {
      conversation = await this.conversations.save(
        this.conversations.create({
          locale: request.locale,
          startPath: request.pagePath,
          visitorKey: ip ? visitorKey(ip) : null,
          sessionHash,
        }),
      );
    }
    return { apiKey, conversation, history, request, sessionHash };
  }

  async answer(
    chat: PreparedChat,
    emit: (event: ChatEvent) => void,
    signal: AbortSignal,
    ip: string | null,
  ): Promise<void> {
    const { conversation, request } = chat;
    const timeZone = userTimeZone(request.timeZone);
    emit({ type: "start", conversationId: conversation.id });
    await this.messages.save(
      this.messages.create({
        conversationId: conversation.id,
        role: "user",
        text: request.message,
      }),
    );

    const client = new Anthropic({
      apiKey: chat.apiKey,
      maxRetries: 1,
      timeout: 60_000,
    });
    const system: Anthropic.TextBlockParam[] = [
      {
        type: "text",
        text: SYSTEM_PROMPT,
        cache_control: { type: "ephemeral" },
      },
      {
        type: "text",
        text: chatContext({
          locale: request.locale,
          now: nowIn(timeZone),
          timeZone,
          pagePath: request.pagePath,
          pageSubject: pageSubject(request.pagePath),
        }),
      },
    ];
    const messages: Anthropic.MessageParam[] = [
      ...chat.history,
      { role: "user", content: request.message },
    ];
    const toolContext = {
      get: <T>(token: Parameters<ModuleRef["get"]>[0]) =>
        this.moduleRef.get(token, { strict: false }) as T,
      locale: request.locale,
      ip,
      location: request.location ?? null,
      timeZone,
    };
    const usage: Usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
    const toolCalls: AssistantToolCall[] = [];
    let text = "";
    /** The running round's usage from its stream events, for a round that never finishes. */
    let partial: Usage | null = null;

    try {
      for (let round = 0; round < MAX_ROUNDS; round++) {
        const lastRound = round === MAX_ROUNDS - 1;
        const stream = client.messages.stream(
          {
            model: ASSISTANT_MODEL,
            max_tokens: MAX_TOKENS,
            output_config: { effort: "low" },
            system,
            tools: ASSISTANT_TOOL_PARAMS,
            tool_choice: lastRound ? { type: "none" } : { type: "auto" },
            messages,
          },
          { signal },
        );
        partial = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
        const running = partial;
        stream.on("streamEvent", (event) => {
          if (event.type === "message_start") {
            const start = event.message.usage;
            running.input = start.input_tokens;
            running.cacheRead = start.cache_read_input_tokens ?? 0;
            running.cacheWrite = start.cache_creation_input_tokens ?? 0;
          } else if (event.type === "message_delta") {
            running.output = event.usage.output_tokens;
          }
        });
        let separated = text.length === 0;
        stream.on("text", (delta) => {
          if (!separated) {
            separated = true;
            text += "\n\n";
            emit({ type: "delta", text: "\n\n" });
          }
          text += delta;
          emit({ type: "delta", text: delta });
        });
        const reply = await stream.finalMessage();
        partial = null;
        addUsage(usage, reply.usage);

        const calls = reply.content.filter(
          (block): block is Anthropic.ToolUseBlock => block.type === "tool_use",
        );
        if (reply.stop_reason !== "tool_use" || calls.length === 0) break;

        messages.push({ role: "assistant", content: reply.content });
        const results = await Promise.all(
          calls.map(async (call) => {
            const status = toolStatus(call.name);
            if (status) emit({ type: "status", tool: status });
            const result = await runAssistantTool(
              call.name,
              call.input,
              toolContext,
              (error) =>
                this.logger.error(
                  `Tool ${call.name} failed: ${(error as Error).message}`,
                ),
            );
            if (result.action) emit({ type: "action", action: result.action });
            toolCalls.push({
              name: call.name,
              input: call.input,
              ...(result.isError && { error: true }),
            });
            return {
              type: "tool_result" as const,
              tool_use_id: call.id,
              content: result.content,
              ...(result.isError && { is_error: true }),
            };
          }),
        );
        messages.push({ role: "user", content: results });
      }
    } catch (error) {
      // Tokens of a round cut short are spent all the same: a dropped connection must not be a
      // way around the budget.
      if (partial) {
        usage.input += partial.input;
        usage.output += partial.output;
        usage.cacheRead += partial.cacheRead;
        usage.cacheWrite += partial.cacheWrite;
      }
      if (!signal.aborted) {
        this.logger.error(`Answer failed: ${describeError(error)}`);
      }
      await this.record(conversation, text, toolCalls, usage, chat.sessionHash);
      emit({ type: "error", code: "upstream" });
      return;
    }

    const message = await this.record(
      conversation,
      text,
      toolCalls,
      usage,
      chat.sessionHash,
    );
    emit({
      type: "done",
      conversationId: conversation.id,
      messageId: message.id,
    });
  }

  private async record(
    conversation: AssistantConversation,
    text: string,
    toolCalls: AssistantToolCall[],
    usage: Usage,
    sessionHash: string,
  ): Promise<AssistantMessage> {
    const tokens =
      usage.input + usage.output + usage.cacheRead + usage.cacheWrite;
    const cost = costUsd(usage);
    // Cache reads cost a tenth of a fresh token and are left out of the budget.
    await this.settings.addTokens(
      usage.input + usage.output + usage.cacheWrite,
      sessionHash,
    );
    const message = await this.messages.save(
      this.messages.create({
        conversationId: conversation.id,
        role: "assistant",
        text,
        toolCalls: toolCalls.length > 0 ? toolCalls : null,
        inputTokens: usage.input + usage.cacheWrite,
        outputTokens: usage.output,
        cacheReadTokens: usage.cacheRead,
        costUsd: cost.toFixed(6),
      }),
    );
    await this.conversations
      .createQueryBuilder()
      .update()
      .set({
        messageCount: () => `"messageCount" + 2`,
        totalTokens: () => `"totalTokens" + ${tokens}`,
        costUsd: () => `"costUsd" + ${cost.toFixed(6)}`,
      })
      .where("id = :id", { id: conversation.id })
      .execute();
    return message;
  }
}

/**
 * Stored messages as the API takes them: an answer that failed (empty text) is dropped with the
 * question before it, since an empty assistant turn would be refused and break the chat; the
 * history starts with a question.
 */
export function historyOf(
  messages: Array<Pick<AssistantMessage, "role" | "text">>,
): Anthropic.MessageParam[] {
  const history: Anthropic.MessageParam[] = [];
  for (const message of messages) {
    if (message.role === "assistant" && !message.text.trim()) {
      if (history[history.length - 1]?.role === "user") history.pop();
      continue;
    }
    history.push({ role: message.role, content: message.text });
  }
  while (history[0]?.role === "assistant") history.shift();
  return history;
}

function addUsage(total: Usage, usage: Anthropic.Usage): void {
  total.input += usage.input_tokens;
  total.output += usage.output_tokens;
  total.cacheRead += usage.cache_read_input_tokens ?? 0;
  total.cacheWrite += usage.cache_creation_input_tokens ?? 0;
}

/** What one answer cost, in US dollars. */
export function costUsd(usage: Usage): number {
  return (
    (usage.input * PRICE_PER_MTOK.input +
      usage.output * PRICE_PER_MTOK.output +
      usage.cacheRead * PRICE_PER_MTOK.cacheRead +
      usage.cacheWrite * PRICE_PER_MTOK.cacheWrite) /
    1_000_000
  );
}

/**
 * The park or ride a park.fan path is about, by slug, so "here" can be
 * resolved: `/de/parks/europe/germany/bruehl/phantasialand/taron`.
 */
export function pageSubject(pagePath: string): string | null {
  const parts = pagePath.split("/").filter(Boolean);
  const at = parts.indexOf("parks");
  const park = at >= 0 ? parts[at + 4] : undefined;
  if (park) {
    const next = parts[at + 5];
    if (!next) return `the park with slug "${park}"`;
    const chapter = PARK_CHAPTERS[next];
    if (chapter) return `the ${chapter} of the park with slug "${park}"`;
    return `the ride with slug "${next}" in the park with slug "${park}"`;
  }
  // `/<locale>/news|blog|glossary/<slug>`: a post or a glossary term.
  const [, section, slug] = parts;
  if (slug && section === "news") {
    return `the news post with slug "${slug}" (read it with get_post)`;
  }
  if (slug && section === "blog") {
    return `the article with slug "${slug}" (read it with get_post)`;
  }
  if (slug && section === "glossary") {
    return `the glossary entry with slug "${slug}" (look it up with look_up_glossary)`;
  }
  return null;
}

/** A park's own sub-pages, which sit where a ride's slug would. */
const PARK_CHAPTERS: Record<string, string> = {
  "wait-time-calendar": "wait-time calendar",
  "average-wait-times": "average wait times page",
  "with-kids": "page for a visit with kids",
};

/**
 * Tells one visitor's chats of a day apart without storing the address: the
 * salt is the day and the server's secret, so the hash cannot be reversed by
 * trying every IPv4 address and means nothing the next day.
 */
function visitorKey(ip: string): string {
  const day = new Date().toISOString().slice(0, 10);
  return hashRateLimitKey(
    `${process.env.ASSISTANT_SECRET_KEY ?? ""}:${day}:${ip}`,
  );
}

function describeError(error: unknown): string {
  if (error instanceof Anthropic.APIError)
    return `${error.status} ${error.name}`;
  return error instanceof Error ? error.message : String(error);
}
