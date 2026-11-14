import { Injectable, Logger } from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { InjectRepository } from "@nestjs/typeorm";
import Anthropic from "@anthropic-ai/sdk";
import { DataSource, In, Repository } from "typeorm";
import { isCanonicalUuid } from "../common/utils/uuid-param.util";
import { Park } from "../parks/entities/park.entity";
import { Attraction } from "../attractions/entities/attraction.entity";
import { hashRateLimitKey } from "../common/redis/rate-limit.util";
import { AssistantConversation } from "./entities/assistant-conversation.entity";
import {
  AssistantMessage,
  type AssistantToolCall,
} from "./entities/assistant-message.entity";
import { AssistantSettingsService } from "./assistant-settings.service";
import { AssistantPageService } from "./assistant-page.service";
import {
  chatContext,
  SYSTEM_PROMPT,
  SYSTEM_PROMPT_VERSION,
  type LookedUpEntity,
} from "./system-prompt";
import { repeatsLastQuestion } from "./assistant-junk";
import { RepliesTextGuard } from "./assistant-replies-text";
import { nowIn, userTimeZone } from "./assistant-time";
import type { ChatRequest } from "./assistant.schemas";
import {
  ASSISTANT_TOOL_PARAMS,
  runAssistantTool,
  storedToolInput,
  toolStatus,
  visitorTripItinerary,
  type AnswerState,
  type AssistantToolContext,
  type AssistantToolStatus,
} from "./tools/assistant-tools";
import {
  pickTrip,
  tripDigest,
  visitorTrips,
  type VisitorTrip,
} from "./tools/assistant-plans";
import type { ClientAction } from "./tools/assistant-actions";

export const ASSISTANT_MODEL = "claude-haiku-5-5";

/** Rounds of tool calls per answer; the last round must answer in text. */
const MAX_ROUNDS = 4;
/** Earlier messages sent along: five questions and their answers. */
const HISTORY_MESSAGES = 10;
/** Thinking counts against it too; an answer is meant to be a few sentences. */
const MAX_TOKENS = 2048;
/** Parks and rides of earlier answers named in "This chat", newest first. */
const MAX_LOOKED_UP = 8;
/**
 * How long the on-screen trip's plan may take for "This chat": the planner asked the same plan a
 * moment ago, so it is a Redis hit, and a miss is not worth holding the answer for.
 */
const TRIP_DIGEST_BUDGET_MS = 1500;
/** The tool that only adds the reply chips: a round of nothing else ends the answer. */
const REPLIES_TOOL = "suggest_replies";

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
  /** Park and ride ids earlier answers of this chat used, newest first. */
  lookedUp?: LookedUpId[];
  request: ChatRequest;
  /** The session's id, which the feedback limits are counted under. */
  session: string;
  /** The session's hash, which its token budget is counted under. */
  sessionHash: string;
}

/** Why a chat is refused before anything is streamed. */
/** Why a question gets no answer; `junk` repeats the question just answered. */
export type ChatRefusal = "off" | "not-found" | "limit" | "junk" | "ended";

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
    private readonly pages: AssistantPageService,
  ) {}

  /**
   * @param session The caller's checked session: a chat can only be continued from the session
   *   that started it, so a leaked chat id shows nobody else's history.
   */
  async prepare(
    request: ChatRequest,
    ip: string | null,
    session: string,
  ): Promise<PreparedChat | ChatRefusal> {
    const sessionHash = hashRateLimitKey(session);
    // The switch only decides who is shown the widget: admins and `?assistant=on` testers chat
    // while it is off. What stops every chat is no key or a spent budget.
    const apiKey = await this.settings.apiKey();
    if (!apiKey || (await this.settings.budgetSpent())) return "off";
    // One visitor may not spend the day's budget that every visitor shares.
    if (await this.settings.visitorOverBudget(sessionHash)) return "limit";

    let conversation: AssistantConversation | null;
    let history: Anthropic.MessageParam[] = [];
    let lookedUp: LookedUpId[] = [];
    if (request.conversationId) {
      conversation = await this.conversations.findOneBy({
        id: request.conversationId,
      });
      if (!conversation || conversation.sessionHash !== sessionHash) {
        return "not-found";
      }
      // A chat Parky ended stays ended; the session's own ban runs out on its own.
      if (conversation.endedAt) return "ended";
      const earlier = await this.messages.find({
        where: { conversationId: conversation.id },
        order: { createdAt: "DESC" },
        take: HISTORY_MESSAGES,
      });
      earlier.reverse();
      if (repeatsLastQuestion(request.message, earlier, new Date())) {
        return "junk";
      }
      history = historyOf(earlier);
      lookedUp = lookedUpIds(earlier);
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
    return {
      apiKey,
      conversation,
      history,
      lookedUp,
      request,
      session,
      sessionHash,
    };
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
    // The site's live regions, when the widget says; a trip elsewhere is not the visitor's to see.
    const liveRegions = request.liveRegions
      ? new Set(request.liveRegions)
      : undefined;
    const trips = visitorTrips(request.trips).filter(
      (trip) => !liveRegions || liveRegions.has(trip.region.slug),
    );
    const answer: AnswerState = { round: 0, done: new Set() };
    const toolContext: AssistantToolContext = {
      get: <T>(token: Parameters<ModuleRef["get"]>[0]) =>
        this.moduleRef.get(token, { strict: false }) as T,
      locale: request.locale,
      ip,
      location: request.location ?? null,
      timeZone,
      planner: request.planner ?? null,
      trips,
      ...(liveRegions && { liveRegions }),
      interview: request.interview ?? null,
      alerts: request.alerts ?? null,
      favorites: request.favorites ?? null,
      ridden: new Set(request.ridden ?? []),
      chat: {
        conversationId: conversation.id,
        pagePath: request.pagePath,
        session: chat.session,
        sessionHash: chat.sessionHash,
        message: request.message,
        previousMessage: lastQuestion(chat.history),
      },
      answer,
    };
    const [pageSubject, digest] = await Promise.all([
      this.pages.subject(request.pagePath, request.locale),
      this.tripDigestOf(toolContext, pickTrip(trips)),
    ]);
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
          pageSubject,
          planner: request.planner,
          trips,
          tripDigest: digest,
          // Only the first question of a chat answers the greeting.
          opener: chat.history.length === 0 ? (request.opener ?? null) : null,
          liveRegions: request.liveRegions ?? null,
          interview: request.interview ?? null,
          alerts: request.alerts,
          favorites: request.favorites,
          riddenCount: request.ridden?.length ?? 0,
          lookedUp: await this.namesOf(chat.lookedUp ?? []),
        }),
      },
    ];
    const messages: Anthropic.MessageParam[] = [
      ...chat.history,
      { role: "user", content: request.message },
    ];
    const usage: Usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };
    const toolCalls: AssistantToolCall[] = [];
    // Chips the model wrote as text never reach the page; they go out as the replies tool's.
    const guard = new RepliesTextGuard();
    const show = (piece: string) => {
      const shown = guard.push(piece);
      if (shown) emit({ type: "delta", text: shown });
    };
    const runTool = async (name: string, input: unknown) => {
      const status = toolStatus(name);
      if (status) emit({ type: "status", tool: status });
      const result = await runAssistantTool(name, input, toolContext, (error) =>
        this.logger.error(`Tool ${name} failed: ${(error as Error).message}`),
      );
      if (result.action) emit({ type: "action", action: result.action });
      toolCalls.push({
        name,
        input: storedToolInput(input),
        ...(result.isError && {
          error: true,
          errorKind: result.errorKind,
        }),
        ...(result.issues && { issues: result.issues }),
        ...(result.ignoredKeys && { ignoredKeys: result.ignoredKeys }),
      });
      return result;
    };
    /** The running round's usage from its stream events, for a round that never finishes. */
    let partial: Usage | null = null;

    let breakpoint: Anthropic.CacheControlEphemeral | null = null;
    try {
      for (let round = 0; round < MAX_ROUNDS; round++) {
        answer.round = round;
        const lastRound = round === MAX_ROUNDS - 1;
        breakpoint = moveCacheBreakpoint(messages, breakpoint);
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
        let separated = !guard.started;
        stream.on("text", (delta) => {
          if (!separated) {
            separated = true;
            show("\n\n");
          }
          show(delta);
        });
        const reply = await stream.finalMessage();
        partial = null;
        addUsage(usage, reply.usage);

        const calls = reply.content.filter(
          (block): block is Anthropic.ToolUseBlock => block.type === "tool_use",
        );
        if (reply.stop_reason !== "tool_use" || calls.length === 0) break;

        messages.push({ role: "assistant", content: reply.content });
        // The replies close an answer: alone in a round they need no round after them, and beside
        // a lookup they would sit under text the next round is still to write.
        const repliesOnly = calls.every((call) => call.name === REPLIES_TOOL);
        const results = await Promise.all(
          calls.map(async (call) => {
            if (call.name === REPLIES_TOOL && !repliesOnly) {
              return {
                type: "tool_result" as const,
                tool_use_id: call.id,
                content:
                  "Not shown: call suggest_replies last, alone, after your answer's text",
                is_error: true,
              };
            }
            const result = await runTool(call.name, call.input);
            return {
              type: "tool_result" as const,
              tool_use_id: call.id,
              content: result.content,
              ...(result.isError && { is_error: true }),
            };
          }),
        );
        messages.push({ role: "user", content: results });
        if (repliesOnly) break;
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
      guard.finish();
      await this.record(
        conversation,
        guard.text,
        toolCalls,
        usage,
        chat.sessionHash,
      );
      emit({ type: "error", code: "upstream" });
      return;
    }

    const { rest, replies } = guard.finish();
    if (rest) emit({ type: "delta", text: rest });
    if (replies?.length && !answer.done.has(REPLIES_TOOL)) {
      this.logger.warn("Replies written as text: moved to suggest_replies");
      await runTool(REPLIES_TOOL, { replies });
    }
    const message = await this.record(
      conversation,
      guard.text,
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

  /**
   * The trip on screen in the hub's planner, planned as the planner sees it, one line a day: what
   * Parky's unasked hints read. Null without such a trip, past the budget or on a refusal; the
   * trip's own line in "This chat" says then to read it with get_trip_plan.
   */
  private async tripDigestOf(
    ctx: AssistantToolContext,
    trip: VisitorTrip | null,
  ): Promise<string[] | null> {
    if (!trip?.onScreen) return null;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const late = new Promise<null>((resolve) => {
      timer = setTimeout(() => resolve(null), TRIP_DIGEST_BUDGET_MS);
    });
    try {
      const itinerary = await Promise.race([
        visitorTripItinerary(ctx, trip).catch((error: unknown) => {
          this.logger.warn(`Trip digest failed: ${describeError(error)}`);
          return null;
        }),
        late,
      ]);
      return itinerary ? tripDigest(itinerary) : null;
    } finally {
      clearTimeout(timer);
    }
  }

  /**
   * Names for the ids earlier answers used: two primary-key reads, so Parky can reuse an id
   * instead of searching again. An id that no longer resolves is left out.
   */
  private async namesOf(ids: LookedUpId[]): Promise<LookedUpEntity[]> {
    if (ids.length === 0) return [];
    const data = this.moduleRef.get(DataSource, { strict: false });
    const idsOf = (kind: LookedUpId["kind"]) =>
      ids.filter((ref) => ref.kind === kind).map((ref) => ref.id);
    try {
      const [parks, rides] = await Promise.all([
        data.getRepository(Park).find({
          where: { id: In(idsOf("park")) },
          select: { id: true, name: true },
        }),
        data.getRepository(Attraction).find({
          where: { id: In(idsOf("ride")) },
          relations: { park: true },
          select: { id: true, name: true, park: { id: true, name: true } },
        }),
      ]);
      const names = new Map<string, LookedUpEntity>();
      for (const park of parks) {
        names.set(park.id, { kind: "park", id: park.id, name: park.name });
      }
      for (const ride of rides) {
        names.set(ride.id, {
          kind: "ride",
          id: ride.id,
          name: ride.name,
          ...(ride.park?.name && { park: ride.park.name }),
        });
      }
      return ids.flatMap((ref) => names.get(ref.id) ?? []);
    } catch (error) {
      // A context line is a convenience: without it Parky searches again, as before.
      this.logger.warn(`Looked-up names failed: ${describeError(error)}`);
      return [];
    }
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
        promptVersion: SYSTEM_PROMPT_VERSION,
      }),
    );
    await this.conversations
      .createQueryBuilder()
      .update()
      .set({
        messageCount: () => `"messageCount" + 2`,
        totalTokens: () => `"totalTokens" + ${tokens}`,
        costUsd: () => `"costUsd" + ${cost.toFixed(6)}`,
        promptVersion: SYSTEM_PROMPT_VERSION,
      })
      .where("id = :id", { id: conversation.id })
      .execute();
    return message;
  }
}

/**
 * Puts the one cache breakpoint of the conversation on its last block, and takes it off the block
 * that had it. The next round then reads the question, the history and every earlier tool result
 * from the cache at a tenth of the price, instead of paying for all of it again; one moving
 * breakpoint is enough, because a read looks back from it for the longest prefix cached before.
 */
export function moveCacheBreakpoint(
  messages: Anthropic.MessageParam[],
  previous: Anthropic.CacheControlEphemeral | null,
): Anthropic.CacheControlEphemeral {
  const last = messages[messages.length - 1];
  if (typeof last.content === "string") {
    last.content = [{ type: "text", text: last.content }];
  }
  for (const message of messages) {
    if (typeof message.content === "string") continue;
    for (const block of message.content) {
      if ("cache_control" in block && block.cache_control === previous) {
        delete block.cache_control;
      }
    }
  }
  const block = last.content[last.content.length - 1] as {
    cache_control?: Anthropic.CacheControlEphemeral | null;
  };
  const control: Anthropic.CacheControlEphemeral = { type: "ephemeral" };
  block.cache_control = control;
  return control;
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

/** A park or ride id an earlier answer used. */
export interface LookedUpId {
  kind: "park" | "ride";
  id: string;
}

/**
 * The park and ride ids earlier answers passed to their tools, newest first and at most
 * {@link MAX_LOOKED_UP}: the stored tool calls keep their inputs, not their results, so a
 * `search_parks` names nothing, but the id it found shows up in the next call. Failed calls are
 * skipped. shortcut: a park given by slug is skipped (slugs repeat across parks); Parky passes ids.
 */
export function lookedUpIds(
  messages: Array<Pick<AssistantMessage, "toolCalls">>,
): LookedUpId[] {
  const found: LookedUpId[] = [];
  const seen = new Set<string>();
  const add = (kind: LookedUpId["kind"], value: unknown) => {
    const id = typeof value === "string" ? value.toLowerCase() : null;
    if (!isCanonicalUuid(id) || seen.has(id)) return;
    seen.add(id);
    found.push({ kind, id });
  };
  const list = (value: unknown): unknown[] =>
    Array.isArray(value) ? value : [];
  for (const message of [...messages].reverse()) {
    for (const call of [...(message.toolCalls ?? [])].reverse()) {
      if (call.error || !call.input || typeof call.input !== "object") continue;
      const input = call.input as Record<string, unknown>;
      add("ride", input.rideId);
      list(input.rideIds).forEach((id) => add("ride", id));
      add("park", input.park);
      add("park", input.parkId);
      list(input.parks).forEach((id) => add("park", id));
    }
  }
  return found.slice(0, MAX_LOOKED_UP);
}

/** The visitor's question before this one, as they typed it, or null in a new chat. */
export function lastQuestion(history: Anthropic.MessageParam[]): string | null {
  for (let i = history.length - 1; i >= 0; i--) {
    const message = history[i];
    if (message.role === "user" && typeof message.content === "string") {
      return message.content;
    }
  }
  return null;
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
