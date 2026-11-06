import type { ModuleRef } from "@nestjs/core";
import type { Repository } from "typeorm";
import type { AssistantSettingsService } from "./assistant-settings.service";
import type { AssistantPageService } from "./assistant-page.service";
import type { AssistantConversation } from "./entities/assistant-conversation.entity";
import type { AssistantMessage } from "./entities/assistant-message.entity";

const requests: Array<Record<string, unknown>> = [];

/** Every request answers with a tool call, so only the round limit ends the loop. */
jest.mock("@anthropic-ai/sdk", () => {
  class APIError extends Error {}
  class Anthropic {
    static APIError = APIError;
    messages = {
      stream: (params: Record<string, unknown>) => {
        // A copy: the loop goes on changing its messages after the request is sent.
        requests.push({
          ...params,
          messages: structuredClone(params.messages),
        });
        const answersInText =
          (params.tool_choice as { type: string }).type === "none";
        const listeners: Array<(text: string) => void> = [];
        return {
          on: (_event: string, listener: (text: string) => void) => {
            listeners.push(listener);
          },
          finalMessage: () => {
            if (answersInText) listeners.forEach((l) => l("Fertig!"));
            return Promise.resolve({
              stop_reason: answersInText ? "end_turn" : "tool_use",
              usage: { input_tokens: 100, output_tokens: 10 },
              content: answersInText
                ? [{ type: "text", text: "Fertig!" }]
                : [
                    {
                      type: "tool_use",
                      id: `call-${requests.length}`,
                      name: "get_post",
                      input: { slug: "a-post" },
                    },
                  ],
            });
          },
        };
      },
    };
  }
  return { __esModule: true, default: Anthropic };
});

import {
  AssistantChatService,
  historyOf,
  lookedUpIds,
  type ChatEvent,
} from "./assistant-chat.service";
import {
  chatContext,
  SYSTEM_PROMPT,
  SYSTEM_PROMPT_VERSION,
} from "./system-prompt";

function repository<T extends object>(saved: Partial<T>) {
  return {
    create: (value: Partial<T>) => value,
    save: (value: Partial<T>) => Promise.resolve({ ...saved, ...value }),
    createQueryBuilder: () => {
      const builder = {
        update: () => builder,
        set: () => builder,
        where: () => builder,
        execute: () => Promise.resolve(),
      };
      return builder;
    },
  } as unknown as Repository<T>;
}

describe("AssistantChatService.answer", () => {
  it("stops calling tools after four rounds and answers in text", async () => {
    const settings = {
      addTokens: jest.fn(),
    } as unknown as AssistantSettingsService;
    const posts = { post: jest.fn().mockResolvedValue({ title: "A post" }) };
    const moduleRef = { get: () => posts } as unknown as ModuleRef;
    const messages = repository<AssistantMessage>({ id: "message-1" });
    const saveMessage = jest.spyOn(messages, "save");
    const service = new AssistantChatService(
      settings,
      moduleRef,
      repository<AssistantConversation>({}),
      messages,
      {
        subject: () => Promise.resolve(null),
      } as unknown as AssistantPageService,
    );

    const events: ChatEvent[] = [];
    await service.answer(
      {
        apiKey: "sk-ant-test",
        conversation: { id: "chat-1" } as AssistantConversation,
        history: [],
        request: {
          locale: "de",
          pagePath: "/de",
          message: "Was gibt es Neues?",
          sessionToken: "s1.checked-by-the-controller",
        },
        session: "session-1",
        sessionHash: "session-hash",
      },
      (event) => events.push(event),
      new AbortController().signal,
      "203.0.113.7",
    );

    expect(requests).toHaveLength(4);
    expect(
      requests.map((r) => (r.tool_choice as { type: string }).type),
    ).toEqual(["auto", "auto", "auto", "none"]);
    expect(requests[0].model).toBe("claude-haiku-5-5");
    expect(posts.post).toHaveBeenCalledTimes(3);
    expect(events[0]).toEqual({ type: "start", conversationId: "chat-1" });
    expect(events.filter((e) => e.type === "status")).toHaveLength(3);
    expect(events[events.length - 1]).toEqual({
      type: "done",
      conversationId: "chat-1",
      messageId: "message-1",
    });
    expect(settings.addTokens).toHaveBeenCalledWith(440, "session-hash");
    // The admin tells answers of one prompt from another's by it.
    expect(saveMessage).toHaveBeenCalledWith(
      expect.objectContaining({
        role: "assistant",
        promptVersion: SYSTEM_PROMPT_VERSION,
      }),
    );

    // One breakpoint per request, on the last block, so each round reads the one before cached.
    for (const request of requests) {
      const messages = request.messages as Array<{
        content: Array<{ cache_control?: unknown }>;
      }>;
      const marked = messages.flatMap((message) =>
        message.content.filter((block) => block.cache_control),
      );
      const last = messages[messages.length - 1].content;
      expect(marked).toEqual([last[last.length - 1]]);
    }
  });
});

describe("historyOf", () => {
  it("drops a failed answer with its question, so the next turn is valid", () => {
    expect(
      historyOf([
        { role: "assistant", text: "orphan" },
        { role: "user", text: "Wie voll?" },
        { role: "assistant", text: "Ziemlich." },
        { role: "user", text: "Und morgen?" },
        { role: "assistant", text: "" },
      ]),
    ).toEqual([
      { role: "user", content: "Wie voll?" },
      { role: "assistant", content: "Ziemlich." },
    ]);
  });
});

describe("lookedUpIds", () => {
  const PARK = "0b5c9a52-6d44-4c3e-9a4e-5f1e2d3c4b5a";
  const RIDE = "7f3e2d1c-0b9a-4876-8543-210fedcba987";
  const OTHER = "11111111-2222-4333-8444-555555555555";

  it("lists the ids earlier tool calls used, newest first, once each", () => {
    expect(
      lookedUpIds([
        {
          toolCalls: [
            { name: "get_park_live", input: { park: PARK.toUpperCase() } },
            { name: "get_ride", input: { rideId: RIDE } },
            // A failed call and a slug name nothing.
            { name: "get_ride", input: { rideId: OTHER }, error: true },
            { name: "get_park_live", input: { park: "epic-universe" } },
          ],
        },
        { toolCalls: null },
        {
          toolCalls: [
            { name: "search_parks", input: { query: "epic universe" } },
            { name: "show_card", input: { kind: "rides", park: PARK } },
          ],
        },
      ]),
    ).toEqual([
      { kind: "park", id: PARK },
      { kind: "ride", id: RIDE },
    ]);
  });

  it("keeps at most eight", () => {
    const ids = Array.from(
      { length: 12 },
      (_, i) => `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`,
    );
    const found = lookedUpIds(
      ids.map((rideId) => ({
        toolCalls: [{ name: "get_ride", input: { rideId } }],
      })),
    );
    expect(found).toHaveLength(8);
    expect(found[0].id).toBe(ids[11]);
  });

  it("goes into the uncached chat block with names", () => {
    const context = chatContext({
      locale: "de",
      now: "2026-11-05 14:00",
      timeZone: "America/New_York",
      pagePath: "/de",
      lookedUp: [
        {
          kind: "ride",
          id: RIDE,
          name: "Monsters Unchained",
          park: "Epic Universe",
        },
        { kind: "park", id: PARK, name: "Epic Universe" },
      ],
    });
    expect(context).toContain("use these ids instead of searching again");
    expect(context).toContain(
      `- ride Monsters Unchained (Epic Universe): ${RIDE}`,
    );
    expect(context).toContain(`- park Epic Universe: ${PARK}`);
    expect(SYSTEM_PROMPT).not.toContain("Looked up earlier");
  });
});
