import type { ModuleRef } from "@nestjs/core";
import type { Repository } from "typeorm";
import type { AssistantSettingsService } from "./assistant-settings.service";
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
        requests.push(params);
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
  type ChatEvent,
} from "./assistant-chat.service";

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
    const service = new AssistantChatService(
      settings,
      moduleRef,
      repository<AssistantConversation>({}),
      repository<AssistantMessage>({ id: "message-1" }),
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
