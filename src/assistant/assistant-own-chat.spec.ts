import type { ModuleRef } from "@nestjs/core";
import type { Repository } from "typeorm";
import { hashRateLimitKey } from "../common/redis/rate-limit.util";
import { AssistantChallengeService } from "./assistant-challenge.service";
import type { AssistantChatService } from "./assistant-chat.service";
import { AssistantController } from "./assistant.controller";
import { AssistantRateLimitService } from "./assistant-rate-limit.service";
import { AssistantRecordsService } from "./assistant-records.service";
import { readMasterKey } from "./assistant-secret.util";
import {
  issueSessionToken,
  SESSION_TTL_SECONDS,
  sessionKey,
} from "./assistant-session.util";
import type { AssistantSettingsService } from "./assistant-settings.service";
import type { AssistantConversation } from "./entities/assistant-conversation.entity";
import type { AssistantMessage } from "./entities/assistant-message.entity";
import type { SiteFeedback } from "./entities/site-feedback.entity";
import { chatMessage, chatSummary } from "../mcp/tools/assistant-records";

/**
 * A visitor's own chat (PAR-1430): reporting one of its answers and deleting it from the server,
 * through the controller with real session tokens over an in-memory chat table.
 */

const SECRET = "ab".repeat(32);
const KEY = sessionKey(readMasterKey(SECRET)!);
const CHAT = "00000000-0000-4000-8000-000000000001";
const ANSWER = "00000000-0000-4000-8000-0000000000a1";
const QUESTION = "00000000-0000-4000-8000-0000000000a0";
const NOWHERE = "00000000-0000-4000-8000-00000000dead";

/** The session id a token carries: its nonce, the third part. */
const sessionOf = (token: string) => token.split(".")[2];
const bearer = (token: string) => `Bearer ${token}`;

function counters() {
  const values = new Map<string, number>();
  return {
    incr: (key: string) => {
      values.set(key, (values.get(key) ?? 0) + 1);
      return Promise.resolve(values.get(key));
    },
    expire: () => Promise.resolve(1),
    ttl: () => Promise.resolve(3600),
  };
}

function setup({ statsFail = false, appSession = false } = {}) {
  const owner = issueSessionToken(
    KEY,
    Date.now(),
    appSession ? "key-app" : undefined,
  );
  const stranger = issueSessionToken(KEY);
  const expired = issueSessionToken(
    KEY,
    Date.now() - (SESSION_TTL_SECONDS + 60) * 1000,
  );

  const chats = new Map<string, Partial<AssistantConversation>>([
    [
      CHAT,
      {
        id: CHAT,
        sessionHash: hashRateLimitKey(sessionOf(owner)),
        apiKeyId: null,
        reportedAt: null,
      },
    ],
  ]);
  const messages: Array<Partial<AssistantMessage>> = [
    { id: QUESTION, conversationId: CHAT, role: "user", text: "Was ist los?" },
    {
      id: ANSWER,
      conversationId: CHAT,
      role: "assistant",
      text: "Die Erde ist flach.",
      rating: null,
      promptVersion: null,
      reportReason: null,
      reportNote: null,
      reportedAt: null,
    },
  ];
  const feedbackRows: Array<Partial<SiteFeedback>> = [
    { id: "f1", conversationId: CHAT, message: "Danke" },
  ];
  /** Every statement the counted delete ran, by its first words. */
  const statements: string[] = [];

  const removeChat = (id: string) => {
    const had = chats.delete(id);
    // The foreign key's cascade.
    for (let i = messages.length - 1; i >= 0; i--) {
      if (messages[i].conversationId === id) messages.splice(i, 1);
    }
    return had;
  };
  const manager = {
    query: (sql: string, params: unknown[]) => {
      const head = sql.trim().split(/\s+/).slice(0, 3).join(" ");
      statements.push(head);
      if (head.startsWith("SELECT id FROM")) {
        const chat = chats.get(params[0] as string);
        return Promise.resolve(
          chat && chat.sessionHash === params[1] ? [{ id: chat.id }] : [],
        );
      }
      if (head.startsWith("INSERT INTO")) {
        return statsFail
          ? Promise.reject(new Error("relation does not exist"))
          : Promise.resolve([]);
      }
      for (const id of params[0] as string[]) removeChat(id);
      return Promise.resolve([]);
    },
  };
  const conversations = {
    existsBy: (where: { id: string; sessionHash: string }) =>
      Promise.resolve(chats.get(where.id)?.sessionHash === where.sessionHash),
    findOneBy: ({ id }: { id: string }) =>
      Promise.resolve(chats.get(id) ?? null),
    update: ({ id }: { id: string }, change: object) => {
      Object.assign(chats.get(id) ?? {}, change);
      return Promise.resolve({ affected: 1 });
    },
    delete: ({ id }: { id: string }) =>
      Promise.resolve({ affected: removeChat(id) ? 1 : 0 }),
    manager: {
      // A failed statement rolls the whole transaction back: nothing it did counts.
      transaction: async (run: (m: typeof manager) => Promise<void>) => {
        const before = new Map(chats);
        const kept = [...messages];
        try {
          await run(manager);
        } catch (error) {
          chats.clear();
          for (const [id, chat] of before) chats.set(id, chat);
          messages.splice(0, messages.length, ...kept);
          throw error;
        }
      },
    },
  } as unknown as Repository<AssistantConversation>;
  const messageRepo = {
    update: (where: Partial<AssistantMessage>, change: object) => {
      const hit = messages.filter(
        (m) =>
          m.id === where.id &&
          m.conversationId === where.conversationId &&
          m.role === where.role,
      );
      for (const message of hit) Object.assign(message, change);
      return Promise.resolve({ affected: hit.length });
    },
    find: ({ where }: { where: { conversationId: string } }) =>
      Promise.resolve(
        messages.filter((m) => m.conversationId === where.conversationId),
      ),
  } as unknown as Repository<AssistantMessage>;
  const feedback = {
    update: (where: { conversationId: string }, change: object) => {
      for (const row of feedbackRows) {
        if (row.conversationId === where.conversationId) {
          Object.assign(row, change);
        }
      }
      return Promise.resolve({ affected: 1 });
    },
    find: ({ where }: { where: { conversationId: string } }) =>
      Promise.resolve(
        feedbackRows.filter((f) => f.conversationId === where.conversationId),
      ),
  } as unknown as Repository<SiteFeedback>;

  const records = new AssistantRecordsService(
    conversations,
    messageRepo,
    feedback,
  );
  const controller = new AssistantController(
    {} as AssistantSettingsService,
    {} as AssistantChatService,
    records,
    new AssistantRateLimitService(counters() as never),
    new AssistantChallengeService({} as never),
    {} as ModuleRef,
  );
  return {
    controller,
    records,
    chats,
    messages,
    feedbackRows,
    statements,
    owner,
    stranger,
    expired,
  };
}

describe("a visitor's own chat (PAR-1430)", () => {
  const before = process.env.ASSISTANT_SECRET_KEY;
  beforeAll(() => {
    process.env.ASSISTANT_SECRET_KEY = SECRET;
  });
  afterAll(() => {
    if (before === undefined) delete process.env.ASSISTANT_SECRET_KEY;
    else process.env.ASSISTANT_SECRET_KEY = before;
  });

  describe("DELETE /v1/assistant/conversations/:id", () => {
    it("deletes the chat and its messages for the session that had it, and keeps the day's numbers", async () => {
      const { controller, chats, messages, feedbackRows, statements, owner } =
        setup();
      await expect(
        controller.deleteConversation(CHAT, bearer(owner)),
      ).resolves.toBeUndefined();
      expect(chats.size).toBe(0);
      expect(messages).toHaveLength(0);
      // Feedback is the visitor's message to the site: it stays, without the link.
      expect(feedbackRows[0]).toMatchObject({ id: "f1", conversationId: null });
      // Locked, counted into the four statistics, then deleted, in one transaction.
      expect(statements).toEqual([
        "SELECT id FROM",
        "INSERT INTO assistant_day_stats",
        "INSERT INTO assistant_usage_day_stats",
        "INSERT INTO assistant_day_stats",
        "INSERT INTO assistant_tool_day_stats",
        "DELETE FROM assistant_conversations",
      ]);
    });

    it("works the same for an app session, with the token alone", async () => {
      const { controller, chats, owner } = setup({ appSession: true });
      await controller.deleteConversation(CHAT, bearer(owner));
      expect(chats.size).toBe(0);
    });

    it("is idempotent: the second call answers the same and changes nothing", async () => {
      const { controller, chats, statements, owner } = setup();
      await controller.deleteConversation(CHAT, bearer(owner));
      const after = statements.length;
      await expect(
        controller.deleteConversation(CHAT, bearer(owner)),
      ).resolves.toBeUndefined();
      expect(chats.size).toBe(0);
      expect(statements).toHaveLength(after);
    });

    it("deletes nothing with another session's token, and answers as for a chat that never was", async () => {
      const { controller, chats, messages, statements, stranger } = setup();
      await expect(
        controller.deleteConversation(CHAT, bearer(stranger)),
      ).resolves.toBeUndefined();
      await expect(
        controller.deleteConversation(NOWHERE, bearer(stranger)),
      ).resolves.toBeUndefined();
      expect(chats.has(CHAT)).toBe(true);
      expect(messages).toHaveLength(2);
      expect(statements).toEqual([]);
    });

    it.each([
      ["an expired token", (s: ReturnType<typeof setup>) => bearer(s.expired)],
      ["no token", () => undefined],
      ["a token without the scheme", (s: ReturnType<typeof setup>) => s.owner],
      [
        "a token with a changed signature",
        (s: ReturnType<typeof setup>) => bearer(`${s.owner.slice(0, -2)}xx`),
      ],
    ])("refuses %s with 401 and deletes nothing", async (_name, header) => {
      const state = setup();
      await expect(
        state.controller.deleteConversation(CHAT, header(state)),
      ).rejects.toMatchObject({ status: 401, message: "challenge" });
      expect(state.chats.has(CHAT)).toBe(true);
      expect(state.messages).toHaveLength(2);
    });

    it("still deletes the chat when the statistics cannot be written", async () => {
      const { controller, chats, messages, owner } = setup({ statsFail: true });
      await controller.deleteConversation(CHAT, bearer(owner));
      expect(chats.size).toBe(0);
      expect(messages).toHaveLength(0);
    });

    it("is limited per session", async () => {
      const { controller, owner } = setup();
      for (let i = 0; i < 60; i++) {
        await controller.deleteConversation(NOWHERE, bearer(owner));
      }
      await expect(
        controller.deleteConversation(CHAT, bearer(owner)),
      ).rejects.toMatchObject({ status: 429 });
    });
  });

  describe("POST /v1/assistant/report", () => {
    const report = (sessionToken: string | undefined, extra: object = {}) => ({
      conversationId: CHAT,
      messageId: ANSWER,
      reason: "wrong",
      sessionToken,
      ...extra,
    });

    it("stores the reason and the note at the answer, and the admin and the MCP tools show it", async () => {
      const { controller, records, owner } = setup();
      await expect(
        controller.report(report(owner, { note: "  Das stimmt nicht.  " })),
      ).resolves.toBeUndefined();

      const chat = (await records.conversation(CHAT))!;
      expect(chat.reportedAt).toBeInstanceOf(Date);
      const answer = chat.messages.find((m) => m.id === ANSWER)!;
      expect(answer).toMatchObject({
        reportReason: "wrong",
        reportNote: "Das stimmt nicht.",
      });
      expect(answer.reportedAt).toEqual(chat.reportedAt);
      expect(chat.messages.find((m) => m.id === QUESTION)).not.toHaveProperty(
        "reportReason",
      );
      expect(chatSummary(chat as never).reportedAt).toEqual(chat.reportedAt);
      expect(chatMessage(answer).report).toEqual({
        reason: "wrong",
        note: "Das stimmt nicht.",
        at: answer.reportedAt,
      });
    });

    it("takes a report without a note, and a second one replaces the first", async () => {
      const { controller, messages, owner } = setup();
      await controller.report(report(owner, { note: "erst" }));
      await controller.report(report(owner, { reason: "inappropriate" }));
      expect(messages[1]).toMatchObject({
        reportReason: "inappropriate",
        reportNote: null,
      });
    });

    it("leaves an unreported answer without a report for the MCP tools", () => {
      const { messages } = setup();
      expect(chatMessage(messages[1] as AssistantMessage)).not.toHaveProperty(
        "report",
      );
    });

    it("answers 404 for another session's chat, a question, or an answer that is not there, and stores nothing", async () => {
      const { controller, chats, messages, owner, stranger } = setup();
      for (const body of [
        report(stranger),
        report(owner, { messageId: QUESTION }),
        report(owner, { messageId: NOWHERE }),
        report(owner, { conversationId: NOWHERE }),
      ]) {
        await expect(controller.report(body)).rejects.toMatchObject({
          status: 404,
        });
      }
      expect(messages[1].reportedAt).toBeNull();
      expect(chats.get(CHAT)!.reportedAt).toBeNull();
    });

    it("refuses an expired token with 401, and a missing token, an unknown reason or a long note with 400", async () => {
      const { controller, messages, owner, expired } = setup();
      await expect(controller.report(report(expired))).rejects.toMatchObject({
        status: 401,
      });
      for (const body of [
        report(undefined),
        report(owner, { reason: "spam" }),
        report(owner, { note: "x".repeat(501) }),
        report(owner, { extra: true }),
      ]) {
        await expect(controller.report(body)).rejects.toMatchObject({
          status: 400,
        });
      }
      expect(messages[1].reportedAt).toBeNull();
    });

    it("takes five an hour per session, apart from the feedback's share", async () => {
      const { controller, owner } = setup();
      for (let i = 0; i < 5; i++) await controller.report(report(owner));
      await expect(controller.report(report(owner))).rejects.toMatchObject({
        status: 429,
      });
    });
  });
});

describe("AssistantRecordsService.listConversations, reported", () => {
  it("narrows the list to chats with a reported answer, or to those without", async () => {
    const where: string[] = [];
    const builder = {
      andWhere: (clause: string) => {
        where.push(clause);
        return builder;
      },
      orderBy: () => builder,
      take: () => builder,
      skip: () => builder,
      getManyAndCount: () => Promise.resolve([[], 0]),
    };
    const records = new AssistantRecordsService(
      {
        createQueryBuilder: () => builder,
      } as unknown as Repository<AssistantConversation>,
      {} as Repository<AssistantMessage>,
      {} as Repository<SiteFeedback>,
    );
    await records.listConversations({ page: 1, reported: true });
    await records.listConversations({ page: 1, reported: false });
    await records.listConversations({ page: 1 });
    expect(where).toEqual(["c.reportedAt IS NOT NULL", "c.reportedAt IS NULL"]);
  });
});
