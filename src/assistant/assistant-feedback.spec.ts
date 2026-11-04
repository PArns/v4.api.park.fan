import type { Repository } from "typeorm";
import {
  AssistantRecordsService,
  CHAT_FEEDBACK_PER_CONVERSATION,
} from "./assistant-records.service";
import type { AssistantConversation } from "./entities/assistant-conversation.entity";
import type { AssistantMessage } from "./entities/assistant-message.entity";
import type { SiteFeedback } from "./entities/site-feedback.entity";
import {
  feedbackQuote,
  runAssistantTool,
  type AssistantToolContext,
} from "./tools/assistant-tools";

const CHAT = "00000000-0000-4000-8000-000000000001";
const OTHER_CHAT = "00000000-0000-4000-8000-000000000002";

/** The records service on an in-memory feedback table; the chat belongs to `hash-1`. */
function setup({ allowed = true } = {}) {
  const rows: Array<Partial<SiteFeedback>> = [];
  const feedback = {
    find: ({ where }: { where: Partial<SiteFeedback> }) =>
      Promise.resolve(
        rows.filter(
          (row) =>
            row.conversationId === where.conversationId &&
            row.source === where.source,
        ),
      ),
    create: (value: Partial<SiteFeedback>) => value,
    save: (value: Partial<SiteFeedback>) => {
      rows.push(value);
      return Promise.resolve({ id: `feedback-${rows.length}`, ...value });
    },
  } as unknown as Repository<SiteFeedback>;
  const conversations = {
    existsBy: ({ id, sessionHash }: { id: string; sessionHash: string }) =>
      Promise.resolve(id === CHAT && sessionHash === "hash-1"),
  } as unknown as Repository<AssistantConversation>;
  const records = new AssistantRecordsService(
    conversations,
    {} as Repository<AssistantMessage>,
    feedback,
  );
  const limiter = {
    feedback: jest.fn().mockResolvedValue({ allowed, retryAfterSeconds: 0 }),
  };
  const get = <T>(token: unknown) =>
    (token === AssistantRecordsService ? records : limiter) as T;
  /** One answer to the question `message`, asked after `previousMessage`. */
  const answerTo = (
    message: string,
    previousMessage: string | null = null,
  ): AssistantToolContext => ({
    get,
    locale: "de",
    ip: "203.0.113.7",
    chat: {
      conversationId: CHAT,
      pagePath: "/de/parks/europe/netherlands",
      session: "session-1",
      sessionHash: "hash-1",
      message,
      previousMessage,
    },
    answer: { round: 0, done: new Set() },
  });
  const submit = (context: AssistantToolContext, input: unknown) =>
    runAssistantTool("submit_feedback", input, context, jest.fn());
  return { rows, records, limiter, answerTo, submit };
}

describe("submit_feedback", () => {
  it("stores the visitor's own words as chat feedback, with the page and language", async () => {
    const { rows, submit, answerTo } = setup();
    const wish =
      "Kannst du einen Vorschlag einreichen, dass die Eisstände mit aufgenommen werden?";
    const result = await submit(answerTo(wish), {
      kind: "feature_wish",
      // What the model may add beyond its two fields is dropped, never stored.
      message: "<img src=x onerror=alert(1)>",
      email: "attacker@example.com",
      pagePath: "https://evil.example",
      conversationId: OTHER_CHAT,
    });
    expect(result.isError).toBe(false);
    expect(result.action).toEqual({ type: "feedback_sent" });
    expect(rows).toEqual([
      {
        message: wish,
        email: null,
        locale: "de",
        pagePath: "/de/parks/europe/netherlands",
        conversationId: CHAT,
        source: "chat",
        kind: "feature_wish",
      },
    ]);
  });

  it("quotes the message before a yes, as it was typed", async () => {
    const { rows, submit, answerTo } = setup();
    const missing =
      "dann füge doch Kinderstaad hinzu. Der ist in Heerlen, Niederlande";
    await submit(answerTo("ja, gib das weiter", missing), {
      kind: "missing_park_or_ride",
      quote: "previous_message",
    });
    expect(rows.map((row) => row.message)).toEqual([missing]);
  });

  it("sends nothing for a yes alone, junk, or a chat with nothing before", async () => {
    const { rows, submit, answerTo } = setup();
    for (const [message, quote] of [
      ["ok mach", "this_message"],
      ["sdfghjklqwrtzp", "this_message"],
      ["Bitte gib das an das Team weiter", "previous_message"],
    ] as const) {
      const refused = await submit(answerTo(message), {
        kind: "bug",
        quote,
      });
      expect(refused.isError).toBe(true);
      expect(refused.action).toBeUndefined();
    }
    expect(rows).toHaveLength(0);
  });

  it("sends once per answer, even when the model calls it twice at once", async () => {
    const { rows, submit, answerTo } = setup();
    const answer = answerTo(
      "Die Wartezeit von Taron zeigt seit Tagen 999 Minuten.",
    );
    const results = await Promise.all([
      submit(answer, { kind: "wrong_data" }),
      submit(answer, { kind: "bug" }),
    ]);
    expect(results.filter((r) => r.action)).toHaveLength(1);
    expect(rows).toHaveLength(1);
  });

  it("refuses the same words twice and caps a chat's share", async () => {
    const { rows, submit, answerTo } = setup();
    const issues = [
      "Die Wartezeit von Taron zeigt seit Tagen 999 Minuten.",
      "Kinderstaad in Heerlen fehlt bei euch noch komplett.",
      "Und eine Liste der Eisstände pro Park wäre super.",
    ];
    expect((await submit(answerTo(issues[0]), { kind: "bug" })).action).toEqual(
      {
        type: "feedback_sent",
      },
    );
    const repeat = await submit(answerTo(issues[0]), { kind: "bug" });
    expect(repeat.isError).toBe(true);
    await submit(answerTo(issues[1]), { kind: "missing_park_or_ride" });
    expect(rows).toHaveLength(CHAT_FEEDBACK_PER_CONVERSATION);
    const third = await submit(answerTo(issues[2]), { kind: "feature_wish" });
    expect(third.isError).toBe(true);
    expect(rows).toHaveLength(CHAT_FEEDBACK_PER_CONVERSATION);
  });

  it("draws on the feedback form's limits, and stores nothing past them", async () => {
    const { rows, limiter, submit, answerTo } = setup({ allowed: false });
    const refused = await submit(
      answerTo("Die Seite lädt auf meinem Handy sehr langsam."),
      {
        kind: "bug",
      },
    );
    expect(refused.isError).toBe(true);
    expect(limiter.feedback).toHaveBeenCalledWith("session-1", "203.0.113.7");
    expect(rows).toHaveLength(0);
  });

  it("refuses outside a chat", async () => {
    const { submit, answerTo } = setup();
    const { chat: _chat, ...withoutChat } = answerTo(
      "Die Seite ist super, danke!",
    );
    expect((await submit(withoutChat, { kind: "praise" })).isError).toBe(true);
  });
});

describe("feedbackQuote", () => {
  const chat = {
    conversationId: CHAT,
    pagePath: "/de",
    session: "s",
    sessionHash: "h",
    message: "  und die Eisstände bitte auch  ",
    previousMessage: "Kinderstaad in Heerlen fehlt",
  };

  it("joins both messages as typed, trimmed, and cuts at the form's length", () => {
    expect(feedbackQuote(chat, "both")).toBe(
      "Kinderstaad in Heerlen fehlt\n\nund die Eisstände bitte auch",
    );
    expect(
      feedbackQuote(
        { ...chat, message: "Taron zeigt 999 Minuten. ".repeat(100) },
        "this_message",
      )?.length,
    ).toBe(2000);
  });
});

describe("AssistantRecordsService, feedback and ratings of another session", () => {
  it("files form feedback under a chat only when the chat is the sender's", async () => {
    const { rows, records } = setup();
    const entry = {
      message: "Super Seite!",
      email: null,
      locale: "de",
      pagePath: "/de",
      conversationId: CHAT,
    };
    await records.addFeedback({ ...entry, sessionHash: "hash-1" });
    await records.addFeedback({ ...entry, sessionHash: "someone-else" });
    expect(rows.map((row) => row.conversationId)).toEqual([CHAT, null]);
  });

  it("rates nothing in another session's chat", async () => {
    const update = jest.fn();
    const records = new AssistantRecordsService(
      {
        existsBy: () => Promise.resolve(false),
      } as unknown as Repository<AssistantConversation>,
      { update } as unknown as Repository<AssistantMessage>,
      {} as Repository<SiteFeedback>,
    );
    await expect(records.rate(CHAT, OTHER_CHAT, -1, "hash-2")).resolves.toBe(
      false,
    );
    expect(update).not.toHaveBeenCalled();
  });
});
