import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { AssistantConversation } from "./entities/assistant-conversation.entity";
import { AssistantMessage } from "./entities/assistant-message.entity";
import {
  SiteFeedback,
  type FeedbackKind,
  type FeedbackSource,
  type FeedbackStatus,
} from "./entities/site-feedback.entity";

/** How long chats are kept; feedback is kept until it is deleted by hand. */
export const CONVERSATION_RETENTION_DAYS = 7;

/** Chats one prune transaction deletes. */
const PRUNE_BATCH = 2000;

/** Rows per page of the admin's lists; the MCP tools ask for fewer. */
export const RECORDS_PAGE_SIZE = 50;

/** Feedback Parky passes on from one chat at most: a second, different issue still goes in. */
export const CHAT_FEEDBACK_PER_CONVERSATION = 2;

/**
 * The stored side of Parky: ratings and feedback from visitors, the lists the
 * admin reads, and the retention of chats.
 */
@Injectable()
export class AssistantRecordsService {
  constructor(
    @InjectRepository(AssistantConversation)
    private readonly conversations: Repository<AssistantConversation>,
    @InjectRepository(AssistantMessage)
    private readonly messages: Repository<AssistantMessage>,
    @InjectRepository(SiteFeedback)
    private readonly feedback: Repository<SiteFeedback>,
  ) {}

  /**
   * False when the answer is not Parky's, not in that chat, or the chat is another session's: a
   * chat id seen elsewhere, in the admin or a log, rates nothing.
   */
  async rate(
    conversationId: string,
    messageId: string,
    rating: 1 | -1,
    sessionHash: string,
  ) {
    if (
      !(await this.conversations.existsBy({ id: conversationId, sessionHash }))
    ) {
      return false;
    }
    const result = await this.messages.update(
      { id: messageId, conversationId, role: "assistant" },
      { rating },
    );
    if (!result.affected) return false;
    // A chat with any thumbs down is listed as down, so the admin finds it.
    await this.conversations.query(
      `UPDATE assistant_conversations
          SET rating = CASE WHEN EXISTS (
                SELECT 1 FROM assistant_messages
                 WHERE "conversationId" = $1::uuid AND rating = -1
              ) THEN -1 ELSE 1 END
        WHERE id = $1::uuid`,
      [conversationId],
    );
    return true;
  }

  async addFeedback(entry: {
    message: string;
    email: string | null;
    locale: string;
    pagePath: string;
    conversationId: string | null;
    /** The sender's session: only a chat it had itself is linked. */
    sessionHash: string;
    source?: FeedbackSource;
    kind?: FeedbackKind | null;
  }): Promise<string> {
    const { sessionHash, conversationId: chatId, ...fields } = entry;
    // A chat id that does not exist, or is another session's, is dropped rather than refused:
    // the feedback is worth keeping either way, but nobody files it under a stranger's chat.
    const conversationId =
      chatId && (await this.conversations.existsBy({ id: chatId, sessionHash }))
        ? chatId
        : null;
    const saved = await this.feedback.save(
      this.feedback.create({ ...fields, conversationId }),
    );
    return saved.id;
  }

  /**
   * Why Parky may not pass `message` on from this chat: its share is used up (`cap`), or the same
   * words went in already (`repeat`). Null when it may.
   */
  async chatFeedbackRefusal(
    conversationId: string,
    message: string,
  ): Promise<"cap" | "repeat" | null> {
    const sent = await this.feedback.find({
      where: { conversationId, source: "chat" },
      select: { message: true },
    });
    if (sent.some((entry) => entry.message === message)) return "repeat";
    return sent.length >= CHAT_FEEDBACK_PER_CONVERSATION ? "cap" : null;
  }

  async listConversations(params: {
    rating?: number;
    q?: string;
    /** Only chats whose latest answer ran on this prompt version. */
    promptVersion?: number;
    page: number;
    pageSize?: number;
  }) {
    const pageSize = params.pageSize ?? RECORDS_PAGE_SIZE;
    const query = this.conversations
      .createQueryBuilder("c")
      .orderBy("c.createdAt", "DESC")
      .take(pageSize)
      .skip((params.page - 1) * pageSize);
    if (params.rating !== undefined)
      query.andWhere("c.rating = :rating", { rating: params.rating });
    if (params.promptVersion !== undefined) {
      query.andWhere("c.promptVersion = :promptVersion", {
        promptVersion: params.promptVersion,
      });
    }
    if (params.q) {
      query.andWhere(
        `EXISTS (SELECT 1 FROM assistant_messages m
                  WHERE m."conversationId" = c.id AND m.text ILIKE :q)`,
        { q: `%${params.q.replace(/[\\%_]/g, "\\$&")}%` },
      );
    }
    const [items, total] = await query.getManyAndCount();
    const firstQuestions = await this.firstQuestions(items.map((c) => c.id));
    return {
      total,
      page: params.page,
      pageSize,
      items: items.map((c) => ({
        ...c,
        firstQuestion: firstQuestions.get(c.id) ?? null,
      })),
    };
  }

  private async firstQuestions(ids: string[]): Promise<Map<string, string>> {
    if (ids.length === 0) return new Map();
    const rows: Array<{ conversationId: string; text: string }> =
      await this.messages.query(
        `SELECT DISTINCT ON ("conversationId") "conversationId", text
           FROM assistant_messages
          WHERE "conversationId" = ANY($1::uuid[]) AND role = 'user'
          ORDER BY "conversationId", "createdAt"`,
        [ids],
      );
    return new Map(rows.map((row) => [row.conversationId, row.text]));
  }

  async conversation(id: string) {
    const conversation = await this.conversations.findOneBy({ id });
    if (!conversation) return null;
    const [messages, feedback] = await Promise.all([
      this.messages.find({
        where: { conversationId: id },
        order: { createdAt: "ASC" },
      }),
      this.feedback.find({
        where: { conversationId: id },
        order: { createdAt: "ASC" },
      }),
    ]);
    return { ...conversation, messages, feedback };
  }

  async listFeedback(params: {
    status?: FeedbackStatus;
    page: number;
    pageSize?: number;
  }) {
    const pageSize = params.pageSize ?? RECORDS_PAGE_SIZE;
    const [items, total] = await this.feedback.findAndCount({
      where: params.status ? { status: params.status } : {},
      order: { createdAt: "DESC" },
      take: pageSize,
      skip: (params.page - 1) * pageSize,
    });
    return { total, page: params.page, pageSize, items };
  }

  async setFeedbackStatus(id: string, status: FeedbackStatus) {
    const result = await this.feedback.update({ id }, { status });
    return (result.affected ?? 0) > 0;
  }

  /**
   * Chats, cost and ratings of the last 30 days, per UTC day, and open feedback: the live rows of
   * the last week plus the daily sums `pruneConversations` kept of the deleted ones.
   */
  async stats() {
    const [days, ratings, openFeedback] = await Promise.all([
      this.conversations.query(
        `SELECT to_char(day, 'YYYY-MM-DD') AS day,
                sum(chats)::int AS chats,
                sum(messages)::int AS messages,
                sum(tokens)::bigint::text AS tokens,
                sum(cost)::float AS "costUsd"
           FROM (
             SELECT ("createdAt" AT TIME ZONE 'UTC')::date AS day, count(*) AS chats,
                    coalesce(sum("messageCount"), 0) AS messages,
                    coalesce(sum("totalTokens"), 0) AS tokens,
                    coalesce(sum("costUsd"), 0) AS cost
               FROM assistant_conversations
              WHERE "createdAt" > now() - interval '30 days'
              GROUP BY 1
             UNION ALL
             SELECT day, chats, messages, tokens, "costUsd"
               FROM assistant_day_stats
              WHERE day > (now() AT TIME ZONE 'UTC')::date - 30 AND chats > 0
           ) AS both_halves
          GROUP BY 1 ORDER BY 1`,
      ),
      this.messages.query(
        `SELECT sum(up)::int AS up, sum(down)::int AS down
           FROM (
             SELECT count(*) FILTER (WHERE rating = 1) AS up,
                    count(*) FILTER (WHERE rating = -1) AS down
               FROM assistant_messages
              WHERE "createdAt" > now() - interval '30 days'
             UNION ALL
             SELECT coalesce(sum(up), 0), coalesce(sum(down), 0)
               FROM assistant_day_stats
              WHERE day > (now() AT TIME ZONE 'UTC')::date - 30
           ) AS both_halves`,
      ),
      this.feedback.countBy({ status: "new" }),
    ]);
    return { days, ratings: ratings[0], openFeedback };
  }

  /**
   * Deletes one chat with its messages, for the admin. Feedback sent from it stays and only
   * loses its link. False when there is no such chat.
   */
  async deleteConversation(id: string): Promise<boolean> {
    await this.feedback.update(
      { conversationId: id },
      { conversationId: null },
    );
    const result = await this.conversations.delete({ id });
    return (result.affected ?? 0) > 0;
  }

  /**
   * Deletes chats past the retention, ended ones included; their messages go with them (the
   * foreign key cascades) and feedback keeps its link as it is. Each batch first adds what it
   * deletes to `assistant_day_stats`, in the same transaction, so the 30-day statistic neither
   * loses nor double-counts a chat. Batches keep the first run after a shorter retention, weeks
   * of chats at once, from holding one long lock.
   */
  async pruneConversations(): Promise<number> {
    let removed = 0;
    for (;;) {
      const batch = await this.conversations.manager.transaction(
        async (manager) => {
          const rows: Array<{ id: string }> = await manager.query(
            `SELECT id FROM assistant_conversations
              WHERE "createdAt" < now() - make_interval(days => $1)
              ORDER BY "createdAt" LIMIT $2
              FOR UPDATE SKIP LOCKED`,
            [CONVERSATION_RETENTION_DAYS, PRUNE_BATCH],
          );
          if (rows.length === 0) return 0;
          const ids = rows.map((row) => row.id);
          await manager.query(
            `INSERT INTO assistant_day_stats (day, chats, messages, tokens, "costUsd")
             SELECT ("createdAt" AT TIME ZONE 'UTC')::date, count(*),
                    coalesce(sum("messageCount"), 0), coalesce(sum("totalTokens"), 0),
                    coalesce(sum("costUsd"), 0)
               FROM assistant_conversations WHERE id = ANY($1::uuid[])
              GROUP BY 1
             ON CONFLICT (day) DO UPDATE SET
               chats = assistant_day_stats.chats + EXCLUDED.chats,
               messages = assistant_day_stats.messages + EXCLUDED.messages,
               tokens = assistant_day_stats.tokens + EXCLUDED.tokens,
               "costUsd" = assistant_day_stats."costUsd" + EXCLUDED."costUsd"`,
            [ids],
          );
          await manager.query(
            `INSERT INTO assistant_day_stats (day, up, down)
             SELECT ("createdAt" AT TIME ZONE 'UTC')::date,
                    count(*) FILTER (WHERE rating = 1), count(*) FILTER (WHERE rating = -1)
               FROM assistant_messages
              WHERE "conversationId" = ANY($1::uuid[]) AND rating IS NOT NULL
              GROUP BY 1
             ON CONFLICT (day) DO UPDATE SET
               up = assistant_day_stats.up + EXCLUDED.up,
               down = assistant_day_stats.down + EXCLUDED.down`,
            [ids],
          );
          await manager.query(
            `DELETE FROM assistant_conversations WHERE id = ANY($1::uuid[])`,
            [ids],
          );
          return ids.length;
        },
      );
      removed += batch;
      if (batch < PRUNE_BATCH) return removed;
    }
  }
}
