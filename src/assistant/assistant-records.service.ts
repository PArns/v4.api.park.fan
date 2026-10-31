import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { AssistantConversation } from "./entities/assistant-conversation.entity";
import { AssistantMessage } from "./entities/assistant-message.entity";
import {
  SiteFeedback,
  type FeedbackStatus,
} from "./entities/site-feedback.entity";

/** How long chats are kept; feedback is kept until it is deleted by hand. */
export const CONVERSATION_RETENTION_DAYS = 90;

const PAGE_SIZE = 50;

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

  /** False when the answer is not Parky's or not in that chat. */
  async rate(conversationId: string, messageId: string, rating: 1 | -1) {
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
  }): Promise<string> {
    // A chat id that does not exist is dropped rather than refused: the
    // feedback is worth keeping either way.
    const conversationId =
      entry.conversationId &&
      (await this.conversations.existsBy({ id: entry.conversationId }))
        ? entry.conversationId
        : null;
    const saved = await this.feedback.save(
      this.feedback.create({ ...entry, conversationId }),
    );
    return saved.id;
  }

  async listConversations(params: {
    rating?: number;
    q?: string;
    page: number;
  }) {
    const query = this.conversations
      .createQueryBuilder("c")
      .orderBy("c.createdAt", "DESC")
      .take(PAGE_SIZE)
      .skip((params.page - 1) * PAGE_SIZE);
    if (params.rating !== undefined)
      query.andWhere("c.rating = :rating", { rating: params.rating });
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
      pageSize: PAGE_SIZE,
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

  async listFeedback(params: { status?: FeedbackStatus; page: number }) {
    const [items, total] = await this.feedback.findAndCount({
      where: params.status ? { status: params.status } : {},
      order: { createdAt: "DESC" },
      take: PAGE_SIZE,
      skip: (params.page - 1) * PAGE_SIZE,
    });
    return { total, page: params.page, pageSize: PAGE_SIZE, items };
  }

  async setFeedbackStatus(id: string, status: FeedbackStatus) {
    const result = await this.feedback.update({ id }, { status });
    return (result.affected ?? 0) > 0;
  }

  /** Chats, cost and ratings of the last 30 days, per UTC day, and open feedback. */
  async stats() {
    const [days, ratings, openFeedback] = await Promise.all([
      this.conversations.query(
        `SELECT to_char(date_trunc('day', "createdAt" AT TIME ZONE 'UTC'), 'YYYY-MM-DD') AS day,
                count(*)::int AS chats,
                coalesce(sum("messageCount"), 0)::int AS messages,
                coalesce(sum("totalTokens"), 0)::bigint::text AS tokens,
                coalesce(sum("costUsd"), 0)::float AS "costUsd"
           FROM assistant_conversations
          WHERE "createdAt" > now() - interval '30 days'
          GROUP BY 1 ORDER BY 1`,
      ),
      this.messages.query(
        `SELECT count(*) FILTER (WHERE rating = 1)::int AS up,
                count(*) FILTER (WHERE rating = -1)::int AS down
           FROM assistant_messages
          WHERE "createdAt" > now() - interval '30 days'`,
      ),
      this.feedback.countBy({ status: "new" }),
    ]);
    return { days, ratings: ratings[0], openFeedback };
  }

  /** Deletes chats past the retention; their messages go with them. */
  async pruneConversations(): Promise<number> {
    const result = await this.conversations
      .createQueryBuilder()
      .delete()
      .where(`"createdAt" < now() - make_interval(days => :days)`, {
        days: CONVERSATION_RETENTION_DAYS,
      })
      .execute();
    return result.affected ?? 0;
  }
}
