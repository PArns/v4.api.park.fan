import { Injectable } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { AssistantConversation } from "./entities/assistant-conversation.entity";
import {
  ASSISTANT_TOOL_ERROR_KINDS,
  AssistantMessage,
} from "./entities/assistant-message.entity";
import {
  SiteFeedback,
  type FeedbackKind,
  type FeedbackSource,
  type FeedbackStatus,
} from "./entities/site-feedback.entity";
import { likeContains } from "../common/utils/like-pattern.util";

/** How long chats are kept; feedback is kept until it is deleted by hand. */
export const CONVERSATION_RETENTION_DAYS = 7;

/** Chats one prune transaction deletes. */
const PRUNE_BATCH = 2000;

/** Rows per page of the admin's lists; the MCP tools ask for fewer. */
export const RECORDS_PAGE_SIZE = 50;

/** Feedback Parky passes on from one chat at most: a second, different issue still goes in. */
export const CHAT_FEEDBACK_PER_CONVERSATION = 2;

/** What a tool report reads: a window of UTC days, narrowed or not. */
export interface ToolStatsQuery {
  /** First and last UTC day, both included. */
  from: string;
  to: string;
  /** Parky's tool names; any other name a call carries counts as `(unknown)`. */
  tools: readonly string[];
  /** The trip regions an input's `region` may name; any other is `''`. */
  regions: readonly string[];
  promptVersion?: number;
  tool?: string;
}

/** One cell of the tool report: calls of a tool by outcome, region and prompt version. */
export interface ToolStatRow {
  tool: string;
  outcome: string;
  region: string;
  promptVersion: number;
  calls: number;
}

/** What the kept chats' tool inputs say, beside the counts. */
export interface ToolInputStats {
  /** The first UTC day of the window whose chats are still kept. */
  keptFrom: string;
  answers: number;
  answersWithTools: number;
  calls: number;
  issues: Array<{ tool: string; path: string; count: number }>;
  ignoredKeys: Array<{ tool: string; key: string; count: number }>;
  changeTripKeys: Array<{ key: string; count: number }>;
  interviewOps: Array<{ op: string; outcome: string; count: number }>;
}

/** A stored call's outcome: `ok`, its error kind, or `error` for a failure stored before the kinds. */
const TOOL_OUTCOME_SQL = `CASE WHEN c->>'error' = 'true'
  THEN CASE WHEN c->>'errorKind' IN (${ASSISTANT_TOOL_ERROR_KINDS.map((k) => `'${k}'`).join(", ")})
            THEN c->>'errorKind' ELSE 'error' END
  ELSE 'ok' END`;

/** The interview tool's ops and the outcomes of its `end` (`interview` in assistant-tools.ts). */
const INTERVIEW_OPS = new Set(["start", "answered", "skip", "ask", "end"]);
const INTERVIEW_OUTCOMES = new Set(["done", "left"]);

/** The calls of the window's answers, one row per call: what both halves of the tool report read. */
const LIVE_CALLS = `
  FROM assistant_messages m
  CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(m."toolCalls") = 'array' THEN m."toolCalls" ELSE '[]'::jsonb END
  ) AS c
 WHERE m.role = 'assistant'`;

/**
 * The stored side of Parky: ratings and feedback from visitors, the lists the
 * admin reads, and the retention of chats.
 */
@Injectable()
export class AssistantRecordsService {
  /** Parky's tool names and the trip regions, for the prune's tool sums (`AssistantModule` sets them). */
  private toolNames: readonly string[] = [];
  private regionSlugs: readonly string[] = [];

  /** The names a tool sum keeps; a name outside them is `(unknown)`, a region `''`. */
  setToolVocabulary(
    tools: readonly string[],
    regions: readonly string[],
  ): void {
    this.toolNames = tools;
    this.regionSlugs = regions;
  }

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
        { q: likeContains(params.q) },
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
   * deletes to `assistant_day_stats`, and its tool calls to `assistant_tool_day_stats`, in the
   * same transaction, so neither the 30-day statistic nor the tool report loses or
   * double-counts a chat. Batches keep the first run after a shorter retention, weeks
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
            `INSERT INTO assistant_tool_day_stats
                    (day, prompt_version, tool, outcome, region, calls)
             SELECT (m."createdAt" AT TIME ZONE 'UTC')::date,
                    coalesce(m."promptVersion", 0),
                    CASE WHEN c->>'name' = ANY($2::text[]) THEN c->>'name' ELSE '(unknown)' END,
                    ${TOOL_OUTCOME_SQL},
                    CASE WHEN c->'input'->>'region' = ANY($3::text[])
                         THEN c->'input'->>'region' ELSE '' END,
                    count(*)
               ${LIVE_CALLS} AND m."conversationId" = ANY($1::uuid[])
              GROUP BY 1, 2, 3, 4, 5
             ON CONFLICT (day, prompt_version, tool, outcome, region) DO UPDATE SET
               calls = assistant_tool_day_stats.calls + EXCLUDED.calls`,
            [ids, this.toolNames, this.regionSlugs],
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

  /**
   * Parky's tool calls over a window of UTC days, per tool, outcome (`ok`, an
   * error kind, `error` for a failure stored before the kinds), region and
   * prompt version: the kept chats' calls plus the sums the prune kept of the
   * deleted ones.
   */
  async toolStats(query: ToolStatsQuery): Promise<ToolStatRow[]> {
    const params: unknown[] = [
      `${query.from}T00:00:00.000Z`,
      `${query.to}T00:00:00.000Z`,
      query.tools,
      query.regions,
    ];
    const narrow: string[] = [];
    if (query.promptVersion !== undefined) {
      params.push(query.promptVersion);
      narrow.push(`prompt_version = $${params.length}`);
    }
    if (query.tool !== undefined) {
      params.push(query.tool);
      narrow.push(`tool = $${params.length}`);
    }
    const rows: Array<{
      tool: string;
      outcome: string;
      region: string;
      prompt_version: number;
      calls: number;
    }> = await this.messages.query(
      `SELECT tool, outcome, region, prompt_version, sum(calls)::int AS calls
         FROM (
           SELECT coalesce(m."promptVersion", 0) AS prompt_version,
                  CASE WHEN c->>'name' = ANY($3::text[]) THEN c->>'name' ELSE '(unknown)' END AS tool,
                  ${TOOL_OUTCOME_SQL} AS outcome,
                  CASE WHEN c->'input'->>'region' = ANY($4::text[])
                       THEN c->'input'->>'region' ELSE '' END AS region,
                  1 AS calls
             ${LIVE_CALLS}
              AND m."createdAt" >= $1::timestamptz
              AND m."createdAt" < $2::timestamptz + interval '1 day'
           UNION ALL
           SELECT prompt_version, tool, outcome, region, calls
             FROM assistant_tool_day_stats
            WHERE day >= ($1::timestamptz AT TIME ZONE 'UTC')::date
              AND day <= ($2::timestamptz AT TIME ZONE 'UTC')::date
         ) AS both_halves
        ${narrow.length > 0 ? `WHERE ${narrow.join(" AND ")}` : ""}
        GROUP BY tool, outcome, region, prompt_version
        ORDER BY sum(calls) DESC, tool, outcome, region, prompt_version`,
      params,
    );
    return rows.map((row) => ({
      tool: row.tool,
      outcome: row.outcome,
      region: row.region,
      promptVersion: row.prompt_version,
      calls: row.calls,
    }));
  }

  /**
   * What only the kept chats can say, over the window's days they cover:
   * answers and those with tool calls, the paths a schema refused, the keys
   * it dropped, which keys `change_trip` was given, and the interview's ops.
   * Keys and paths only, never a value.
   */
  async toolInputStats(query: ToolStatsQuery): Promise<ToolInputStats> {
    const params: unknown[] = [
      `${query.from}T00:00:00.000Z`,
      `${query.to}T00:00:00.000Z`,
    ];
    let where = `m."createdAt" >= $1::timestamptz
                 AND m."createdAt" < $2::timestamptz + interval '1 day'`;
    if (query.promptVersion !== undefined) {
      params.push(query.promptVersion);
      where += ` AND coalesce(m."promptVersion", 0) = $${params.length}`;
    }
    // The answers and the two narrowed reads below take the window alone.
    const windowParams = [...params];
    let toolWhere = "";
    if (query.tool !== undefined) {
      params.push(query.tool);
      toolWhere = ` AND c->>'name' = $${params.length}`;
    }
    /** A list of strings each call of the window carries under `field`, counted per tool. */
    const listed = (field: "issues" | "ignoredKeys") =>
      this.messages.query(
        `SELECT tool, value, count(*)::int AS count FROM (
           SELECT c->>'name' AS tool, c->'${field}' AS list
             ${LIVE_CALLS} AND ${where}${toolWhere}
              AND jsonb_typeof(c->'${field}') = 'array') s
           CROSS JOIN LATERAL jsonb_array_elements_text(s.list) AS value
          GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2`,
        params,
      ) as Promise<Array<{ tool: string; value: string; count: number }>>;
    const [answers, issues, ignored, changeKeys, ops, oldest] =
      await Promise.all([
        this.messages.query(
          `SELECT count(*)::int AS answers,
                count(*) FILTER (WHERE jsonb_typeof(m."toolCalls") = 'array'
                                   AND jsonb_array_length(m."toolCalls") > 0)::int AS with_tools,
                coalesce(sum(CASE WHEN jsonb_typeof(m."toolCalls") = 'array'
                                  THEN jsonb_array_length(m."toolCalls") ELSE 0 END), 0)::int AS calls
           FROM assistant_messages m
          WHERE m.role = 'assistant' AND ${where}`,
          windowParams,
        ),
        listed("issues"),
        listed("ignoredKeys"),
        this.messages.query(
          `SELECT key, count(*)::int AS count FROM (
           SELECT c->'input' AS input
             ${LIVE_CALLS} AND ${where}
              AND c->>'name' = 'change_trip'
              AND jsonb_typeof(c->'input') = 'object') s
           CROSS JOIN LATERAL jsonb_object_keys(s.input) AS key
          GROUP BY 1 ORDER BY 2 DESC, 1`,
          windowParams,
        ),
        this.messages.query(
          `SELECT c->'input'->>'op' AS op, coalesce(c->'input'->>'outcome', '') AS outcome,
                count(*)::int AS count
           ${LIVE_CALLS} AND ${where}
            AND c->>'name' = 'interview'
          GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2`,
          windowParams,
        ),
        this.conversations.query(
          `SELECT to_char(min("createdAt") AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS first
           FROM assistant_conversations`,
        ),
      ]);
    const kept = oldest[0]?.first as string | null | undefined;
    const keyed = (value: unknown) =>
      typeof value === "string" && /^[A-Za-z0-9_.]{1,60}$/.test(value);
    return {
      keptFrom: kept && kept > query.from ? kept : query.from,
      answers: answers[0]?.answers ?? 0,
      answersWithTools: answers[0]?.with_tools ?? 0,
      calls: answers[0]?.calls ?? 0,
      issues: issues
        .filter((row) => query.tools.includes(row.tool) && keyed(row.value))
        .map((row) => ({ tool: row.tool, path: row.value, count: row.count })),
      ignoredKeys: ignored
        .filter((row) => query.tools.includes(row.tool) && keyed(row.value))
        .map((row) => ({ tool: row.tool, key: row.value, count: row.count })),
      changeTripKeys: (
        changeKeys as Array<{ key: string; count: number }>
      ).filter((row) => keyed(row.key)),
      interviewOps: (
        ops as Array<{ op: string | null; outcome: string; count: number }>
      )
        .filter((row) => INTERVIEW_OPS.has(row.op ?? ""))
        .map((row) => ({
          op: row.op as string,
          outcome: INTERVIEW_OUTCOMES.has(row.outcome) ? row.outcome : "",
          count: row.count,
        })),
    };
  }
}
