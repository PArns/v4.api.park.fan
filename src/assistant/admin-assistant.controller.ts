import {
  Body,
  Delete,
  Get,
  HttpCode,
  HttpException,
  HttpStatus,
  NotFoundException,
  Param,
  ParseUUIDPipe,
  Patch,
  Post,
  Put,
  Query,
} from "@nestjs/common";
import { ApiOperation } from "@nestjs/swagger";
import Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";
import { SelfAudited } from "../admin/auth/admin-audit.interceptor";
import {
  AdminMinRole,
  CurrentAdmin,
} from "../admin/auth/admin-auth.decorators";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import {
  AssistantSettingsService,
  type AssistantSettingsView,
} from "./assistant-settings.service";
import { AssistantRecordsService } from "./assistant-records.service";
import { ASSISTANT_MODEL } from "./assistant-chat.service";
import { SYSTEM_PROMPT_VERSION } from "./system-prompt";
import {
  feedbackStatusSchema,
  settingsUpdateSchema,
} from "./assistant.schemas";
import { FEEDBACK_STATUSES } from "./entities/site-feedback.entity";
import { AssistantKnowledgeService } from "./assistant-knowledge.service";
import { knowledgeTopicSchema, knowledgeWriteSchema } from "./knowledge";
import { AdminApi } from "../admin/auth/admin-api.decorator";
import { addIsoDays } from "../common/utils/date.util";
import { utcDay } from "../common/utils/reader-hash.util";

const TOKEN_STATS_MAX_DAYS = 90;

const listQuery = z.object({
  page: z.coerce.number().int().min(1).max(1000).default(1),
  rating: z.coerce.number().int().min(-1).max(1).optional(),
  q: z.string().trim().min(2).max(100).optional(),
  promptVersion: z.coerce.number().int().min(1).max(32767).optional(),
  status: z.enum(FEEDBACK_STATUSES).optional(),
  source: z.enum(["web", "app"]).optional(),
  /** `true`: only chats with a reported answer; `false`: only those without. */
  reported: z
    .enum(["true", "false"])
    .transform((value) => value === "true")
    .optional(),
});

/** The token statistic's window: UTC days back from today, today included. */
const tokenStatsQuery = z.object({
  days: z.coerce.number().int().min(1).max(TOKEN_STATS_MAX_DAYS).default(30),
});

function parse<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value);
  if (!result.success) {
    throw new HttpException(
      z.prettifyError(result.error),
      HttpStatus.BAD_REQUEST,
    );
  }
  return result.data;
}

/**
 * Parky in the admin: the settings with the Claude key, the stored chats, the
 * feedback visitors sent and his knowledge base. Changing the settings is for
 * owners; the key is never sent back, only its masked form, and the audit log
 * redacts `apiKey`.
 */
@AdminApi("admin/assistant")
export class AdminAssistantController {
  constructor(
    private readonly settings: AssistantSettingsService,
    private readonly records: AssistantRecordsService,
    private readonly knowledge: AssistantKnowledgeService,
  ) {}

  @Get("settings")
  @ApiOperation({ summary: "Assistant settings, with the key masked" })
  async getSettings(): Promise<
    AssistantSettingsView & { model: string; promptVersion: number }
  > {
    return {
      ...(await this.settings.view()),
      model: ASSISTANT_MODEL,
      promptVersion: SYSTEM_PROMPT_VERSION,
    };
  }

  @Put("settings")
  @AdminMinRole("owner")
  @ApiOperation({ summary: "Change the assistant settings or its key" })
  async putSettings(
    @Body() body: unknown,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<AssistantSettingsView> {
    const change = parse(settingsUpdateSchema, body);
    if (change.apiKey && !(await this.settings.view()).secretConfigured) {
      throw new HttpException(
        "ASSISTANT_SECRET_KEY is not set on the API; no key can be stored",
        HttpStatus.CONFLICT,
      );
    }
    return this.settings.update(change, admin.email);
  }

  @Post("settings/test")
  @HttpCode(HttpStatus.OK)
  @AdminMinRole("owner")
  @ApiOperation({ summary: "Check the stored key with a one-token request" })
  async testKey(): Promise<{ ok: boolean; error?: string }> {
    const apiKey = await this.settings.apiKey();
    if (!apiKey) return { ok: false, error: "No key stored" };
    try {
      await new Anthropic({ apiKey, maxRetries: 0 }).messages.create({
        model: ASSISTANT_MODEL,
        max_tokens: 1,
        messages: [{ role: "user", content: "Hi" }],
      });
      return { ok: true };
    } catch (error) {
      return {
        ok: false,
        error:
          error instanceof Anthropic.APIError
            ? `${error.status ?? ""} ${error.name}`.trim()
            : "Anthropic not reachable",
      };
    }
  }

  @Get("stats")
  @ApiOperation({ summary: "Chats, tokens, cost and ratings of 30 days" })
  async stats() {
    return {
      ...(await this.records.stats()),
      tokensToday: await this.settings.tokensToday(),
    };
  }

  @Get("stats/tokens")
  @ApiOperation({
    summary:
      "Tokens and cost per UTC day and source (web, app), and who spent most",
  })
  tokenStats(@Query() query: unknown) {
    const { days } = parse(tokenStatsQuery, query);
    const to = utcDay(new Date());
    return this.records.tokenStats({ from: addIsoDays(to, -(days - 1)), to });
  }

  @Get("conversations")
  @ApiOperation({
    summary: "Stored chats, newest first",
    description:
      "`reported=true` keeps the chats with an answer a visitor reported (`reportedAt` on the " +
      "row); the reason and note are on the answer in the chat's detail.",
  })
  conversations(@Query() query: unknown) {
    const { page, rating, q, promptVersion, source, reported } = parse(
      listQuery,
      query,
    );
    return this.records.listConversations({
      page,
      rating,
      q,
      promptVersion,
      source,
      reported,
    });
  }

  @Get("conversations/:id")
  @ApiOperation({
    summary: "One chat with its messages, tool calls and feedback",
  })
  async conversation(@Param("id", new ParseUUIDPipe()) id: string) {
    const found = await this.records.conversation(id);
    if (!found) throw new NotFoundException();
    return found;
  }

  @Delete("conversations/:id")
  @HttpCode(HttpStatus.NO_CONTENT)
  @AdminMinRole("editor")
  @ApiOperation({
    summary: "Delete one chat with its messages; its feedback stays, unlinked",
  })
  async deleteConversation(
    @Param("id", new ParseUUIDPipe()) id: string,
  ): Promise<void> {
    if (!(await this.records.deleteConversation(id))) {
      throw new NotFoundException();
    }
  }

  @Get("feedback")
  @ApiOperation({ summary: "Feedback from visitors, newest first" })
  feedback(@Query() query: unknown) {
    const { page, status } = parse(listQuery, query);
    return this.records.listFeedback({ page, status });
  }

  @Patch("feedback/:id")
  @AdminMinRole("editor")
  @ApiOperation({ summary: "Mark feedback as done or archived" })
  async setFeedbackStatus(
    @Param("id", new ParseUUIDPipe()) id: string,
    @Body() body: unknown,
  ): Promise<{ ok: true }> {
    const { status } = parse(feedbackStatusSchema, body);
    if (!(await this.records.setFeedbackStatus(id, status))) {
      throw new NotFoundException();
    }
    return { ok: true };
  }

  @Get("knowledge")
  @ApiOperation({
    summary: "Parky's knowledge base: every topic, switched-off ones too",
  })
  knowledgeTopics() {
    return this.knowledge.list();
  }

  @Get("knowledge/:topic")
  @ApiOperation({
    summary: "One topic of Parky's knowledge base, with its edits",
  })
  async knowledgeTopic(@Param("topic") topic: string) {
    const name = parse(knowledgeTopicSchema, topic);
    const found = await this.knowledge.get(name);
    if (!found) throw new NotFoundException();
    return { ...found, history: await this.knowledge.history(name) };
  }

  @Put("knowledge/:topic")
  @AdminMinRole("editor")
  @SelfAudited()
  @ApiOperation({
    summary: "Create a topic of Parky's knowledge base or replace its fields",
  })
  saveKnowledgeTopic(
    @Param("topic") topic: string,
    @Body() body: unknown,
    @CurrentAdmin() admin: AdminPrincipal,
  ) {
    return this.knowledge.save(
      parse(knowledgeTopicSchema, topic),
      parse(knowledgeWriteSchema, body),
      admin,
    );
  }

  @Delete("knowledge/:topic")
  @HttpCode(HttpStatus.NO_CONTENT)
  @AdminMinRole("editor")
  @SelfAudited()
  @ApiOperation({
    summary: "Delete a topic; the audit log keeps all of it",
  })
  async deleteKnowledgeTopic(
    @Param("topic") topic: string,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<void> {
    const removed = await this.knowledge.remove(
      parse(knowledgeTopicSchema, topic),
      admin,
    );
    if (!removed) throw new NotFoundException();
  }
}
