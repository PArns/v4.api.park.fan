import {
  Body,
  Controller,
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
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import { ApiOperation, ApiSecurity, ApiTags } from "@nestjs/swagger";
import Anthropic from "@anthropic-ai/sdk";
import { z } from "zod";
import { AdminAuthGuard } from "../admin/auth/admin-auth.guard";
import { AdminAuditInterceptor } from "../admin/auth/admin-audit.interceptor";
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

const listQuery = z.object({
  page: z.coerce.number().int().min(1).max(1000).default(1),
  rating: z.coerce.number().int().min(-1).max(1).optional(),
  q: z.string().trim().min(2).max(100).optional(),
  status: z.enum(FEEDBACK_STATUSES).optional(),
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
 * Parky in the admin: the settings with the Claude key, the stored chats and
 * the feedback visitors sent. Changing the settings is for owners; the key is
 * never sent back, only its masked form, and the audit log redacts `apiKey`.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/assistant")
@UseGuards(AdminAuthGuard)
@UseInterceptors(AdminAuditInterceptor)
export class AdminAssistantController {
  constructor(
    private readonly settings: AssistantSettingsService,
    private readonly records: AssistantRecordsService,
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

  @Get("conversations")
  @ApiOperation({ summary: "Stored chats, newest first" })
  conversations(@Query() query: unknown) {
    const { page, rating, q } = parse(listQuery, query);
    return this.records.listConversations({ page, rating, q });
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
}
