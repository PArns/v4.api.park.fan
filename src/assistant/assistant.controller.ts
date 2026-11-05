import {
  Body,
  Controller,
  Get,
  Header,
  HttpCode,
  HttpException,
  HttpStatus,
  Post,
  Req,
  Res,
} from "@nestjs/common";
import { ApiOperation, ApiResponse, ApiTags } from "@nestjs/swagger";
import type { Request, Response } from "express";
import { z } from "zod";
import { getRateLimitIp } from "../common/utils/request.util";
import {
  hashRateLimitKey,
  type RateLimitVerdict,
} from "../common/redis/rate-limit.util";
import { RATE_LIMITED_RESPONSE } from "../common/filters/rate-limited-response";
import { AssistantChallengeService } from "./assistant-challenge.service";
import { SESSION_TTL_SECONDS } from "./assistant-session.util";
import {
  AssistantSettingsService,
  type AssistantPublicConfig,
} from "./assistant-settings.service";
import { AssistantChatService, type ChatEvent } from "./assistant-chat.service";
import { AssistantRecordsService } from "./assistant-records.service";
import { AssistantRateLimitService } from "./assistant-rate-limit.service";
import { junkReason } from "./assistant-junk";
import {
  chatRequestSchema,
  feedbackRequestSchema,
  ratingRequestSchema,
  sessionRequestSchema,
} from "./assistant.schemas";

/** Parses a body with zod; a refusal names the fields, never echoes the input. */
function parse<T>(schema: z.ZodType<T>, body: unknown): T {
  const result = schema.safeParse(body);
  if (!result.success) {
    throw new HttpException(
      { statusCode: 400, message: z.prettifyError(result.error) },
      HttpStatus.BAD_REQUEST,
    );
  }
  return result.data;
}

/**
 * Parky, park.fan's chat assistant, and the feedback form beside it.
 *
 * The browser calls the writes here directly (CORS, see
 * docs/deployment/cors.md); none of them is cacheable. The config is read by
 * park.fan's server and may be cached for a minute.
 *
 * Every write needs a session token from `POST session`, which takes a solved
 * Turnstile challenge: a script cannot spend Anthropic tokens or fill the
 * feedback table by posting to these URLs.
 */
@ApiTags("assistant")
@Controller("assistant")
export class AssistantController {
  constructor(
    private readonly settings: AssistantSettingsService,
    private readonly chat: AssistantChatService,
    private readonly records: AssistantRecordsService,
    private readonly rateLimit: AssistantRateLimitService,
    private readonly challenge: AssistantChallengeService,
  ) {}

  @Get("config")
  @Header("Cache-Control", "public, max-age=60, s-maxage=60")
  @ApiOperation({
    summary: "Whether the assistant is on, and for what share of visitors",
  })
  config(): Promise<AssistantPublicConfig> {
    return this.settings.publicConfig();
  }

  @Post("session")
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: "Trade a solved Turnstile challenge for a session token",
    description:
      "The token is valid for two hours and counts the limits per session; " +
      "chat, rating and feedback refuse a request without it (401).",
  })
  @ApiResponse(RATE_LIMITED_RESPONSE)
  async session(
    @Body() body: unknown,
    @Req() request: Request,
  ): Promise<{ sessionToken: string; expiresInSeconds: number }> {
    const { turnstileToken } = parse(sessionRequestSchema, body);
    refuseOverLimit(
      await this.rateLimit.check({ ip: getRateLimitIp(request) }, "session"),
    );
    const opened = await this.challenge.open(
      turnstileToken,
      getRateLimitIp(request),
    );
    if (opened === "off") {
      throw new HttpException("off", HttpStatus.SERVICE_UNAVAILABLE);
    }
    if (opened === "failed") {
      throw new HttpException("Challenge failed", HttpStatus.FORBIDDEN);
    }
    return {
      sessionToken: opened.token,
      expiresInSeconds: SESSION_TTL_SECONDS,
    };
  }

  @Post("chat")
  @ApiOperation({
    summary: "Ask the assistant (server-sent events)",
    description:
      "Streams `start`, `status` (a tool is running), `delta` (text) and `done` or `error` " +
      "events. The server keeps the history; send the conversationId from `start` with the " +
      "next question.",
  })
  @ApiResponse(RATE_LIMITED_RESPONSE)
  @ApiResponse({
    status: 422,
    description:
      "`unclear`: junk (no words, one key held down, a run of letters without a vowel, or the " +
      "question just answered asked again). The third in a row pauses the session.",
  })
  @ApiResponse({ status: 503, description: "The assistant is switched off." })
  async ask(
    @Body() body: unknown,
    @Req() request: Request,
    @Res() response: Response,
  ): Promise<void> {
    const input = parse(chatRequestSchema, body);
    const ip = getRateLimitIp(request);
    const session = this.challenge.session(input.sessionToken);
    if (!session) {
      response
        .status(HttpStatus.UNAUTHORIZED)
        .setHeader("Cache-Control", "no-store")
        .json({ statusCode: 401, message: "challenge" });
      return;
    }
    const pause = await this.rateLimit.cooldown(session);
    if (pause > 0) {
      tooManyQuestions(response, pause);
      return;
    }
    const verdict = await this.rateLimit.check(
      { session },
      "chatShort",
      "chatBurst",
      "chatDay",
    );
    if (!verdict.allowed) {
      tooManyQuestions(response, verdict.retryAfterSeconds);
      return;
    }
    if (junkReason(input.message)) {
      await this.refuseJunk(response, session);
      return;
    }

    const prepared = await this.chat.prepare(input, ip, session);
    if (prepared === "junk") {
      await this.refuseJunk(response, session);
      return;
    }
    await this.rateLimit.clearStrikes(session);
    if (typeof prepared === "string") {
      const status = {
        off: HttpStatus.SERVICE_UNAVAILABLE,
        "not-found": HttpStatus.NOT_FOUND,
        limit: HttpStatus.TOO_MANY_REQUESTS,
      }[prepared];
      response
        .status(status)
        .setHeader("Cache-Control", "no-store")
        .json({ statusCode: status, message: prepared });
      return;
    }

    response.status(HttpStatus.OK);
    response.setHeader("Content-Type", "text/event-stream; charset=utf-8");
    response.setHeader("Cache-Control", "no-store, no-transform");
    response.setHeader("X-Accel-Buffering", "no");
    response.flushHeaders();

    const abort = new AbortController();
    response.on("close", () => abort.abort());
    // `compression` buffers until it has enough bytes; flush per event so
    // each piece of text reaches the browser as it is written.
    const flushable = response as Response & { flush?: () => void };
    const emit = (event: ChatEvent) => {
      if (response.writableEnded) return;
      response.write(
        `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`,
      );
      flushable.flush?.();
    };
    await this.chat.answer(prepared, emit, abort.signal, ip);
    if (!response.writableEnded) response.end();
  }

  @Post("rating")
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Thumbs up or down for one answer" })
  @ApiResponse(RATE_LIMITED_RESPONSE)
  async rate(@Body() body: unknown): Promise<void> {
    const input = parse(ratingRequestSchema, body);
    const session = this.requireSession(input.sessionToken);
    refuseOverLimit(await this.rateLimit.check({ session }, "rating"));
    const rated = await this.records.rate(
      input.conversationId,
      input.messageId,
      input.rating,
      hashRateLimitKey(session),
    );
    if (!rated) throw new HttpException("No such answer", HttpStatus.NOT_FOUND);
  }

  @Post("feedback")
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({ summary: "Send feedback about the site" })
  @ApiResponse(RATE_LIMITED_RESPONSE)
  async feedback(
    @Body() body: unknown,
    @Req() request: Request,
  ): Promise<{ id: string }> {
    const input = parse(feedbackRequestSchema, body);
    const session = this.requireSession(input.sessionToken);
    refuseOverLimit(
      await this.rateLimit.feedback(session, getRateLimitIp(request)),
    );
    const id = await this.records.addFeedback({
      message: input.message,
      email: input.email || null,
      locale: input.locale,
      pagePath: input.pagePath,
      conversationId: input.conversationId ?? null,
      sessionHash: hashRateLimitKey(session),
      source: "form",
    });
    return { id };
  }

  /**
   * Junk gets no answer and costs no tokens: a 422 the widget words as "ask me about parks",
   * and the third in a row a pause, refused as too many questions.
   */
  private async refuseJunk(response: Response, session: string): Promise<void> {
    const pause = await this.rateLimit.strike(session);
    if (pause > 0) {
      tooManyQuestions(response, pause);
      return;
    }
    response
      .status(HttpStatus.UNPROCESSABLE_ENTITY)
      .setHeader("Cache-Control", "no-store")
      .json({ statusCode: 422, message: "unclear" });
  }

  private requireSession(token: string): string {
    const session = this.challenge.session(token);
    if (!session) throw new HttpException("challenge", HttpStatus.UNAUTHORIZED);
    return session;
  }
}

function refuseOverLimit(verdict: RateLimitVerdict): void {
  if (verdict.allowed) return;
  throw new HttpException(
    {
      statusCode: 429,
      message: "Too many requests",
      retryAfter: verdict.retryAfterSeconds,
    },
    HttpStatus.TOO_MANY_REQUESTS,
  );
}

function tooManyQuestions(response: Response, retryAfterSeconds: number): void {
  response
    .status(HttpStatus.TOO_MANY_REQUESTS)
    .setHeader("Retry-After", String(retryAfterSeconds))
    .setHeader("Cache-Control", "no-store")
    .json({ statusCode: 429, message: "Too many questions" });
}
