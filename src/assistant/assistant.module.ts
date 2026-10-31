import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RedisModule } from "../common/redis/redis.module";
import { AssistantController } from "./assistant.controller";
import { AdminAssistantController } from "./admin-assistant.controller";
import { AssistantSettingsService } from "./assistant-settings.service";
import { AssistantChatService } from "./assistant-chat.service";
import { AssistantRecordsService } from "./assistant-records.service";
import { AssistantRateLimitService } from "./assistant-rate-limit.service";
import { AssistantChallengeService } from "./assistant-challenge.service";
import { AssistantPostsClient } from "./tools/assistant-posts.client";
import { AssistantSettings } from "./entities/assistant-settings.entity";
import { AssistantConversation } from "./entities/assistant-conversation.entity";
import { AssistantMessage } from "./entities/assistant-message.entity";
import { SiteFeedback } from "./entities/site-feedback.entity";

/**
 * Parky, the chat assistant on park.fan, with its settings, chats and the
 * site feedback (docs/frontend/assistant.md). Its tools reach the park
 * services through `ModuleRef`, as the MCP server's do, so the park modules
 * are not imported here.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([
      AssistantSettings,
      AssistantConversation,
      AssistantMessage,
      SiteFeedback,
    ]),
    RedisModule,
  ],
  controllers: [AssistantController, AdminAssistantController],
  providers: [
    AssistantSettingsService,
    AssistantChatService,
    AssistantRecordsService,
    AssistantRateLimitService,
    AssistantChallengeService,
    AssistantPostsClient,
  ],
  exports: [AssistantRecordsService],
})
export class AssistantModule {}
