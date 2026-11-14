import { Module, OnModuleInit } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RedisModule } from "../common/redis/redis.module";
import { AssistantController } from "./assistant.controller";
import { AdminAssistantController } from "./admin-assistant.controller";
import { AssistantSettingsService } from "./assistant-settings.service";
import { AssistantChatService } from "./assistant-chat.service";
import { AssistantPageService } from "./assistant-page.service";
import { AssistantRecordsService } from "./assistant-records.service";
import { AssistantRateLimitService } from "./assistant-rate-limit.service";
import { AssistantChallengeService } from "./assistant-challenge.service";
import { AssistantPostsClient } from "./tools/assistant-posts.client";
import { AssistantSettings } from "./entities/assistant-settings.entity";
import { AssistantConversation } from "./entities/assistant-conversation.entity";
import { AssistantMessage } from "./entities/assistant-message.entity";
import { SiteFeedback } from "./entities/site-feedback.entity";
import { ASSISTANT_TOOLS } from "./tools/assistant-tools";
import { TRIP_REGIONS } from "../trip-regions/regions";

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
    AssistantPageService,
    AssistantRecordsService,
    AssistantRateLimitService,
    AssistantChallengeService,
    AssistantPostsClient,
  ],
  exports: [AssistantRecordsService, AssistantSettingsService],
})
export class AssistantModule implements OnModuleInit {
  constructor(private readonly records: AssistantRecordsService) {}

  /** The names the tool sums keep: the tools import the records, so the records cannot import them. */
  onModuleInit(): void {
    this.records.setToolVocabulary(
      ASSISTANT_TOOLS.map((tool) => tool.name),
      Object.keys(TRIP_REGIONS),
    );
  }
}
