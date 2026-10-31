import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { AssistantRecordsService } from "../../assistant/assistant-records.service";

/**
 * Keeps Parky's chats to their retention: chats older than
 * `CONVERSATION_RETENTION_DAYS` are deleted with their messages. One indexed
 * DELETE a night; feedback is not touched.
 */
@Processor("assistant")
export class AssistantMaintenanceProcessor {
  private readonly logger = new Logger(AssistantMaintenanceProcessor.name);

  constructor(private readonly records: AssistantRecordsService) {}

  @Process("prune-conversations")
  async handlePrune(_job: Job): Promise<void> {
    try {
      const removed = await this.records.pruneConversations();
      if (removed > 0)
        this.logger.log(`🧹 Pruned ${removed} assistant chat(s)`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.logger.error(`Assistant chat prune failed: ${message}`);
      throw error;
    }
  }
}
