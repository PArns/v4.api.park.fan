import type { Repository } from "typeorm";
import { AssistantRecordsService } from "./assistant-records.service";
import type { AssistantConversation } from "./entities/assistant-conversation.entity";
import type { AssistantMessage } from "./entities/assistant-message.entity";
import type { SiteFeedback } from "./entities/site-feedback.entity";

describe("AssistantRecordsService.deleteConversation", () => {
  const id = "00000000-0000-4000-8000-000000000001";

  function service(affected: number) {
    const calls: string[] = [];
    const conversations = {
      delete: jest.fn(() => {
        calls.push("delete chat");
        return Promise.resolve({ affected });
      }),
    } as unknown as Repository<AssistantConversation>;
    const feedback = {
      update: jest.fn(() => {
        calls.push("unlink feedback");
        return Promise.resolve({ affected: 1 });
      }),
    } as unknown as Repository<SiteFeedback>;
    const records = new AssistantRecordsService(
      conversations,
      {} as Repository<AssistantMessage>,
      feedback,
    );
    return { records, conversations, feedback, calls };
  }

  it("keeps the chat's feedback, unlinked, and deletes the chat", async () => {
    const { records, feedback, conversations, calls } = service(1);
    await expect(records.deleteConversation(id)).resolves.toBe(true);
    expect(feedback.update).toHaveBeenCalledWith(
      { conversationId: id },
      { conversationId: null },
    );
    expect(conversations.delete).toHaveBeenCalledWith({ id });
    expect(calls).toEqual(["unlink feedback", "delete chat"]);
  });

  it("says so when there was no such chat", async () => {
    await expect(service(0).records.deleteConversation(id)).resolves.toBe(
      false,
    );
  });
});
