import { z } from "zod";
import { AssistantKnowledgeService } from "../../assistant/assistant-knowledge.service";
import {
  assistantKnowledgeTool,
  deleteAssistantKnowledgeTool,
  writeAssistantKnowledgeTool,
} from "./assistant-knowledge.tools";
import { adminContext, callAs } from "./__fixtures__/admin-context";

const TOPIC = {
  topic: "forecasts",
  readWhen: "read it when a forecast is doubted",
  text: "Three crowd readings exist, and they answer different questions.",
  enabled: true,
  updatedBy: null,
  createdAt: new Date("2026-11-15T12:00:00Z"),
  updatedAt: new Date("2026-11-15T12:00:00Z"),
};

function knowledge() {
  return {
    list: jest.fn(async () => [TOPIC]),
    get: jest.fn(async (topic: string) =>
      topic === "forecasts" ? TOPIC : null,
    ),
    history: jest.fn(async () => []),
    save: jest.fn(async () => ({
      topic: TOPIC,
      created: false,
      changed: true,
    })),
    remove: jest.fn(async (topic: string) =>
      topic === "forecasts" ? TOPIC : null,
    ),
  };
}

describe("Parky's knowledge base in the admin MCP", () => {
  it("lists every topic without its text, and reads one with its history", async () => {
    const service = knowledge();
    const { ctx } = adminContext("viewer", [
      [AssistantKnowledgeService, service],
    ]);
    const list = (await assistantKnowledgeTool.handler({}, ctx)) as {
      topics: Array<Record<string, unknown>>;
    };
    expect(list.topics).toEqual([
      expect.objectContaining({ topic: "forecasts", chars: TOPIC.text.length }),
    ]);
    expect(list.topics[0]).not.toHaveProperty("text");
    const one = await assistantKnowledgeTool.handler(
      { topic: "forecasts" },
      ctx,
    );
    expect(one).toMatchObject({ text: TOPIC.text, history: [] });
  });

  it("writes nothing on a dry run and answers the topic before and after", async () => {
    const service = knowledge();
    const { ctx } = adminContext("editor", [
      [AssistantKnowledgeService, service],
    ]);
    const input = z.object(writeAssistantKnowledgeTool.input).parse({
      topic: "forecasts",
      readWhen: "read it when a forecast or a level is doubted",
      text: TOPIC.text,
    });
    const result = await writeAssistantKnowledgeTool.handler(input, ctx);
    expect(result).toMatchObject({ dryRun: true, action: "update" });
    expect(service.save).not.toHaveBeenCalled();
  });

  it("saves under the calling account with dryRun false", async () => {
    const service = knowledge();
    const { ctx } = adminContext("editor", [
      [AssistantKnowledgeService, service],
    ]);
    await writeAssistantKnowledgeTool.handler(
      {
        topic: "forecasts",
        readWhen: TOPIC.readWhen,
        text: TOPIC.text,
        enabled: false,
        dryRun: false,
      },
      ctx,
    );
    expect(service.save).toHaveBeenCalledWith(
      "forecasts",
      { readWhen: TOPIC.readWhen, text: TOPIC.text, enabled: false },
      ctx.admin,
    );
  });

  it("refuses a write to a viewer and a delete without confirm", async () => {
    expect(
      (await callAs(writeAssistantKnowledgeTool, "viewer", {})).isError,
    ).toBe(true);
    expect(
      z
        .object(deleteAssistantKnowledgeTool.input)
        .safeParse({ topic: "forecasts" }).success,
    ).toBe(false);
  });

  it("deletes a topic it has and refuses one it has not", async () => {
    const service = knowledge();
    const { ctx } = adminContext("editor", [
      [AssistantKnowledgeService, service],
    ]);
    expect(
      await deleteAssistantKnowledgeTool.handler(
        { topic: "forecasts", confirm: true },
        ctx,
      ),
    ).toEqual({ deleted: TOPIC });
    await expect(
      deleteAssistantKnowledgeTool.handler(
        { topic: "tickets", confirm: true },
        ctx,
      ),
    ).rejects.toThrow(/No topic tickets/);
  });
});
