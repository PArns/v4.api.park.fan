import { z } from "zod";
import { defineMcpTool, McpToolError } from "../mcp-tool";
import { confirmInput, dryRunInput } from "../mcp-write";
import { AssistantKnowledgeService } from "../../assistant/assistant-knowledge.service";
import {
  KNOWLEDGE_MAX_TEXT_CHARS,
  knowledgeTopicSchema,
  knowledgeWriteSchema,
} from "../../assistant/knowledge";
import { adminKnowledgeUrl } from "./links";

/**
 * Parky's knowledge base through the admin MCP: read it, write a topic, delete one, as the
 * admin's knowledge page does. Writes go through the same service, audited under this account
 * with the whole topic before and after.
 */

export const assistantKnowledgeTool = defineMcpTool({
  name: "assistant_knowledge",
  title: "Parky's knowledge base",
  description:
    "Parky's knowledge base, the wiki he reads with read_knowledge only when a question needs " +
    "it: without topic, every topic with when Parky reads it (readWhen, which his prompt lists), " +
    "whether it is on, its length and its last edit; with topic, that topic in full and its " +
    "edits, newest first, each with the whole topic before and after. Topics are English, " +
    "written for Parky, never shown to visitors.",
  input: {
    topic: knowledgeTopicSchema
      .optional()
      .describe("One topic to read in full, with its history"),
  },
  async handler({ topic }, ctx) {
    const knowledge = ctx.get(AssistantKnowledgeService);
    if (!topic) {
      const topics = await knowledge.list();
      return {
        adminUrl: adminKnowledgeUrl(),
        topics: topics.map((entry) => ({
          topic: entry.topic,
          readWhen: entry.readWhen,
          enabled: entry.enabled,
          chars: entry.text.length,
          updatedAt: entry.updatedAt,
          updatedBy: entry.updatedBy,
        })),
      };
    }
    const entry = await knowledge.get(topic);
    if (!entry) throw new McpToolError(`No topic ${topic}`);
    return {
      adminUrl: adminKnowledgeUrl(topic),
      ...entry,
      history: await knowledge.history(topic),
    };
  },
});

export const writeAssistantKnowledgeTool = defineMcpTool({
  name: "write_assistant_knowledge",
  title: "Write a topic of Parky's knowledge base",
  description:
    "Creates a topic of Parky's knowledge base or replaces its fields, as the admin's knowledge " +
    "page does. readWhen is the line his prompt lists: what the topic holds and when to read it " +
    `(20 to 400 characters). text is the background itself, in English, for Parky (40 to ` +
    `${KNOWLEDGE_MAX_TEXT_CHARS} characters): facts he says in his own words, and what to do with ` +
    "them. enabled false keeps a topic but takes it out of his prompt. The next chat reads the " +
    "change within a minute, without a deploy. A dry run (the default) answers the topic before " +
    "and after without saving; with dryRun false the write is audited under this account. Read " +
    "the topic with assistant_knowledge first.",
  minRole: "editor",
  writes: "change",
  input: {
    topic: knowledgeTopicSchema.describe(
      "The topic's name: an existing one to change, a new one (lowercase words joined by _) to create",
    ),
    readWhen: knowledgeWriteSchema.shape.readWhen.describe(
      "What the topic holds and when Parky reads it, the line his prompt lists",
    ),
    text: knowledgeWriteSchema.shape.text.describe(
      "The background itself, in English, written for Parky",
    ),
    enabled: z
      .boolean()
      .default(true)
      .describe("false keeps the topic out of Parky's prompt"),
    dryRun: dryRunInput("the topic before and after"),
  },
  async handler({ topic, readWhen, text, enabled, dryRun }, ctx) {
    const knowledge = ctx.get(AssistantKnowledgeService);
    if (dryRun) {
      const before = await knowledge.get(topic);
      return {
        dryRun: true,
        action: before ? "update" : "create",
        before,
        after: { topic, readWhen, text, enabled },
      };
    }
    const saved = await knowledge.save(
      topic,
      { readWhen, text, enabled },
      ctx.admin,
    );
    return { dryRun: false, ...saved, adminUrl: adminKnowledgeUrl(topic) };
  },
});

export const deleteAssistantKnowledgeTool = defineMcpTool({
  name: "delete_assistant_knowledge",
  title: "Delete a topic of Parky's knowledge base",
  description:
    "Deletes one topic of Parky's knowledge base for good, as the admin's delete does: the " +
    "audit log keeps the whole topic under this account, enough to write it back. A topic that " +
    "should only rest is written with enabled false instead.",
  minRole: "editor",
  writes: "delete",
  input: {
    topic: knowledgeTopicSchema.describe("The topic, from assistant_knowledge"),
    confirm: confirmInput("deletes the topic"),
  },
  async handler({ topic }, ctx) {
    const removed = await ctx
      .get(AssistantKnowledgeService)
      .remove(topic, ctx.admin);
    if (!removed) throw new McpToolError(`No topic ${topic}`);
    return { deleted: removed };
  },
});
