import { z } from "zod";
import { FORECASTS } from "./forecasts";
import type { KnowledgeEntry } from "./knowledge-entry";

export type { KnowledgeEntry };

/**
 * The topics an empty knowledge base starts with. After that the table is the base: the admin and
 * the admin MCP edit it, and these files are never read again (`AssistantKnowledgeService.seed`).
 */
export const KNOWLEDGE_SEED: readonly KnowledgeEntry[] = [FORECASTS];

/** A topic's name: lowercase words joined by "_", as read_knowledge, the admin and the MCP take it. */
export const knowledgeTopicSchema = z
  .string()
  .trim()
  .min(2)
  .max(40)
  .regex(/^[a-z]+(?:_[a-z]+)*$/, "lowercase words joined by _");

/** A topic's text is read into a chat's context: past this it costs more than a lookup should. */
export const KNOWLEDGE_MAX_TEXT_CHARS = 4000;

/** The fields of one topic as the admin and the MCP write them. */
export const knowledgeWriteSchema = z.object({
  readWhen: z.string().trim().min(20).max(400),
  text: z.string().trim().min(40).max(KNOWLEDGE_MAX_TEXT_CHARS),
  enabled: z.boolean().default(true),
});

export type KnowledgeWrite = z.infer<typeof knowledgeWriteSchema>;

/** The prompt's list of topics: one line each, its name and when to read it. */
export function knowledgeIndex(
  entries: readonly Pick<KnowledgeEntry, "id" | "readWhen">[],
): string {
  return entries.map((entry) => `- ${entry.id}: ${entry.readWhen}`).join("\n");
}
