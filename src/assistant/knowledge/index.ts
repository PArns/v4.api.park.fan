import { z } from "zod";
import { APP } from "./app";
import { BEST_TIMES } from "./best-times";
import { CROWD_SCALE } from "./crowd-scale";
import { DATA_SOURCES } from "./data-sources";
import { FORECASTS } from "./forecasts";
import { PRIVACY } from "./privacy";
import { RIDE_STATUS } from "./ride-status";
import type { KnowledgeEntry } from "./knowledge-entry";

export type { KnowledgeEntry };

/**
 * The topics the knowledge base starts with. At boot each one the table has never held is added
 * (`AssistantKnowledgeService.seed`); one that is there is never overwritten and one deleted in the
 * admin is never added back. After that the table is the base, which the admin and the MCP edit.
 */
export const KNOWLEDGE_SEED: readonly KnowledgeEntry[] = [
  FORECASTS,
  CROWD_SCALE,
  DATA_SOURCES,
  RIDE_STATUS,
  BEST_TIMES,
  PRIVACY,
  APP,
];

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
