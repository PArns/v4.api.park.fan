import { FORECASTS } from "./forecasts";
import type { KnowledgeEntry } from "./knowledge-entry";

export type { KnowledgeEntry };

/**
 * Parky's knowledge base, one entry per topic. A new topic is a file beside this one, added
 * here; the prompt's "# Knowledge" lists every entry's `readWhen`, and read_knowledge returns
 * its `text`. Changing an entry changes answers as the prompt does, so it bumps
 * `SYSTEM_PROMPT_VERSION` too. docs/frontend/assistant.md#parkys-knowledge-base.
 */
export const KNOWLEDGE: readonly KnowledgeEntry[] = [FORECASTS];

/** The topics read_knowledge takes, in the order the prompt lists them. */
export const KNOWLEDGE_TOPICS = KNOWLEDGE.map((entry) => entry.id) as [
  string,
  ...string[],
];

/** One entry by its topic, or undefined. */
export function knowledgeEntry(topic: string): KnowledgeEntry | undefined {
  return KNOWLEDGE.find((entry) => entry.id === topic);
}

/** The prompt's list of topics: one line each, its name and when to read it. */
export function knowledgeIndex(): string {
  return KNOWLEDGE.map((entry) => `- ${entry.id}: ${entry.readWhen}`).join(
    "\n",
  );
}
