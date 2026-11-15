/**
 * One topic of Parky's knowledge base: background he reads with read_knowledge only when a
 * question needs it, so it costs nothing in the chats that do not.
 */
export interface KnowledgeEntry {
  /** The topic's name, as read_knowledge takes it: lowercase, words joined by "_". */
  id: string;
  /** What the topic holds and when to read it, one sentence or two: the prompt lists these. */
  readWhen: string;
  /** The background itself, in English, written for Parky and never shown to a visitor. */
  text: string;
}
