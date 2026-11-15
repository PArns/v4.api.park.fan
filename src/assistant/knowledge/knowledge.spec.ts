import { SYSTEM_PROMPT } from "../system-prompt";
import { KNOWLEDGE, knowledgeEntry } from "./index";

/** A topic is read into a chat's context: past this it costs more than a lookup should. */
const MAX_TEXT_CHARS = 4000;

describe("Parky's knowledge base", () => {
  it("names every topic once, in the form read_knowledge takes", () => {
    const ids = KNOWLEDGE.map((entry) => entry.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const id of ids) expect(id).toMatch(/^[a-z]+(?:_[a-z]+)*$/);
  });

  it.each(KNOWLEDGE.map((entry) => [entry.id, entry] as const))(
    "%s says when to read it and stays short",
    (_id, entry) => {
      expect(entry.readWhen.length).toBeGreaterThan(20);
      expect(entry.text.length).toBeLessThanOrEqual(MAX_TEXT_CHARS);
    },
  );

  it("lists every topic in the prompt, and only there finds its text", () => {
    for (const entry of KNOWLEDGE) {
      expect(SYSTEM_PROMPT).toContain(`- ${entry.id}: ${entry.readWhen}`);
      expect(SYSTEM_PROMPT).not.toContain(entry.text);
      expect(knowledgeEntry(entry.id)).toBe(entry);
    }
  });
});
