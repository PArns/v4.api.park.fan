import { RepliesTextGuard } from "./assistant-replies-text";

/** Streams `text` in pieces of `size` characters, as the model's deltas arrive. */
function stream(text: string, size: number) {
  const guard = new RepliesTextGuard();
  const pieces: string[] = [];
  for (let i = 0; i < text.length; i += size) {
    pieces.push(guard.push(text.slice(i, i + size)));
  }
  const { rest, replies } = guard.finish();
  pieces.push(rest);
  return { shown: pieces.join(""), replies, stored: guard.text };
}

describe("RepliesTextGuard", () => {
  const answer =
    "Klar, gern Frage für Frage! Zuerst die Basics: **Welcher Park** soll es sein, und **welcher Tag**?";

  it("never streams a replies block and hands its items over as the chips", () => {
    for (const size of [1, 3, 40]) {
      const { shown, replies, stored } = stream(
        `${answer}\n\n# Replies\n- Heute\n- Morgen\n- Ich nenne dir den Park gleich`,
        size,
      );
      expect(shown).toBe(answer);
      expect(stored).toBe(answer);
      expect(replies).toEqual([
        "Heute",
        "Morgen",
        "Ich nenne dir den Park gleich",
      ]);
    }
  });

  it.each([
    "**Antworten:**\n* „Heute“\n* Morgen",
    "Réponses :\n\n1. Aujourd'hui\n2. Demain\n",
    "## suggest_replies\n- Today\n- Tomorrow\n\n",
  ])("reads the label and items of %j", (block) => {
    const { shown, replies } = stream(`${answer}\n\n${block}`, 2);
    expect(shown).toBe(answer);
    expect(replies).toHaveLength(2);
  });

  it.each([
    // A list under a sentence of its own is content.
    `${answer}\n\nMeine Tipps:\n- Taron\n- Black Mamba`,
    // A label word that a paragraph goes on from.
    `${answer}\n\nAntworten kann ich dir gleich.\n- Taron`,
    // Five items are a list, not chips.
    `${answer}\n\nReplies:\n- a\n- b\n- c\n- d\n- e`,
    // A block with text after it is not the answer's end.
    `${answer}\n\nReplies\n- Heute\n- Morgen\n\nDann geht's los.`,
  ])("leaves %j as it is", (text) => {
    const { shown, replies } = stream(text, 3);
    expect(shown).toBe(text);
    expect(replies).toBeNull();
  });
});
