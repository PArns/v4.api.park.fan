/**
 * Reply chips written into the answer instead of the `suggest_replies` call: a label of their own
 * ("# Replies", "**Antworten:**", "Réponses :") over up to four short list items at the very end of
 * the answer. The page would show a "Replies" heading with bullets. The guard holds back every line
 * that may still turn into such a block until it is clear, so the stream never shows one, and hands
 * the block's items over to be sent as the chips.
 */

/** Labels as {@link plain} writes them: the chips' own name, never a word a real list may carry. */
const LABELS = [
  "replies",
  "reply",
  "suggested replies",
  "quick replies",
  "reply suggestions",
  "reply options",
  "reply chips",
  "suggest replies",
  "antworten",
  "antwortvorschlage",
  "antwortoptionen",
  "antwortmoglichkeiten",
  "mogliche antworten",
  "schnellantworten",
  "vorgeschlagene antworten",
  "antwoorden",
  "snelle antwoorden",
  "antwoordopties",
  "antwoordsuggesties",
  "voorgestelde antwoorden",
  "reponses",
  "reponses rapides",
  "reponses suggerees",
  "suggestions de reponse",
  "suggestions de reponses",
  "respuestas",
  "respuestas rapidas",
  "respuestas sugeridas",
  "sugerencias de respuesta",
  "risposte",
  "risposte rapide",
  "risposte suggerite",
  "suggerimenti di risposta",
];
/** One more item than the chips take is no replies block: it is a list. */
const MAX_ITEMS = 4;
/** An item longer than this is content, not a chip. */
const MAX_ITEM_CHARS = 60;

const ITEM = /^\s{0,3}(?:[-*+•–]|\d{1,2}[.)])\s+(.*\S)\s*$/u;
const ITEM_START = /^\s{0,3}(?:(?:[-*+•–]|\d{1,2}[.)])(?:\s.*)?|\d{1,2})?$/u;
const QUOTES = /^["'„“”«»‚‘’]+|["'„“”«»‚‘’]+$/gu;

/** A line without accents, case, Markdown marks or the colon: "**Réponses :**" is "reponses". */
function plain(line: string): string {
  return line
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .toLowerCase()
    .replace(/[#*_:]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function itemOf(line: string): string | null {
  const text = ITEM.exec(line)?.[1]
    .replace(/[*_`]/g, "")
    .replace(QUOTES, "")
    .trim();
  return text && text.length <= MAX_ITEM_CHARS ? text : null;
}

/**
 * Reads `text`, which starts at a line start, as a replies block: its items when it is one,
 * `"open"` while the stream may still make it one, null when it cannot be one.
 */
function readBlock(text: string, ended: boolean): string[] | "open" | null {
  const lines = text.split("\n");
  // Until the stream ends, the last line is still being written.
  const writing = (index: number) => !ended && index === lines.length - 1;
  let at = 0;
  while (at < lines.length - 1 && !lines[at].trim()) at++;
  if (writing(at)) {
    const label = plain(lines[at]);
    return LABELS.some((l) => l.startsWith(label)) ? "open" : null;
  }
  if (!LABELS.includes(plain(lines[at]))) return null;
  const items: string[] = [];
  for (let i = at + 1; i < lines.length; i++) {
    if (!lines[i].trim()) continue;
    if (writing(i)) {
      return items.length < MAX_ITEMS &&
        lines[i].length <= MAX_ITEM_CHARS + 10 &&
        ITEM_START.test(lines[i])
        ? "open"
        : null;
    }
    const item = itemOf(lines[i]);
    if (!item || items.length === MAX_ITEMS) return null;
    items.push(item);
  }
  return ended ? items : "open";
}

/**
 * Sits between the model's text and the stream: `push` each piece and send what it returns,
 * `finish` once the answer ends. What it shows is the answer as stored, without a trailing replies
 * block or trailing white space.
 */
export class RepliesTextGuard {
  private raw = "";
  private shown = 0;

  /** Whether the model wrote any text yet, shown or held. */
  get started(): boolean {
    return this.raw.length > 0;
  }

  /** The answer as shown so far; after {@link finish}, as it is stored. */
  get text(): string {
    return this.raw.slice(0, this.shown);
  }

  /** Takes the next piece of the answer and returns what may be shown now. */
  push(piece: string): string {
    this.raw += piece;
    return this.showUpTo(
      this.holdFrom((rest) => readBlock(rest, false) !== null),
    );
  }

  /**
   * The answer ended: returns the text still to show and the items of a trailing replies block,
   * which is left out of the text (null without one).
   */
  finish(): { rest: string; replies: string[] | null } {
    let replies: string[] | null = null;
    const end = this.holdFrom((rest) => {
      const block = readBlock(rest, true);
      if (Array.isArray(block)) replies = block;
      return Array.isArray(block);
    });
    return { rest: this.showUpTo(end), replies };
  }

  /**
   * Where the held text begins: the first line start from which `holds` still accepts the rest,
   * with the white space before it, else the trailing white space.
   */
  private holdFrom(holds: (rest: string) => boolean): number {
    let at = this.raw.length;
    for (let i = this.shown; i <= this.raw.length; i++) {
      if (i > 0 && this.raw[i - 1] !== "\n") continue;
      if (holds(this.raw.slice(i))) {
        at = i;
        break;
      }
    }
    while (at > this.shown && /\s/.test(this.raw[at - 1])) at--;
    return at;
  }

  private showUpTo(end: number): string {
    const piece = this.raw.slice(this.shown, end);
    this.shown = end;
    return piece;
  }
}
