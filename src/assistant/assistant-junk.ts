/**
 * Messages that are not a question at all, recognised without a model call so
 * they cost no tokens. Deliberately narrow: a real question taken for junk
 * annoys a visitor, while junk let through only costs what any question costs
 * and gets the prompt's playful off-topic line. So only the unmistakable
 * shapes count: nothing to read, one key held down, and letters typed without
 * a single vowel.
 */

/** Vowels in the six languages Parky speaks; `y` counts, so "rhythm" passes. */
const VOWEL = /[aeiouyäöüàáâæèéêëìíîïòóôœùúû]/iu;

const LATIN_WORD = /^[a-zäöüßàáâæçèéêëìíîïñòóôœùúûÿ]+$/iu;

/**
 * Why `message` is junk, or null for anything that might be a question.
 *
 * - `no-words`: not one letter or digit in any script ("???", "🙂🙂🙂").
 * - `one-key`: every letter is the same one, and no digit ("aaaaaa", "k").
 * - `mash`: at least six Latin letters and not one vowel among them
 *   ("sdfghjk", "hjk lkjh"). Abbreviations are written in capitals, so a
 *   message in capitals only is let through ("BTMRR TTD").
 */
export function junkReason(
  message: string,
): "no-words" | "one-key" | "mash" | null {
  const text = message.trim();
  if (!/[\p{L}\p{N}]/u.test(text)) return "no-words";

  const letters = [...text.toLowerCase()].filter((c) => /\p{L}/u.test(c));
  if (
    letters.length > 0 &&
    !/\p{N}/u.test(text) &&
    letters.every((c) => c === letters[0])
  ) {
    return "one-key";
  }

  const words = text.split(/\s+/).filter(Boolean);
  if (
    letters.length >= 6 &&
    words.every((word) => LATIN_WORD.test(word)) &&
    !VOWEL.test(text) &&
    text !== text.toUpperCase()
  ) {
    return "mash";
  }
  return null;
}

/** How long a repeated question counts as a repeat rather than a fresh look at live data. */
export const REPEAT_WINDOW_MS = 2 * 60 * 1000;

/**
 * Whether `message` repeats the last question of this chat within
 * {@link REPEAT_WINDOW_MS}, and that question got an answer. A retry after a
 * failed answer is not a repeat, and the same question minutes later asks for
 * waits that have moved since.
 */
export function repeatsLastQuestion(
  message: string,
  earlier: ReadonlyArray<{ role: string; text: string; createdAt: Date }>,
  now: Date,
): boolean {
  const lastAnswer = earlier[earlier.length - 1];
  const lastQuestion = earlier[earlier.length - 2];
  if (
    lastAnswer?.role !== "assistant" ||
    lastQuestion?.role !== "user" ||
    !lastAnswer.text.trim()
  ) {
    return false;
  }
  const same = (a: string) => a.trim().toLowerCase().replace(/\s+/g, " ");
  return (
    same(lastQuestion.text) === same(message) &&
    now.getTime() - lastQuestion.createdAt.getTime() < REPEAT_WINDOW_MS
  );
}
