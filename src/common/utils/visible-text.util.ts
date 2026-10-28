/**
 * Characters that change how a label reads without being seen: controls,
 * bidirectional overrides and isolates, zero-width and joiner characters, line
 * and paragraph separators, the soft hyphen, the Hangul and Mongolian fillers,
 * and the tag characters. A label someone else typed (an OAuth client's name,
 * a token's name) must read as what it is on an admin page, not as "Claude"
 * followed by a right-to-left override.
 *
 * Written as a string of escapes so no tool turns them into literal
 * characters nobody can see in the source.
 */
const INVISIBLE_CHARACTERS = new RegExp(
  "[" +
    "\\u0000-\\u001f\\u007f-\\u009f" +
    "\\u00ad\\u061c\\u115f\\u1160\\u180e\\u3164\\uffa0\\ufeff" +
    "\\u200b-\\u200f\\u2028-\\u202e\\u2060-\\u2064\\u2066-\\u2069" +
    "\\u{e0000}-\\u{e007f}" +
    "]",
  "gu",
);

/** `value` without invisible characters, trimmed. */
export function stripInvisibleCharacters(value: string): string {
  return value.replace(INVISIBLE_CHARACTERS, "").trim();
}
