/**
 * Control and bidirectional-formatting characters. A label someone else typed
 * (an OAuth client's name, a token's name) must read as what it is on an admin
 * page, not as "Claude" followed by a right-to-left override.
 */
const INVISIBLE_CHARACTERS = /[\u0000-\u001f\u007f-\u009f​-‏‪-‮⁦-⁩﻿]/g;

/** `value` without invisible characters, trimmed. */
export function stripInvisibleCharacters(value: string): string {
  return value.replace(INVISIBLE_CHARACTERS, "").trim();
}
