/**
 * LIKE and ILIKE patterns from what a visitor or an admin typed. A bound parameter keeps `%` and
 * `_` as wildcards, so a search for "__" would match every row; escaped with Postgres' default
 * LIKE escape character, the backslash, they match only themselves.
 */

/** `value` with LIKE's wildcards and its escape character escaped. */
export function escapeLike(value: string): string {
  return value.replace(/[\\%_]/g, "\\$&");
}

/** A pattern matching `value` anywhere. */
export function likeContains(value: string): string {
  return `%${escapeLike(value)}%`;
}

/** A pattern matching `value` at the start. */
export function likeStartsWith(value: string): string {
  return `${escapeLike(value)}%`;
}
