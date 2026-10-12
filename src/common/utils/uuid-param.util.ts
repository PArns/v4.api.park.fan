/**
 * Bind ids so Postgres can use the index on a uuid column (PAR-871).
 *
 * The rule: never cast an indexed uuid column, cast the parameter.
 *
 *     WRONG  a."parkId"::text = $1
 *     RIGHT  a."parkId" = $1::uuid          -- or = ANY($1::uuid[])
 *
 * Casting the parameter adds one failure mode: a value that is not a uuid makes
 * the cast raise 22P02 and the whole statement fails, where the text comparison
 * just matched nothing. Test with `isCanonicalUuid` first. It accepts exactly
 * what `uuid::text` prints (lowercase, hyphenated), so a value it rejects is one
 * the old comparison could never have matched: same rows, before and after.
 */
const CANONICAL_UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export function isCanonicalUuid(value: unknown): value is string {
  return typeof value === "string" && CANONICAL_UUID.test(value);
}
