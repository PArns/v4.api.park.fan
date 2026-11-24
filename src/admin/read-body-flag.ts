/**
 * Read a flag out of a body that nothing validates, and refuse to guess.
 *
 * An inline body rather than a DTO gives the `ValidationPipe` no metatype to
 * work with, so it coerces nothing — and Nest parses
 * `application/x-www-form-urlencoded` out of the box, where every value
 * arrives as a string. `dryRun=true` from a curl one-liner is therefore
 * `"true"`, and `"true" === true` is false: a strict comparison reads the one
 * request that asked in as many words for a dry run as permission to delete.
 * Which is, to the letter, the trap the attraction merge endpoint sprang once.
 *
 * A value that is neither is `undefined` here and a 400 at the call site. On an
 * endpoint that deletes parks, a `dryRun: "yes"` nobody can interpret must not
 * be interpreted — in either direction. The one value that is not a 400 is
 * `null`, which the call site reads as the absent key it stands for in JSON.
 */
export function readBodyFlag(value: unknown): boolean | undefined {
  if (typeof value === "boolean") return value;
  if (value === "true") return true;
  if (value === "false") return false;
  return undefined;
}
