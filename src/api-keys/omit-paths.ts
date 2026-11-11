/**
 * A copy of `value` without the fields the paths name. A path is dot-separated
 * and `[]` after a segment walks every element of that array:
 * `attractions[].bestVisitTimes` drops `bestVisitTimes` from each attraction.
 *
 * Only the objects on a path are copied; everything else is shared with the
 * input, which is never changed. That matters because the input is often the
 * very object a service keeps in its cache.
 */
export function omitPaths<T>(value: T, paths: readonly string[]): T {
  let result: unknown = value;
  for (const path of paths) result = omitPath(result, path.split("."));
  return result as T;
}

function omitPath(value: unknown, segments: string[]): unknown {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return value;
  }
  const [head, ...rest] = segments;
  const walksArray = head.endsWith("[]");
  const key = walksArray ? head.slice(0, -2) : head;
  const record = value as Record<string, unknown>;
  if (!(key in record)) return value;

  if (rest.length === 0 && !walksArray) {
    const { [key]: _omitted, ...kept } = record;
    return kept;
  }
  const child = record[key];
  const next = walksArray
    ? Array.isArray(child)
      ? child.map((item) => omitPath(item, rest))
      : child
    : omitPath(child, rest);
  return next === child ? value : { ...record, [key]: next };
}
