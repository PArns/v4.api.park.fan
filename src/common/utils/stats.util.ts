/** The arithmetic mean; undefined for no values. */
export function mean(values: readonly number[]): number | undefined {
  return values.length === 0
    ? undefined
    : values.reduce((s, v) => s + v, 0) / values.length;
}

/** The median, the mean of the middle two for an even count; undefined for no values. */
export function median(values: readonly number[]): number | undefined {
  if (values.length === 0) return undefined;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}
