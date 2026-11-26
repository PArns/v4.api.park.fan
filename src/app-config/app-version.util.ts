/** `1`, `1.0` or `1.0.1`; each part a number, so `1.10` is above `1.9`. */
export const VERSION_PATTERN = /^\d+(\.\d+){0,2}$/;

/** A version string as three numbers, missing parts 0; null when it is not a version. */
export function parseVersion(value: string): [number, number, number] | null {
  if (!VERSION_PATTERN.test(value)) return null;
  const [major = 0, minor = 0, patch = 0] = value.split(".").map(Number);
  return [major, minor, patch];
}

/**
 * Whether an app of this version and build is too old: its version is below
 * the minimum, or equal to it with a build below the minimum build. No minimum
 * blocks nothing; a version that cannot be read is not blocked either, since
 * the server must never lock out an app it cannot place.
 */
export function isBelowMinimum(
  app: { version: string; build: number | null },
  min: { minVersion: string | null; minBuild: number | null },
): boolean {
  if (!min.minVersion) return false;
  const have = parseVersion(app.version);
  const need = parseVersion(min.minVersion);
  if (!have || !need) return false;
  for (let i = 0; i < 3; i++) {
    if (have[i] !== need[i]) return have[i] < need[i];
  }
  return (
    min.minBuild !== null && app.build !== null && app.build < min.minBuild
  );
}
