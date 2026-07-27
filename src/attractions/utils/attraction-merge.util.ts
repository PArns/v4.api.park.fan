/**
 * Decides which slug the surviving row of an attraction merge should carry.
 *
 * `generateUniqueSlug` appends "-2", "-3", … on collision, so a suffixed slug
 * is almost always the accidental second row while the bare one is the URL
 * already in the sitemap and in Google's index. The survivor should therefore
 * take the base slug whenever the pair is a base/suffix pair.
 *
 * The suffix is only stripped when the other row actually holds the resulting
 * base slug — that keeps names genuinely ending in digits ("Route 66",
 * "PEANUTS™ 500") intact, and avoids claiming a base slug that may still be
 * occupied by some third row in the same park.
 */
export function resolveSurvivingSlug(
  winnerSlug: string,
  loserSlug: string,
): string {
  const winnerBase = winnerSlug.replace(/-[0-9]+$/, "");

  if (winnerBase !== winnerSlug && winnerBase === loserSlug) {
    return loserSlug;
  }

  return winnerSlug;
}

export interface DuplicateCandidate {
  id: string;
  slug: string;
  name: string;
  queueTimesEntityId: string | null;
  hasCoordinates: boolean;
  /** Rows written in the last 7 days — i.e. is ingestion still feeding this row. */
  recentQueueRows: number;
  totalQueueRows: number;
  createdAt: Date;
}

function normalizeName(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]/g, "");
}

/**
 * Whether two rows sharing a base/suffix slug are provably the same ride.
 *
 * A shared slug stem is NOT sufficient. Among the real pairs, `main-train` and
 * `main-train-2` hold "Main Train" and "Choco Chip Creek (215)" — two distinct
 * rides that merely collided, and merging them would destroy one. Likewise
 * "Power Builder" vs its Halloween overlay, and "Spindeln - Nyhet" vs
 * "Spindeln - Nyhet 2026", which is a legitimately numbered ride name.
 *
 * So we require positive evidence: either the names agree once punctuation and
 * case are stripped, or both rows carry the same Queue-Times id — which means
 * the upstream source itself considers them one ride. Everything else is left
 * for a human.
 */
export function isSafeToAutoMerge(
  a: DuplicateCandidate,
  b: DuplicateCandidate,
): boolean {
  if (normalizeName(a.name) === normalizeName(b.name)) {
    return true;
  }

  return (
    a.queueTimesEntityId !== null &&
    a.queueTimesEntityId === b.queueTimesEntityId
  );
}

/**
 * Picks which of two duplicate rows survives.
 *
 * Metadata is inherited from the loser either way and the dedupe of same-key
 * rows is symmetric, so the choice is about which identity stays: the row that
 * live ingestion is still writing to. Deeper history and finally age break the
 * remaining ties. Mirrors the weighting of `calculateParkPriority`, where
 * recent queue data is likewise the strongest signal.
 */
export function chooseDuplicateWinner(
  a: DuplicateCandidate,
  b: DuplicateCandidate,
): { winnerId: string; loserId: string } {
  const rank = (c: DuplicateCandidate): number[] => [
    c.recentQueueRows > 0 ? 1 : 0,
    c.recentQueueRows,
    c.totalQueueRows,
    c.queueTimesEntityId ? 1 : 0,
    c.hasCoordinates ? 1 : 0,
    -c.createdAt.getTime(), // older wins
  ];

  const rankA = rank(a);
  const rankB = rank(b);

  for (let i = 0; i < rankA.length; i++) {
    if (rankA[i] !== rankB[i]) {
      return rankA[i] > rankB[i]
        ? { winnerId: a.id, loserId: b.id }
        : { winnerId: b.id, loserId: a.id };
    }
  }

  return { winnerId: a.id, loserId: b.id };
}
