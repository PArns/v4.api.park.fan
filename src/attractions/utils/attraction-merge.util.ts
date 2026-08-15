import { generateSlug } from "../../common/utils/slug.util";
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
  survivingName?: string,
): string {
  const winnerBase = winnerSlug.replace(/-[0-9]+$/, "");

  if (winnerBase !== winnerSlug && winnerBase === loserSlug) {
    return loserSlug;
  }

  // Duplicates found by a shared Queue-Times id need a different rule, because
  // their slugs do not derive from one another at all. The winner is whichever
  // row ingestion still feeds, and that row's slug can be an unrelated leftover:
  // merging Energylandia's pair would have published "Choco Chip Creek" at
  // /main-train-2, and "Mini Track' Tour Ride" at /lolipop-farm.
  //
  // So when neither slug is the other's stem, prefer whichever one the
  // surviving NAME actually produces. A public URL should read like the ride.
  if (survivingName) {
    const fromName = generateSlug(survivingName);
    if (loserSlug === fromName && winnerSlug !== fromName) {
      return loserSlug;
    }
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
  return (
    name
      // Queue-Times publishes some parks' own map numbers inside the ride
      // name — Energylandia's feed says "Draken (155)" and "Frutti Loop (39)"
      // where ThemeParks.wiki says "Draken" and "Frutti Loop". Stripping the
      // trailing number is what lets those two rows recognise each other; it
      // is an artefact of one source's formatting, not part of the name.
      .replace(/\s*\(\d+\)\s*$/, "")
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "")
  );
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
 * So we require positive evidence: the names must agree once punctuation, case
 * and a trailing map number are stripped. Everything else is left for a human.
 *
 * A shared Queue-Times id used to be sufficient on its own. It is not, and
 * Carowinds shows why: `Blackbeard's Revenge - Cannonball Drop & Captain's
 * Curse` and `Blackbeard's Revenge - Pirate's Plank` both carry id 14744, and
 * their slugs say `tube-slides` and `drop-slides`. They are two slide
 * complexes the upstream lumped under one id, and auto-merging them would
 * destroy one. The same shape appears at Kings Island, where two stations of
 * one railroad share an id, and at Six Flags Great Escape, where three
 * Sasquatch rows do.
 *
 * The id still finds the pair — see the detector — it just no longer decides
 * it. That costs a human glance at cases like `Kiddy Hawk Cove` / `Kiddy Hawk`,
 * which is the right price for a merge that cannot be undone.
 */
export function isSafeToAutoMerge(
  a: DuplicateCandidate,
  b: DuplicateCandidate,
): boolean {
  return normalizeName(a.name) === normalizeName(b.name);
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
