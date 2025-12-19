import { Park } from "../entities/park.entity";
import { ScheduleEntry } from "../entities/schedule-entry.entity";
import { Repository } from "typeorm";

/**
 * Calculate similarity between two park names using Levenshtein distance
 * Returns a score from 0 (completely different) to 1 (identical)
 *
 * @param name1 - First park name
 * @param name2 - Second park name
 * @returns Similarity score (0-1)
 */
export function calculateNameSimilarity(name1: string, name2: string): number {
  // Normalize names: lowercase, remove special characters, trim whitespace
  const normalize = (str: string) =>
    str
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, "")
      .replace(/\s+/g, " ")
      .trim();

  const n1 = normalize(name1);
  const n2 = normalize(name2);

  if (n1 === n2) return 1.0;

  // Calculate Levenshtein distance
  const distance = levenshteinDistance(n1, n2);
  const maxLength = Math.max(n1.length, n2.length);

  // Convert distance to similarity (0-1)
  return maxLength === 0 ? 1.0 : 1 - distance / maxLength;
}

/**
 * Levenshtein distance algorithm
 * Calculates the minimum number of single-character edits needed to change one word into another
 */
function levenshteinDistance(str1: string, str2: string): number {
  const m = str1.length;
  const n = str2.length;

  // Create a 2D array for dynamic programming
  const dp: number[][] = Array(m + 1)
    .fill(null)
    .map(() => Array(n + 1).fill(0));

  // Initialize first row and column
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;

  // Fill the matrix
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      if (str1[i - 1] === str2[j - 1]) {
        dp[i][j] = dp[i - 1][j - 1];
      } else {
        dp[i][j] = Math.min(
          dp[i - 1][j] + 1, // deletion
          dp[i][j - 1] + 1, // insertion
          dp[i - 1][j - 1] + 1, // substitution
        );
      }
    }
  }

  return dp[m][n];
}

/**
 * Find a potential duplicate park based on name similarity
 * Uses a high threshold (90%) to avoid false positives
 *
 * @param parkName - Name of the new park to check
 * @param existingParks - List of existing parks to compare against
 * @param similarityThreshold - Minimum similarity score (default: 0.90)
 * @returns The most similar park if above threshold, null otherwise
 */
export function findDuplicatePark(
  parkName: string,
  existingParks: Park[],
  similarityThreshold: number = 0.9,
): Park | null {
  let bestMatch: Park | null = null;
  let bestScore = 0;

  for (const park of existingParks) {
    const score = calculateNameSimilarity(parkName, park.name);

    if (score >= similarityThreshold && score > bestScore) {
      bestScore = score;
      bestMatch = park;
    }
  }

  return bestMatch;
}

/**
 * Check if a park has any schedule entries
 *
 * @param parkId - Park ID (UUID)
 * @param scheduleRepo - ScheduleEntry repository
 * @returns True if park has at least one schedule entry
 */
export async function hasScheduleData(
  parkId: string,
  scheduleRepo: Repository<ScheduleEntry>,
): Promise<boolean> {
  const count = await scheduleRepo.count({
    where: { parkId },
  });

  return count > 0;
}

/**
 * Check if a park has recent queue data (within last 24 hours)
 *
 * @param parkId - Park ID (UUID)
 * @param dataSource - TypeORM DataSource for raw queries
 * @returns True if park has recent queue data
 */
export async function hasRecentQueueData(
  parkId: string,
  dataSource: any,
): Promise<boolean> {
  const result = await dataSource.query(
    `
    SELECT EXISTS (
      SELECT 1
      FROM attractions a
      JOIN queue_data qd ON qd."attractionId" = a.id
      WHERE a."parkId" = $1
      AND qd.timestamp > NOW() - INTERVAL '24 hours'
      LIMIT 1
    ) as has_data
    `,
    [parkId],
  );

  return result[0]?.has_data || false;
}

/**
 * Priority score for park merging
 * Higher score = preferred park to keep
 *
 * Criteria:
 * - Has recent queue data: +15 (HIGHEST PRIORITY - live data is most valuable)
 * - Has schedule data: +10
 * - Has Queue-Times ID: +5
 * - Has ThemeParks.wiki ID: +3
 * - Has geocoding data: +2
 *
 * @param park - Park entity
 * @param hasSchedule - Whether park has schedule entries
 * @param hasQueueData - Whether park has recent queue data (optional)
 * @returns Priority score
 */
export function calculateParkPriority(
  park: Park,
  hasSchedule: boolean,
  hasQueueData: boolean = false,
): number {
  let score = 0;

  // Recent queue data is the highest priority - means the park is actively used
  if (hasQueueData) score += 15;
  if (hasSchedule) score += 10;
  if (park.queueTimesEntityId) score += 5;
  if (park.wikiEntityId) score += 3;
  if (park.country && park.city) score += 2;

  return score;
}
