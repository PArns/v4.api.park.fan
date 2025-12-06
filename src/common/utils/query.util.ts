/**
 * Query parsing and normalization utilities
 */

/**
 * Normalize sort direction to SQL-safe value
 *
 * Ensures only valid SQL sort directions are used in queries
 *
 * @param direction - User-provided sort direction (case-insensitive)
 * @returns 'ASC' or 'DESC'
 */
export function normalizeSortDirection(direction: string): "ASC" | "DESC" {
  return direction.toUpperCase() === "DESC" ? "DESC" : "ASC";
}

/**
 * Parse sort parameter into field and direction
 *
 * Handles formats like "name:asc", "createdAt:desc"
 *
 * @param sort - Sort parameter string (e.g., "name:asc")
 * @returns Tuple of [field, direction]
 */
export function parseSortParameter(sort: string): [string, "ASC" | "DESC"] {
  const [field, direction = "asc"] = sort.split(":");
  return [field, normalizeSortDirection(direction)];
}
