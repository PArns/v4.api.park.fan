import { ObjectLiteral, SelectQueryBuilder } from "typeorm";

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

/**
 * Applies page/limit pagination to a query builder and returns data + total.
 * Shared by the attractions/shows/restaurants list endpoints.
 */
export async function paginate<T extends ObjectLiteral>(
  queryBuilder: SelectQueryBuilder<T>,
  page: number = 1,
  limit: number = 10,
): Promise<{ data: T[]; total: number }> {
  const [data, total] = await queryBuilder
    .skip((page - 1) * limit)
    .take(limit)
    .getManyAndCount();
  return { data, total };
}
