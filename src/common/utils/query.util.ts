import { Brackets, ObjectLiteral, SelectQueryBuilder } from "typeorm";

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

/**
 * `a OR b OR c` in brackets, so it ANDs with the rest of a query as one term.
 * Every condition shares `parameters`.
 */
export function anyOf(
  conditions: string[],
  parameters?: ObjectLiteral,
): Brackets {
  return new Brackets((where) => {
    conditions.forEach((condition, index) => {
      if (index === 0) where.where(condition, parameters);
      else where.orWhere(condition, parameters);
    });
  });
}
