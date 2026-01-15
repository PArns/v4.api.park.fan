/**
 * Search Counts Types
 *
 * Used for search result counts
 */

export interface EntityCount {
  returned: number;
  total: number;
}

export interface SearchCounts {
  park: EntityCount;
  attraction: EntityCount;
  show: EntityCount;
  restaurant: EntityCount;
  location: EntityCount;
}
