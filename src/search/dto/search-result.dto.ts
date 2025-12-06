export class SearchResultItemDto {
  type: "park" | "attraction" | "show" | "restaurant";
  id: string;
  slug: string;
}

export class SearchResultDto {
  results: SearchResultItemDto[];
  total: number;
  query: string;
  searchTypes: string[];
}
