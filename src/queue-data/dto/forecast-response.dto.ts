/**
 * Response DTO for GET /v1/attractions/:slug/forecasts
 * Returns wait time predictions
 */
export class ForecastItemDto {
  predictedTime: string; // ISO 8601
  predictedWaitTime: number;
  confidencePercentage: number | null;
  source: string; // 'themeparks_wiki' or 'our_ml_model'
}

export class ForecastResponseDto {
  attraction: {
    id: string;
    name: string;
    slug: string;
  };

  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
  };

  forecasts: ForecastItemDto[];
}
