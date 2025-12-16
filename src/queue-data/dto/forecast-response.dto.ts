import { ApiProperty } from "@nestjs/swagger";

/**
 * Response DTO for GET /v1/attractions/:slug/forecasts
 * Returns wait time predictions
 */
export class ForecastItemDto {
  @ApiProperty({ description: "Predicted time (ISO 8601)" })
  predictedTime: string; // ISO 8601

  @ApiProperty({ description: "Predicted wait time in minutes" })
  predictedWaitTime: number;

  @ApiProperty({
    description: "Confidence percentage",
    required: false,
    nullable: true,
  })
  confidencePercentage: number | null;

  @ApiProperty({ description: "Source of the prediction" })
  source: string; // 'themeparks_wiki' or 'our_ml_model'
}

export class ForecastResponseDto {
  @ApiProperty({ description: "Attraction details" })
  attraction: {
    id: string;
    name: string;
    slug: string;
  };

  @ApiProperty({ description: "Park details" })
  park: {
    id: string;
    name: string;
    slug: string;
    timezone: string;
  };

  @ApiProperty({
    description: "List of forecast items",
    type: [ForecastItemDto],
  })
  forecasts: ForecastItemDto[];
}
