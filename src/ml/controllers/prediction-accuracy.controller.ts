import { Controller, Get, Param, Query, ParseIntPipe } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { PredictionAccuracyService } from "../services/prediction-accuracy.service";

/**
 * Prediction Accuracy Controller
 *
 * Endpoints for viewing prediction vs actual comparisons at multiple scales:
 * - Per Attraction: Individual ride accuracy
 * - Per Park: Overall park prediction quality
 * - Global: System-wide performance metrics
 *
 * Useful for:
 * - Monitoring model performance
 * - Debugging prediction errors
 * - Building user trust with transparency
 * - Identifying which parks/attractions need model improvements
 */
@ApiTags("predictions")
@Controller("ml/accuracy")
export class PredictionAccuracyController {
  constructor(private readonly accuracyService: PredictionAccuracyService) {}

  /**
   * Get global accuracy statistics across all predictions
   *
   * Shows system-wide performance to track if model is improving over time
   *
   * GET /v1/ml/accuracy/global?days=30
   */
  @Get("global")
  @ApiOperation({
    summary: "Get global prediction accuracy",
    description:
      "Returns system-wide accuracy statistics covering all attractions",
  })
  @ApiResponse({
    status: 200,
    description: "Global accuracy stats retrieved successfully",
  })
  async getGlobalStats(
    @Query("days", new ParseIntPipe({ optional: true })) days?: number,
  ) {
    const stats = await this.accuracyService.getGlobalAccuracyStats(days || 30);

    return {
      period: `Last ${days || 30} days`,
      statistics: stats,
    };
  }

  /**
   * Get accuracy statistics for a park (all attractions averaged)
   *
   * Useful to see which parks have better/worse predictions
   *
   * GET /v1/ml/accuracy/parks/:parkId/stats?days=30
   */
  @Get("parks/:parkId/stats")
  @ApiOperation({
    summary: "Get park accuracy statistics",
    description: "Returns aggregated prediction accuracy for a specific park",
  })
  @ApiResponse({
    status: 200,
    description: "Park accuracy stats retrieved successfully",
  })
  async getParkStats(
    @Param("parkId") parkId: string,
    @Query("days", new ParseIntPipe({ optional: true })) days?: number,
  ) {
    const stats = await this.accuracyService.getParkAccuracyStats(
      parkId,
      days || 30,
    );

    return {
      parkId,
      period: `Last ${days || 30} days`,
      statistics: stats,
    };
  }

  /**
   * Get prediction accuracy statistics for an attraction
   *
   * GET /v1/ml/accuracy/attractions/:slug/stats?days=30
   */
  @Get("attractions/:attractionId/stats")
  @ApiOperation({
    summary: "Get attraction accuracy statistics",
    description:
      "Returns prediction accuracy metrics for a specific attraction",
  })
  @ApiResponse({
    status: 200,
    description: "Attraction accuracy stats retrieved successfully",
  })
  async getAttractionStats(
    @Param("attractionId") attractionId: string,
    @Query("days", new ParseIntPipe({ optional: true })) days?: number,
  ) {
    const stats = await this.accuracyService.getAttractionAccuracyStats(
      attractionId,
      days || 30,
    );

    return {
      attractionId,
      period: `Last ${days || 30} days`,
      statistics: stats,
    };
  }

  /**
   * Get recent prediction vs actual comparisons
   *
   * GET /v1/ml/accuracy/attractions/:slug/comparisons?limit=50
   */
  @Get("attractions/:attractionId/comparisons")
  @ApiOperation({
    summary: "Get recent prediction comparisons",
    description: "Returns a list of recent predictions vs actual wait times",
  })
  @ApiResponse({
    status: 200,
    description: "Comparisons retrieved successfully",
  })
  async getComparisons(
    @Param("attractionId") attractionId: string,
    @Query("limit", new ParseIntPipe({ optional: true })) limit?: number,
  ) {
    const comparisons = await this.accuracyService.getRecentComparisons(
      attractionId,
      limit || 50,
    );

    return {
      attractionId,
      count: comparisons.length,
      comparisons,
    };
  }

  /**
   * Analyze which features correlate with high prediction errors
   *
   * Identifies patterns in high-error predictions:
   * - Which hours are hardest to predict
   * - Which weather conditions cause issues
   * - Which days of week have higher errors
   * - Temperature ranges with more errors
   *
   * GET /v1/ml/accuracy/features/analysis?threshold=15&days=30&attractionId=uuid
   */
  @Get("features/analysis")
  @ApiOperation({
    summary: "Analyze prediction error correlations",
    description:
      "Identifies features (time, weather, etc.) associated with high prediction errors",
  })
  @ApiResponse({
    status: 200,
    description: "Feature analysis retrieved successfully",
  })
  async analyzeFeatureErrors(
    @Query("threshold", new ParseIntPipe({ optional: true }))
    threshold?: number,
    @Query("days", new ParseIntPipe({ optional: true })) days?: number,
    @Query("attractionId") attractionId?: string,
  ) {
    const analysis = await this.accuracyService.analyzeFeatureErrors(
      threshold || 15,
      days || 30,
      attractionId,
    );

    return analysis;
  }
}
