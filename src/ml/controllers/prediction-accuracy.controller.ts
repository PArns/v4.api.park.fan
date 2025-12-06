import { Controller, Get, Param, Query, ParseIntPipe } from "@nestjs/common";
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
