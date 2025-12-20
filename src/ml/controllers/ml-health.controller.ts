import { Controller, Get } from "@nestjs/common";
import { ApiTags, ApiOperation, ApiResponse } from "@nestjs/swagger";
import { MLService } from "../ml.service";
import { MLModelService } from "../services/ml-model.service";

/**
 * MLHealthController
 *
 * Health and status endpoints for ML system monitoring
 */
@ApiTags("ML Dashboard")
@ApiTags("health")
@Controller("ml")
export class MLHealthController {
  constructor(
    private mlService: MLService,
    private modelService: MLModelService,
  ) {}

  /**
   * Check ML Service Health
   *
   * GET /v1/ml/health
   *
   * Returns comprehensive ML system health:
   * - ML service (Python FastAPI) connectivity
   * - Active model information and age
   * - System operational status
   */
  @Get("health")
  @ApiOperation({
    summary: "Check ML service health",
    description:
      "Verifies ML service connectivity and returns active model status",
  })
  @ApiResponse({
    status: 200,
    description: "ML service health status with model info",
  })
  @ApiResponse({
    status: 503,
    description: "ML service is unavailable",
  })
  async checkHealth() {
    const [isHealthy, activeModel] = await Promise.all([
      this.mlService.isHealthy(),
      this.modelService.getActiveModel(),
    ]);

    return {
      timestamp: new Date().toISOString(),
      status: isHealthy ? "healthy" : "unhealthy",
      mlService: {
        status: isHealthy ? "healthy" : "unhealthy",
        url: process.env.ML_SERVICE_URL || "http://ml-service:8000",
      },
      model: activeModel
        ? {
            version: activeModel.version,
            trainedAt: activeModel.trainedAt,
            age: this.modelService.getModelAge(new Date(activeModel.trainedAt)),
            isActive: activeModel.isActive,
            metrics: {
              mae: activeModel.mae,
              rmse: activeModel.rmse,
              mape: activeModel.mape,
              r2: activeModel.r2Score,
            },
          }
        : null,
      message: activeModel
        ? "ML system operational"
        : "No active model - train a model first",
    };
  }
}
