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
    ) { }

    /**
     * Check ML Service Health
     *
     * GET /v1/ml/health
     *
     * Checks if the ML service (Python FastAPI) is reachable
     */
    @Get("health")
    @ApiOperation({
        summary: "Check ML service health",
        description: "Verifies connectivity to the ML service",
    })
    @ApiResponse({
        status: 200,
        description: "ML service is healthy",
    })
    @ApiResponse({
        status: 503,
        description: "ML service is unavailable",
    })
    async checkHealth() {
        const isHealthy = await this.mlService.isHealthy();

        return {
            status: isHealthy ? "healthy" : "unhealthy",
            service: "ML Service (Python FastAPI)",
            timestamp: new Date().toISOString(),
        };
    }

    /**
     * Get ML System Status
     *
     * GET /v1/ml/status
     *
     * Returns overall ML system status including:
     * - ML service connectivity
     * - Active model information
     * - Recent prediction activity
     */
    @Get("status")
    @ApiOperation({
        summary: "Get ML system status",
        description:
            "Returns comprehensive status including service health and model info",
    })
    @ApiResponse({
        status: 200,
        description: "Status retrieved successfully",
    })
    async getStatus() {
        const [isHealthy, activeModel] = await Promise.all([
            this.mlService.isHealthy(),
            this.modelService.getActiveModel(),
        ]);

        return {
            timestamp: new Date().toISOString(),
            mlService: {
                status: isHealthy ? "healthy" : "unhealthy",
                url: process.env.ML_SERVICE_URL || "http://ml-service:8000",
            },
            model: activeModel
                ? {
                    version: activeModel.version,
                    trainedAt: activeModel.trainedAt,
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
