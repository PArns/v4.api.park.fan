"""
FastAPI ML Service for Wait Time Predictions
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging

from model import WaitTimeModel
from predict import predict_wait_times, predict_for_park
from schedule_filter import filter_predictions_by_schedule
from config import get_settings
from db import fetch_active_model_version

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()

# Initialize FastAPI
app = FastAPI(
    title="Park.fan ML Service",
    description="Wait time prediction service using CatBoost",
    version="1.0.0",
)

# Global model instance
model: Optional[WaitTimeModel] = None


@app.on_event("startup")
async def startup_event():
    """Load model on startup - queries database for active model"""
    global model
    try:
        # Query database for active model version
        model_version = fetch_active_model_version()
        logger.info(f"Loading active model version {model_version} (from database)...")
        model = WaitTimeModel(model_version)
        model.load()
        logger.info("✅ Model loaded successfully")
    except FileNotFoundError:
        logger.warning("⚠️  No trained model found. Train a model first using train.py")
        model = None
    except Exception as e:
        logger.error(f"❌ Error loading model: {e}")
        model = None


# Request/Response models
class WeatherForecastItem(BaseModel):
    """Hourly weather forecast item"""

    time: str
    temperature: Optional[float]
    precipitation: Optional[float]
    rain: Optional[float]
    snowfall: Optional[float]
    weatherCode: Optional[int]
    windSpeed: Optional[float]


class PredictionRequest(BaseModel):
    """Request for wait time predictions"""

    attractionIds: List[str]
    parkIds: List[str]
    predictionType: str = "hourly"  # 'hourly' or 'daily'
    baseTime: Optional[str] = None  # ISO format, defaults to now
    weatherForecast: Optional[List[WeatherForecastItem]] = None
    currentWaitTimes: Optional[Dict[str, int]] = None
    recentWaitTimes: Optional[Dict[str, int]] = None  # ~30 mins ago for velocity
    featureContext: Optional[Dict[str, Any]] = None  # Phase 2 features


class PredictionResponse(BaseModel):
    """Single prediction response"""

    attractionId: str
    parkId: str  # Added for schedule filtering
    predictedTime: str
    predictedWaitTime: int
    predictionType: str
    confidence: float
    trend: str = "stable"  # "increasing", "decreasing", "stable"
    crowdLevel: str
    baseline: float
    modelVersion: str


class BulkPredictionResponse(BaseModel):
    """Response containing multiple predictions"""

    predictions: List[PredictionResponse]
    count: int
    modelVersion: str


class ModelInfoResponse(BaseModel):
    """Model information"""

    version: str
    trainedAt: Optional[str]
    metrics: Optional[dict]
    features: Optional[List[str]]
    train_samples: Optional[int] = None
    val_samples: Optional[int] = None


# Endpoints
@app.get("/")
async def root():
    """Health check"""
    return {
        "service": "park.fan ML Service",
        "status": "running",
        "model_loaded": model is not None,
        "model_version": model.version if model else None,
    }


@app.get("/health")
async def health():
    """Health check endpoint - returns healthy even without model for initial deployment"""
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "model_version": model.version if model else None,
        "ready_for_predictions": model is not None,
    }


@app.get("/model/info", response_model=ModelInfoResponse)
async def get_model_info():
    """Get model information"""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    return ModelInfoResponse(
        version=model.version,
        trainedAt=model.metadata.get("trained_at"),
        metrics=model.metadata.get("metrics"),
        features=model.metadata.get("features_used"),
        train_samples=model.metadata.get("train_samples"),  # Add samples
        val_samples=model.metadata.get("val_samples"),  # Add samples
    )


@app.post("/model/reload")
async def reload_model():
    """Force reload of the active model from database"""
    global model
    try:
        model_version = fetch_active_model_version()
        logger.info(
            f"Reloading active model version {model_version} (from database)..."
        )

        new_model = WaitTimeModel(model_version)
        new_model.load()

        # Atomically swap
        model = new_model
        logger.info("✅ Model reloaded successfully")

        return {
            "status": "success",
            "message": f"Model reloaded. Version: {model.version}",
            "version": model.version,
        }
    except Exception as e:
        logger.error(f"❌ Error reloading model: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reload model: {str(e)}")


# Training status tracking
training_status = {
    "is_training": False,
    "current_version": None,
    "started_at": None,
    "finished_at": None,
    "status": "idle",
    "error": None,
}


class TrainRequest(BaseModel):
    """Training request"""

    version: Optional[str] = None


@app.post("/train")
async def train_model_endpoint(request: TrainRequest):
    """
    Trigger model training in background

    This endpoint starts training asynchronously and returns immediately.
    Use /train/status to check progress.
    """
    global training_status

    if training_status["is_training"]:
        raise HTTPException(status_code=409, detail="Training already in progress")

    # Generate version if not provided
    version = request.version
    if not version:
        now = datetime.now(timezone.utc)
        version = f"v{now.strftime('%Y%m%d_%H%M%S')}"

    # Start training in background thread
    import threading
    from train import train_model

    def training_worker():
        global training_status
        try:
            training_status["is_training"] = True
            training_status["current_version"] = version
            training_status["started_at"] = datetime.now(timezone.utc).isoformat()
            training_status["status"] = "training"
            training_status["error"] = None
            training_status["finished_at"] = None

            logger.info(f"🤖 Starting training in background for version {version}")
            train_model(version=version)

            training_status["status"] = "completed"
            training_status["finished_at"] = datetime.now(timezone.utc).isoformat()
            training_status["is_training"] = False

            logger.info(f"✅ Training completed for version {version}")

            # Auto-reload the new model
            try:
                global model
                new_model = WaitTimeModel(version)
                new_model.load()
                model = new_model
                logger.info("✅ New model loaded automatically")
            except Exception as e:
                logger.error(f"Failed to auto-load new model: {e}")

        except Exception as e:
            import traceback

            error_traceback = traceback.format_exc()
            logger.error(f"❌ Training failed: {e}")
            logger.error(f"Full traceback:\n{error_traceback}")
            training_status["status"] = "failed"
            training_status["error"] = f"{str(e)}\n\nTraceback:\n{error_traceback}"
            training_status["finished_at"] = datetime.now(timezone.utc).isoformat()
            training_status["is_training"] = False

    # Start background thread
    thread = threading.Thread(target=training_worker, daemon=True)
    thread.start()

    return {
        "status": "training_started",
        "version": version,
        "message": f"Training started in background for version {version}",
        "check_status_at": "/train/status",
    }


@app.get("/train/status")
async def get_training_status():
    """Get current training status"""
    return {
        "is_training": training_status["is_training"],
        "current_version": training_status["current_version"],
        "started_at": training_status["started_at"],
        "finished_at": training_status["finished_at"],
        "status": training_status["status"],
        "error": training_status["error"],
    }


@app.post("/predict", response_model=BulkPredictionResponse)
async def predict(request: PredictionRequest):
    """
    Predict wait times for attractions

    Args:
        request: Prediction request with attraction IDs, park IDs, type

    Returns:
        Bulk prediction response
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    if len(request.attractionIds) != len(request.parkIds):
        raise HTTPException(
            status_code=400, detail="attractionIds and parkIds must have same length"
        )

    if request.predictionType not in ["hourly", "daily"]:
        raise HTTPException(
            status_code=400, detail="predictionType must be 'hourly' or 'daily'"
        )

    # Parse base time
    base_time = None
    if request.baseTime:
        try:
            base_time = datetime.fromisoformat(request.baseTime.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid baseTime format")

    try:
        # Make predictions
        predictions = predict_wait_times(
            model,
            request.attractionIds,
            request.parkIds,
            request.predictionType,
            base_time,
            request.weatherForecast,
            request.currentWaitTimes,
            request.recentWaitTimes,
            request.featureContext,
        )

        # Apply schedule filtering for both hourly and daily predictions
        # Hourly: Only hours within operating times
        # Daily: Only days when park is open (no off-season)
        predictions = filter_predictions_by_schedule(
            predictions, request.parkIds, request.predictionType
        )

        return BulkPredictionResponse(
            predictions=[PredictionResponse(**p) for p in predictions],
            count=len(predictions),
            modelVersion=model.version,
        )

    except Exception as e:
        import traceback

        logger.error(f"Prediction error: {e}")
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/park/{park_id}", response_model=BulkPredictionResponse)
async def predict_park(park_id: str, prediction_type: str = "hourly"):
    """
    Predict wait times for all attractions in a park

    Args:
        park_id: Park ID
        prediction_type: 'hourly' or 'daily'

    Returns:
        Bulk prediction response
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    if prediction_type not in ["hourly", "daily"]:
        raise HTTPException(
            status_code=400, detail="prediction_type must be 'hourly' or 'daily'"
        )

    try:
        predictions = predict_for_park(model, park_id, prediction_type)

        return BulkPredictionResponse(
            predictions=[PredictionResponse(**p) for p in predictions],
            count=len(predictions),
            modelVersion=model.version,
        )

    except Exception as e:
        import traceback

        logger.error(f"Park prediction error: {e}")
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
