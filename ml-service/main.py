"""
FastAPI ML Service for Wait Time Predictions
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime
import logging

from model import WaitTimeModel
from predict import predict_wait_times, predict_for_park
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
    version="1.0.0"
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
    predictionType: str = 'hourly'  # 'hourly' or 'daily'
    baseTime: Optional[str] = None  # ISO format, defaults to now
    weatherForecast: Optional[List[WeatherForecastItem]] = None
    currentWaitTimes: Optional[Dict[str, int]] = None


class PredictionResponse(BaseModel):
    """Single prediction response"""
    attractionId: str
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


# Endpoints
@app.get("/")
async def root():
    """Health check"""
    return {
        "service": "park.fan ML Service",
        "status": "running",
        "model_loaded": model is not None,
        "model_version": model.version if model else None
    }


@app.get("/health")
async def health():
    """Health check endpoint - returns healthy even without model for initial deployment"""
    return {
        "status": "healthy",
        "model_loaded": model is not None,
        "model_version": model.version if model else None,
        "ready_for_predictions": model is not None
    }


@app.get("/model/info", response_model=ModelInfoResponse)
async def get_model_info():
    """Get model information"""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    return ModelInfoResponse(
        version=model.version,
        trainedAt=model.metadata.get('trained_at'),
        metrics=model.metadata.get('metrics'),
        features=model.metadata.get('features_used')
    )


@app.post("/model/reload")
async def reload_model():
    """Force reload of the active model from database"""
    global model
    try:
        model_version = fetch_active_model_version()
        logger.info(f"Reloading active model version {model_version} (from database)...")
        
        new_model = WaitTimeModel(model_version)
        new_model.load()
        
        # Atomically swap
        model = new_model
        logger.info("✅ Model reloaded successfully")
        
        return {
            "status": "success", 
            "message": f"Model reloaded. Version: {model.version}",
            "version": model.version
        }
    except Exception as e:
        logger.error(f"❌ Error reloading model: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reload model: {str(e)}")



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
            status_code=400,
            detail="attractionIds and parkIds must have same length"
        )

    if request.predictionType not in ['hourly', 'daily']:
        raise HTTPException(
            status_code=400,
            detail="predictionType must be 'hourly' or 'daily'"
        )

    # Parse base time
    base_time = None
    if request.baseTime:
        try:
            base_time = datetime.fromisoformat(request.baseTime.replace('Z', '+00:00'))
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
            request.currentWaitTimes
        )

        return BulkPredictionResponse(
            predictions=[PredictionResponse(**p) for p in predictions],
            count=len(predictions),
            modelVersion=model.version
        )

    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/park/{park_id}", response_model=BulkPredictionResponse)
async def predict_park(
    park_id: str,
    prediction_type: str = 'hourly'
):
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

    if prediction_type not in ['hourly', 'daily']:
        raise HTTPException(
            status_code=400,
            detail="prediction_type must be 'hourly' or 'daily'"
        )

    try:
        predictions = predict_for_park(model, park_id, prediction_type)

        return BulkPredictionResponse(
            predictions=[PredictionResponse(**p) for p in predictions],
            count=len(predictions),
            modelVersion=model.version
        )

    except Exception as e:
        logger.error(f"Park prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
