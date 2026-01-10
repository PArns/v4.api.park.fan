"""
CatBoost model wrapper
"""

import os
from typing import Optional, Dict, Any, List
import joblib
from catboost import CatBoostRegressor, Pool
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from config import get_settings
from features import get_feature_columns, get_categorical_features

settings = get_settings()


class WaitTimeModel:
    """Wrapper for CatBoost wait time prediction model"""

    def __init__(self, model_version: Optional[str] = None):
        """
        Initialize model wrapper

        Args:
            model_version: Model version to load. If None, uses settings.MODEL_VERSION
        """
        self.version = model_version or settings.MODEL_VERSION
        self.model: Optional[CatBoostRegressor] = None
        self.metadata: Dict[str, Any] = {}
        # Current feature columns (may include new features not in old models)
        self.feature_columns = get_feature_columns()
        self.categorical_features = get_categorical_features()
        # Model-specific feature columns (set after load, for backward compatibility)
        self.model_feature_columns: Optional[List[str]] = None

    def get_model_path(self) -> str:
        """Get path to model file"""
        os.makedirs(settings.MODEL_DIR, exist_ok=True)
        return os.path.join(settings.MODEL_DIR, f"catboost_{self.version}.cbm")

    def get_metadata_path(self) -> str:
        """Get path to metadata file"""
        os.makedirs(settings.MODEL_DIR, exist_ok=True)
        return os.path.join(settings.MODEL_DIR, f"metadata_{self.version}.pkl")

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series,
        sample_weights: Optional[np.ndarray] = None,
    ) -> Dict[str, float]:
        """
        Train CatBoost model

        Args:
            X_train: Training features
            y_train: Training target
            X_val: Validation features
            y_val: Validation target

        Returns:
            Dict with metrics (mae, rmse, mape, r2)
        """
        # Convert UUID columns to strings for CatBoost
        X_train = X_train.copy()
        X_val = X_val.copy()

        # Convert parkId and attractionId from UUID to string
        if "parkId" in X_train.columns:
            X_train["parkId"] = X_train["parkId"].astype(str)
            X_val["parkId"] = X_val["parkId"].astype(str)

        if "attractionId" in X_train.columns:
            X_train["attractionId"] = X_train["attractionId"].astype(str)
            X_val["attractionId"] = X_val["attractionId"].astype(str)

        # Create CatBoost pools with optional sample weights
        train_pool = Pool(
            X_train[self.feature_columns],
            y_train,
            cat_features=self.categorical_features,
            weight=sample_weights if sample_weights is not None else None,
        )

        val_pool = Pool(
            X_val[self.feature_columns], y_val, cat_features=self.categorical_features
        )

        # Initialize model with virtual ensembles for uncertainty estimation
        import time
        import os

        # Determine thread count (use all available cores by default)
        thread_count = settings.CATBOOST_THREAD_COUNT
        if thread_count == -1:
            # Use all available CPU cores
            thread_count = os.cpu_count() or 4

        print(f"   Thread count: {thread_count}")
        print(f"   Task type: {settings.CATBOOST_TASK_TYPE}")

        training_start = time.time()

        self.model = CatBoostRegressor(
            iterations=settings.CATBOOST_ITERATIONS,
            learning_rate=settings.CATBOOST_LEARNING_RATE,
            depth=settings.CATBOOST_DEPTH,
            l2_leaf_reg=settings.CATBOOST_L2_LEAF_REG,
            loss_function="RMSE",
            eval_metric="RMSE",
            random_seed=settings.CATBOOST_RANDOM_SEED,
            posterior_sampling=True,  # Enable virtual ensembles for uncertainty
            thread_count=thread_count,  # Use all CPU cores for parallel training
            task_type=settings.CATBOOST_TASK_TYPE,  # CPU or GPU
            verbose=100,
            early_stopping_rounds=50,
        )

        # Train
        self.model.fit(train_pool, eval_set=val_pool, use_best_model=True)

        training_time = time.time() - training_start
        print(
            f"\n   Training completed in {training_time:.2f}s ({training_time / 60:.1f} minutes)"
        )

        # Calculate metrics
        y_pred = self.model.predict(X_val[self.feature_columns])
        metrics = self._calculate_metrics(y_val, y_pred)

        # Store metadata
        self.metadata = {
            "version": self.version,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "train_samples": len(X_train),
            "val_samples": len(X_val),
            "metrics": metrics,
            "features_used": self.feature_columns,
            "categorical_features": self.categorical_features,
            "hyperparameters": {
                "iterations": settings.CATBOOST_ITERATIONS,
                "learning_rate": settings.CATBOOST_LEARNING_RATE,
                "depth": settings.CATBOOST_DEPTH,
                "l2_leaf_reg": settings.CATBOOST_L2_LEAF_REG,
            },
        }

        return metrics

    def _calculate_metrics(
        self, y_true: np.ndarray, y_pred: np.ndarray
    ) -> Dict[str, float]:
        """Calculate evaluation metrics"""
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)

        # MAPE (Mean Absolute Percentage Error)
        # Filter out zero values to avoid division by zero
        mask = y_true > 0
        if mask.sum() > 0:
            mape = np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100
        else:
            mape = 0.0  # No valid values for MAPE calculation

        return {
            "mae": float(mae),
            "rmse": float(rmse),
            "mape": float(mape),
            "r2": float(r2),
        }

    def save(self) -> None:
        """Save model and metadata to disk"""
        if self.model is None:
            raise ValueError("No model to save. Train first.")

        # Save CatBoost model
        self.model.save_model(self.get_model_path())

        # Save metadata
        joblib.dump(self.metadata, self.get_metadata_path())

        print(f"✅ Model saved: {self.get_model_path()}")
        print(f"✅ Metadata saved: {self.get_metadata_path()}")

    def load(self) -> None:
        """Load model and metadata from disk"""
        model_path = self.get_model_path()
        metadata_path = self.get_metadata_path()

        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found: {model_path}")

        # Load CatBoost model
        self.model = CatBoostRegressor()
        self.model.load_model(model_path)

        # Load metadata
        if os.path.exists(metadata_path):
            self.metadata = joblib.load(metadata_path)

        # BACKWARD COMPATIBILITY: Use model's feature list if available
        # This allows old models to work with new code that has additional features
        if self.metadata and "features_used" in self.metadata:
            self.model_feature_columns = self.metadata["features_used"]
            expected_features = set(self.model_feature_columns)
            actual_features = set(self.feature_columns)

            missing_features = expected_features - actual_features
            extra_features = actual_features - expected_features

            if missing_features:
                print(
                    f"⚠️  WARNING: Model expects {len(missing_features)} features that are missing in current code:"
                )
                for feat in sorted(missing_features):
                    print(f"   - {feat}")
                print(
                    "   → These will be filled with default values for backward compatibility"
                )

            if extra_features:
                print(
                    f"ℹ️  INFO: {len(extra_features)} new features available (not used by this model):"
                )
                for feat in sorted(extra_features):
                    print(f"   - {feat}")
                print(
                    f"   → Model will use only {len(self.model_feature_columns)} features from training"
                )
        else:
            # No metadata: assume current feature list (new model)
            self.model_feature_columns = self.feature_columns
            print(
                f"ℹ️  INFO: No metadata found, using current feature list ({len(self.feature_columns)} features)"
            )

        print(f"✅ Model loaded: {model_path}")
        print(f"   Version: {self.version}")
        if self.metadata:
            print(f"   Trained at: {self.metadata.get('trained_at', 'unknown')}")
            print(f"   MAE: {self.metadata.get('metrics', {}).get('mae', 'N/A'):.2f}")
            print(
                f"   Features: {len(self.model_feature_columns)} (model) vs {len(self.feature_columns)} (current)"
            )

    def _get_default_feature_values(self) -> Dict[str, Any]:
        """
        Get default values for features (for backward compatibility with old models)

        Note: parkId and attractionId should NOT be in defaults - they must be provided
        in the input DataFrame. If they're missing, it's a data error, not a compatibility issue.

        Returns:
            Dict mapping feature name to default value
        """
        defaults = {
            # IDs (categorical) - These should always be provided, but if missing, use placeholder
            # Note: In practice, these should never be missing, but we provide defaults for safety
            "parkId": "UNKNOWN_PARK",
            "attractionId": "UNKNOWN_ATTRACTION",
            # Time features
            "hour": 12,
            "day_of_week": 3,  # Thursday
            "month": 6,  # June
            "hour_sin": 0.0,
            "hour_cos": 1.0,
            "day_of_week_sin": 0.0,
            "day_of_week_cos": 1.0,
            "month_sin": 0.0,
            "month_cos": 1.0,
            "day_of_year_sin": 0.0,  # NEW
            "day_of_year_cos": 1.0,  # NEW
            "season": 2,  # Summer
            "is_weekend": 0,
            "is_peak_season": 1,  # NEW
            # Weather features
            "temperature_avg": 20.0,
            "temperature_deviation": 0.0,  # NEW
            "precipitation": 0.0,
            "precipitation_last_3h": 0.0,  # NEW
            "windSpeedMax": 0.0,
            "snowfallSum": 0.0,
            "weatherCode": 0,
            "is_raining": 0,
            # Holiday features
            "is_holiday_primary": 0,
            "is_school_holiday_primary": 0,
            "is_holiday_neighbor_1": 0,
            "is_holiday_neighbor_2": 0,
            "is_holiday_neighbor_3": 0,
            "holiday_count_total": 0,
            "school_holiday_count_total": 0,
            "is_school_holiday_any": 0,
            "is_bridge_day": 0,
            # Park schedule features
            "is_park_open": 1,
            "has_special_event": 0,
            "has_extra_hours": 0,
            # Attraction features
            "attraction_type": "UNKNOWN",  # Categorical
            "park_attraction_count": 0,
            # Historical features
            "avg_wait_last_24h": 0.0,
            "avg_wait_last_1h": 0.0,
            "avg_wait_same_hour_last_week": 0.0,
            "avg_wait_same_hour_last_month": 0.0,  # NEW
            "rolling_avg_7d": 0.0,
            "wait_time_velocity": 0.0,
            "trend_7d": 0.0,  # NEW
            "volatility_7d": 0.0,  # NEW
            # Percentile features
            "is_temp_extreme": 0,
            "is_wind_extreme": 0,
            # Context features
            "park_occupancy_pct": 100.0,
            "time_since_park_open_mins": 0.0,
            "had_downtime_today": 0,
            "downtime_minutes_today": 0.0,
            "has_virtual_queue": 0,
            "park_has_schedule": 1,
            # Interaction features
            "hour_weekend_interaction": 0.0,
            "hour_is_weekend": 0.0,  # NEW
            "temp_precip_interaction": 0.0,
            "holiday_occupancy_interaction": 0.0,
            "hour_occupancy_interaction": 0.0,
        }
        return defaults

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Make predictions with backward compatibility support

        Args:
            X: Features DataFrame

        Returns:
            Array of predictions
        """
        if self.model is None:
            raise ValueError("Model not loaded. Call load() first.")

        # Use model's feature list (from metadata) for backward compatibility
        # If not loaded yet, use current feature list
        model_features = self.model_feature_columns or self.feature_columns

        # BACKWARD COMPATIBILITY: Fill missing features with defaults
        missing_cols = set(model_features) - set(X.columns)
        if missing_cols:
            defaults = self._get_default_feature_values()
            # Reduced logging - only log critical missing features
            critical_missing = [
                col for col in missing_cols if col in ["parkId", "attractionId"]
            ]
            if critical_missing:
                print(
                    f"⚠️  CRITICAL: Missing required features: {critical_missing}. Predictions may be inaccurate."
                )
            else:
                # Only log summary for non-critical missing features
                print(
                    f"⚠️  Filling {len(missing_cols)} missing features with defaults (backward compatibility)"
                )

            for col in sorted(missing_cols):
                # Special handling for categorical features
                if col in ["parkId", "attractionId"]:
                    # These should never be missing in practice, but provide default for safety
                    default_val = defaults.get(col, "UNKNOWN")
                elif col in self.categorical_features:
                    # Categorical features: use string default
                    default_val = defaults.get(col, "UNKNOWN")
                else:
                    # Numeric features: use numeric default
                    default_val = defaults.get(col, 0.0)
                X[col] = default_val

        # Check for extra columns (warn but don't fail)
        extra_cols = set(X.columns) - set(model_features)
        if extra_cols:
            import warnings

            warnings.warn(
                f"Extra columns in DataFrame (will be ignored): {sorted(extra_cols)}",
                UserWarning,
            )

        # Ensure columns are in correct order (CatBoost is sensitive to order)
        # Use model's feature order (from training)
        X_ordered = X[model_features].copy()

        predictions = self.model.predict(X_ordered)

        # Ensure no negative predictions
        predictions = np.maximum(predictions, 0)

        return predictions

    def predict_with_uncertainty(self, X: pd.DataFrame) -> Dict[str, np.ndarray]:
        """
        Make predictions with uncertainty intervals using virtual ensembles

        Args:
            X: Features DataFrame

        Returns:
            Dict with:
                - predictions: Point predictions
                - lower_bound: Lower bound of 90% prediction interval
                - upper_bound: Upper bound of 90% prediction interval
                - uncertainty: Width of prediction interval (upper - lower)
        """
        if self.model is None:
            raise ValueError("Model not loaded. Call load() first.")

        # Use model's feature list (from metadata) for backward compatibility
        model_features = self.model_feature_columns or self.feature_columns

        # BACKWARD COMPATIBILITY: Fill missing features with defaults
        missing_cols = set(model_features) - set(X.columns)
        if missing_cols:
            defaults = self._get_default_feature_values()
            for col in missing_cols:
                default_val = defaults.get(col, 0.0)
                X[col] = default_val

        # Ensure columns are in correct order (CatBoost is sensitive to order)
        X_ordered = X[model_features].copy()

        # Get virtual predictions (returns array of shape [n_samples, n_virtual_ensembles])
        virtual_preds = self.model.virtual_ensembles_predict(
            X_ordered,
            prediction_type="TotalUncertainty",
            virtual_ensembles_count=10,  # Use 10 virtual ensembles
        )

        # Calculate statistics
        predictions = np.mean(virtual_preds, axis=1)
        lower_bound = np.percentile(virtual_preds, 5, axis=1)  # 5th percentile
        upper_bound = np.percentile(virtual_preds, 95, axis=1)  # 95th percentile
        uncertainty = upper_bound - lower_bound

        # Ensure no negative predictions
        predictions = np.maximum(predictions, 0)
        lower_bound = np.maximum(lower_bound, 0)
        upper_bound = np.maximum(upper_bound, 0)

        return {
            "predictions": predictions,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "uncertainty": uncertainty,
        }

    def get_feature_importance(self) -> pd.DataFrame:
        """Get feature importance"""
        if self.model is None:
            raise ValueError("Model not loaded.")

        importance = self.model.get_feature_importance()
        # Use model's feature list (from training) for accurate importance mapping
        feature_names = self.model_feature_columns or self.feature_columns

        return pd.DataFrame(
            {"feature": feature_names, "importance": importance}
        ).sort_values("importance", ascending=False)
