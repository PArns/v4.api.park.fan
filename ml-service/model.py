"""
CatBoost model wrapper
"""
import os
from typing import Optional, Dict, Any, List
import joblib
from catboost import CatBoostRegressor, Pool
import pandas as pd
import numpy as np
from datetime import datetime
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
        self.feature_columns = get_feature_columns()
        self.categorical_features = get_categorical_features()

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
        y_val: pd.Series
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
        if 'parkId' in X_train.columns:
            X_train['parkId'] = X_train['parkId'].astype(str)
            X_val['parkId'] = X_val['parkId'].astype(str)

        if 'attractionId' in X_train.columns:
            X_train['attractionId'] = X_train['attractionId'].astype(str)
            X_val['attractionId'] = X_val['attractionId'].astype(str)

        # Create CatBoost pools
        train_pool = Pool(
            X_train[self.feature_columns],
            y_train,
            cat_features=self.categorical_features
        )

        val_pool = Pool(
            X_val[self.feature_columns],
            y_val,
            cat_features=self.categorical_features
        )

        # Initialize model with virtual ensembles for uncertainty estimation
        self.model = CatBoostRegressor(
            iterations=settings.CATBOOST_ITERATIONS,
            learning_rate=settings.CATBOOST_LEARNING_RATE,
            depth=settings.CATBOOST_DEPTH,
            l2_leaf_reg=settings.CATBOOST_L2_LEAF_REG,
            loss_function='RMSE',
            eval_metric='RMSE',
            random_seed=settings.CATBOOST_RANDOM_SEED,
            posterior_sampling=True,  # Enable virtual ensembles for uncertainty
            verbose=100,
            early_stopping_rounds=50
        )

        # Train
        self.model.fit(
            train_pool,
            eval_set=val_pool,
            use_best_model=True
        )

        # Calculate metrics
        y_pred = self.model.predict(X_val[self.feature_columns])
        metrics = self._calculate_metrics(y_val, y_pred)

        # Store metadata
        self.metadata = {
            'version': self.version,
            'trained_at': datetime.utcnow().isoformat(),
            'train_samples': len(X_train),
            'val_samples': len(X_val),
            'metrics': metrics,
            'features_used': self.feature_columns,
            'categorical_features': self.categorical_features,
            'hyperparameters': {
                'iterations': settings.CATBOOST_ITERATIONS,
                'learning_rate': settings.CATBOOST_LEARNING_RATE,
                'depth': settings.CATBOOST_DEPTH,
                'l2_leaf_reg': settings.CATBOOST_L2_LEAF_REG,
            }
        }

        return metrics

    def _calculate_metrics(self, y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
        """Calculate evaluation metrics"""
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        r2 = r2_score(y_true, y_pred)

        # MAPE (Mean Absolute Percentage Error)
        mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100

        return {
            'mae': float(mae),
            'rmse': float(rmse),
            'mape': float(mape),
            'r2': float(r2)
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

        print(f"✅ Model loaded: {model_path}")
        print(f"   Version: {self.version}")
        if self.metadata:
            print(f"   Trained at: {self.metadata.get('trained_at', 'unknown')}")
            print(f"   MAE: {self.metadata.get('metrics', {}).get('mae', 'N/A'):.2f}")

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        Make predictions

        Args:
            X: Features DataFrame

        Returns:
            Array of predictions
        """
        if self.model is None:
            raise ValueError("Model not loaded. Call load() first.")

        # Ensure all feature columns exist
        missing_cols = set(self.feature_columns) - set(X.columns)
        if missing_cols:
            raise ValueError(f"Missing feature columns: {missing_cols}")

        predictions = self.model.predict(X[self.feature_columns])

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

        # Ensure all feature columns exist
        missing_cols = set(self.feature_columns) - set(X.columns)
        if missing_cols:
            raise ValueError(f"Missing feature columns: {missing_cols}")

        # Get virtual predictions (returns array of shape [n_samples, n_virtual_ensembles])
        virtual_preds = self.model.virtual_ensembles_predict(
            X[self.feature_columns],
            prediction_type='TotalUncertainty',
            virtual_ensembles_count=10  # Use 10 virtual ensembles
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
            'predictions': predictions,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound,
            'uncertainty': uncertainty
        }

    def get_feature_importance(self) -> pd.DataFrame:
        """Get feature importance"""
        if self.model is None:
            raise ValueError("Model not loaded.")

        importance = self.model.get_feature_importance()
        feature_names = self.feature_columns

        return pd.DataFrame({
            'feature': feature_names,
            'importance': importance
        }).sort_values('importance', ascending=False)
