-- Initialize TimescaleDB extension
-- This script runs automatically on first container startup

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Set timezone to UTC for consistency
SET timezone = 'UTC';

-- Log successful initialization
DO $$
BEGIN
    RAISE NOTICE 'TimescaleDB extension enabled successfully';
    RAISE NOTICE 'Database timezone set to UTC';
END $$;

-- ==================== INDEXES FOR ML DASHBOARD ====================
-- NOTE: These indexes are created IF NOT EXISTS to support idempotent execution
-- They will be created once the tables exist (after TypeORM creates them)

-- Composite index for faster date-based aggregation on prediction_accuracy
-- Used by: getDailyAccuracyTrends, getSystemAccuracyStats
-- Improves GROUP BY DATE(target_time) queries
CREATE INDEX IF NOT EXISTS idx_pa_target_actual
ON prediction_accuracy(target_time, actual_wait_time)
WHERE actual_wait_time IS NOT NULL;

-- Index for attraction-based queries with JOINs
-- Used by: getTopBottomPerformers (joins with attractions and parks tables)
-- Improves filtering by attraction and time range
CREATE INDEX IF NOT EXISTS idx_pa_attraction_target
ON prediction_accuracy(attraction_id, target_time);
