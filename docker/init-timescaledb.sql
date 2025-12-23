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


