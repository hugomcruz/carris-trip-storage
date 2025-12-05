-- Create database if not exists
-- Note: PostgreSQL doesn't support IF NOT EXISTS for CREATE DATABASE in older versions
-- Run this manually if needed: CREATE DATABASE carris_trips;

-- Connect to the database first
-- \c carris_trips;

-- Trips table to store completion metadata
CREATE TABLE IF NOT EXISTS trips (
    id SERIAL PRIMARY KEY,
    trip_id VARCHAR(255) UNIQUE NOT NULL,
    vehicle_id VARCHAR(100),
    route_id VARCHAR(100),
    driver_id VARCHAR(100),
    license_plate VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    distance_km DECIMAL(10, 2),
    duration_minutes INTEGER,
    duration_seconds INTEGER,
    status VARCHAR(50),
    passenger_count INTEGER,
    fare_amount DECIMAL(10, 2),
    stops_served INTEGER,
    total_positions INTEGER,
    completion_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_trips_trip_id ON trips(trip_id);
CREATE INDEX IF NOT EXISTS idx_trips_vehicle_id ON trips(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_trips_start_time ON trips(start_time);
CREATE INDEX IF NOT EXISTS idx_trips_status ON trips(status);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_trips_updated_at BEFORE UPDATE ON trips
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Trip processing log to track processed trips
CREATE TYPE processing_status_enum AS ENUM ('pending', 'processing', 'completed', 'failed');

CREATE TABLE IF NOT EXISTS trip_processing_log (
    id SERIAL PRIMARY KEY,
    trip_id VARCHAR(255) NOT NULL,
    processing_status processing_status_enum DEFAULT 'pending',
    parquet_file_path VARCHAR(500),
    error_message TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for processing log
CREATE INDEX IF NOT EXISTS idx_processing_log_trip_id ON trip_processing_log(trip_id);
CREATE INDEX IF NOT EXISTS idx_processing_log_status ON trip_processing_log(processing_status);
