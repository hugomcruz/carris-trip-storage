-- Create database if not exists
CREATE DATABASE IF NOT EXISTS trip_data;
USE trip_data;

-- Trips table to store completion metadata
CREATE TABLE IF NOT EXISTS trips (
    id INT AUTO_INCREMENT PRIMARY KEY,
    trip_id VARCHAR(255) UNIQUE NOT NULL,
    vehicle_id VARCHAR(100),
    route_id VARCHAR(100),
    driver_id VARCHAR(100),
    start_time DATETIME,
    end_time DATETIME,
    distance_km DECIMAL(10, 2),
    duration_minutes INT,
    status VARCHAR(50),
    passenger_count INT,
    fare_amount DECIMAL(10, 2),
    completion_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_trip_id (trip_id),
    INDEX idx_vehicle_id (vehicle_id),
    INDEX idx_start_time (start_time),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Trip processing log to track processed trips
CREATE TABLE IF NOT EXISTS trip_processing_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    trip_id VARCHAR(255) NOT NULL,
    processing_status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
    parquet_file_path VARCHAR(500),
    error_message TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_trip_id (trip_id),
    INDEX idx_status (processing_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
