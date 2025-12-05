"""
MySQL client module for storing trip data.
Handles connections to MySQL and insertion of trip completion data.
"""

import mysql.connector
from mysql.connector import Error
import json
import logging
from typing import Dict, Optional, Any
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MySQLClient:
    """MySQL client for trip data storage."""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        """
        Initialize MySQL client.
        
        Args:
            host: MySQL host
            port: MySQL port
            user: MySQL user
            password: MySQL password
            database: MySQL database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to MySQL."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.connection.cursor(dictionary=True)
            logger.info(f"Successfully connected to MySQL at {self.host}:{self.port}/{self.database}")
        except Error as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            raise
    
    def disconnect(self):
        """Close MySQL connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Disconnected from MySQL")
    
    def insert_trip_completion(self, trip_data: Dict[str, Any]) -> bool:
        """
        Insert trip completion data into the trips table.
        
        Args:
            trip_data: Dictionary containing trip completion data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract fields from trip data
            trip_id = trip_data.get('trip_id') or trip_data.get('id')
            vehicle_id = trip_data.get('vehicle_id')
            route_id = trip_data.get('route_id')
            driver_id = trip_data.get('driver_id')
            license_plate = trip_data.get('license_plate')
            
            # Parse timestamps
            start_time = self._parse_timestamp(trip_data.get('start_time'))
            end_time = self._parse_timestamp(trip_data.get('end_time'))
            
            # Extract numeric values
            distance_km = trip_data.get('distance_km') or trip_data.get('distance')
            duration_minutes = trip_data.get('duration_minutes') or trip_data.get('duration')
            duration_seconds = trip_data.get('duration_seconds')
            status = trip_data.get('status', 'completed')
            passenger_count = trip_data.get('passenger_count') or trip_data.get('passengers')
            fare_amount = trip_data.get('fare_amount') or trip_data.get('fare')
            stops_served = trip_data.get('stops_served')
            total_positions = trip_data.get('total_positions')
            
            # Store complete data as JSON
            completion_data = json.dumps(trip_data)
            
            query = """
                INSERT INTO trips (
                    trip_id, vehicle_id, route_id, driver_id, license_plate,
                    start_time, end_time, distance_km, duration_minutes, duration_seconds,
                    status, passenger_count, fare_amount, stops_served, total_positions,
                    completion_data
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    vehicle_id = VALUES(vehicle_id),
                    route_id = VALUES(route_id),
                    driver_id = VALUES(driver_id),
                    license_plate = VALUES(license_plate),
                    start_time = VALUES(start_time),
                    end_time = VALUES(end_time),
                    distance_km = VALUES(distance_km),
                    duration_minutes = VALUES(duration_minutes),
                    duration_seconds = VALUES(duration_seconds),
                    status = VALUES(status),
                    passenger_count = VALUES(passenger_count),
                    fare_amount = VALUES(fare_amount),
                    stops_served = VALUES(stops_served),
                    total_positions = VALUES(total_positions),
                    completion_data = VALUES(completion_data)
            """
            
            values = (
                trip_id, vehicle_id, route_id, driver_id, license_plate,
                start_time, end_time, distance_km, duration_minutes, duration_seconds,
                status, passenger_count, fare_amount, stops_served, total_positions,
                completion_data
            )
            
            self.cursor.execute(query, values)
            self.connection.commit()
            
            logger.info(f"Successfully inserted/updated trip {trip_id} in MySQL")
            return True
            
        except Error as e:
            logger.error(f"Error inserting trip completion data: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def log_trip_processing(self, trip_id: str, status: str, 
                           parquet_file_path: Optional[str] = None,
                           error_message: Optional[str] = None) -> bool:
        """
        Log trip processing status.
        
        Args:
            trip_id: Trip identifier
            status: Processing status (pending, processing, completed, failed)
            parquet_file_path: Path to the generated parquet file (optional)
            error_message: Error message if failed (optional)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = """
                INSERT INTO trip_processing_log (
                    trip_id, processing_status, parquet_file_path, error_message
                ) VALUES (%s, %s, %s, %s)
            """
            
            values = (trip_id, status, parquet_file_path, error_message)
            
            self.cursor.execute(query, values)
            self.connection.commit()
            
            logger.info(f"Logged processing status for trip {trip_id}: {status}")
            return True
            
        except Error as e:
            logger.error(f"Error logging trip processing: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def get_trip_by_id(self, trip_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve trip data by trip ID.
        
        Args:
            trip_id: Trip identifier
            
        Returns:
            Dictionary containing trip data or None
        """
        try:
            query = "SELECT * FROM trips WHERE trip_id = %s"
            self.cursor.execute(query, (trip_id,))
            result = self.cursor.fetchone()
            
            if result:
                # Parse JSON data
                if result.get('completion_data'):
                    try:
                        result['completion_data'] = json.loads(result['completion_data'])
                    except json.JSONDecodeError:
                        pass
                        
            return result
            
        except Error as e:
            logger.error(f"Error retrieving trip {trip_id}: {e}")
            return None
    
    def get_unprocessed_trips(self) -> list:
        """
        Get list of trips that haven't been processed yet.
        
        Returns:
            List of trip IDs
        """
        try:
            query = """
                SELECT t.trip_id 
                FROM trips t
                LEFT JOIN trip_processing_log l ON t.trip_id = l.trip_id
                WHERE l.id IS NULL OR l.processing_status IN ('pending', 'failed')
            """
            
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            
            return [row['trip_id'] for row in results]
            
        except Error as e:
            logger.error(f"Error retrieving unprocessed trips: {e}")
            return []
    
    @staticmethod
    def _parse_timestamp(timestamp_value) -> Optional[datetime]:
        """
        Parse various timestamp formats.
        
        Args:
            timestamp_value: Timestamp in various formats
            
        Returns:
            Datetime object or None
        """
        if not timestamp_value:
            return None
            
        if isinstance(timestamp_value, datetime):
            return timestamp_value
            
        if isinstance(timestamp_value, (int, float)):
            try:
                return datetime.fromtimestamp(timestamp_value)
            except (ValueError, OSError):
                return None
                
        if isinstance(timestamp_value, str):
            # Try common formats
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%SZ',
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(timestamp_value, fmt)
                except ValueError:
                    continue
                    
        return None
