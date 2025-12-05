"""
PostgreSQL client module for storing trip data.
Handles connections to PostgreSQL and insertion of trip completion data.
"""

import psycopg2
from psycopg2 import Error
from psycopg2.extras import RealDictCursor
import json
import logging
from typing import Dict, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class PostgreSQLClient:
    """PostgreSQL client for trip data storage."""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        """
        Initialize PostgreSQL client.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            user: PostgreSQL user
            password: PostgreSQL password
            database: PostgreSQL database name
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to PostgreSQL."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            logger.debug(f"Successfully connected to PostgreSQL at {self.host}:{self.port}/{self.database}")
        except Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def disconnect(self):
        """Close PostgreSQL connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.debug("Disconnected from PostgreSQL")
    
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
            start_date = trip_data.get('start_date')
            service_date = trip_data.get('service_date')
            vehicle_id = trip_data.get('vehicle_id')
            route_id = trip_data.get('route_id')
            route_short_name = trip_data.get('route_short_name')
            route_long_name = trip_data.get('route_long_name')
            license_plate = trip_data.get('license_plate')
            
            # Handle timestamps - convert unix timestamp to datetime
            start_time = self._parse_unix_timestamp(trip_data.get('start_time'))
            end_time = self._parse_unix_timestamp(trip_data.get('end_time'))
            
            # Handle TIME fields - convert empty strings to None
            scheduled_start_time = trip_data.get('scheduled_start_time')
            if scheduled_start_time == '':
                scheduled_start_time = None
            
            scheduled_end_time = trip_data.get('scheduled_end_time')
            if scheduled_end_time == '':
                scheduled_end_time = None
            
            completed_at = self._parse_timestamp(trip_data.get('completed_at'))
            
            # Extract numeric values
            duration_seconds = trip_data.get('duration_seconds')
            status = trip_data.get('status', 'completed')
            stops_served = trip_data.get('stops_served')
            total_positions = trip_data.get('total_positions')
            
            # Insert without storing JSON
            query = """
                INSERT INTO trips (
                    trip_id, start_date, service_date, vehicle_id, route_id, route_short_name, 
                    route_long_name, license_plate, start_time, end_time, 
                    scheduled_start_time, scheduled_end_time, completed_at,
                    duration_seconds, status, stops_served, total_positions
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (trip_id) DO UPDATE SET
                    start_date = EXCLUDED.start_date,
                    service_date = EXCLUDED.service_date,
                    vehicle_id = EXCLUDED.vehicle_id,
                    route_id = EXCLUDED.route_id,
                    route_short_name = EXCLUDED.route_short_name,
                    route_long_name = EXCLUDED.route_long_name,
                    license_plate = EXCLUDED.license_plate,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    scheduled_start_time = EXCLUDED.scheduled_start_time,
                    scheduled_end_time = EXCLUDED.scheduled_end_time,
                    completed_at = EXCLUDED.completed_at,
                    duration_seconds = EXCLUDED.duration_seconds,
                    status = EXCLUDED.status,
                    stops_served = EXCLUDED.stops_served,
                    total_positions = EXCLUDED.total_positions,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            values = (
                trip_id, start_date, service_date, vehicle_id, route_id, route_short_name,
                route_long_name, license_plate, start_time, end_time,
                scheduled_start_time, scheduled_end_time, completed_at,
                duration_seconds, status, stops_served, total_positions
            )
            
            self.cursor.execute(query, values)
            self.connection.commit()
            
            logger.debug(f"Successfully inserted/updated trip {trip_id} in PostgreSQL")
            return True
            
        except Error as e:
            logger.error(f"Error inserting trip completion data: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def log_trip_processing(self, trip_id: str, start_date: str, status: str, 
                           parquet_file_path: Optional[str] = None,
                           error_message: Optional[str] = None) -> bool:
        """
        Log trip processing status.
        Only logs 'completed' or 'failed' status per trip per start_date.
        
        Args:
            trip_id: Trip identifier
            start_date: Trip start date (format: YYYYMMDD)
            status: Processing status (completed or failed)
            parquet_file_path: Path to the generated parquet file (optional)
            error_message: Error message if failed (optional)
            
        Returns:
            True if successful, False otherwise
        """
        # Only log completed or failed status
        if status not in ('completed', 'failed'):
            return True
            
        try:
            query = """
                INSERT INTO trip_processing_log (
                    trip_id, start_date, processing_status, parquet_file_path, error_message
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (trip_id, start_date) DO UPDATE SET
                    processing_status = EXCLUDED.processing_status,
                    parquet_file_path = EXCLUDED.parquet_file_path,
                    error_message = EXCLUDED.error_message,
                    processed_at = CURRENT_TIMESTAMP
            """
            
            values = (trip_id, start_date, status, parquet_file_path, error_message)
            
            self.cursor.execute(query, values)
            self.connection.commit()
            
            logger.debug(f"Logged processing status for trip {trip_id} ({start_date}): {status}")
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
                result = dict(result)
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
    def _parse_unix_timestamp(timestamp_value) -> Optional[datetime]:
        """
        Parse unix timestamp (seconds since epoch) to datetime.
        
        Args:
            timestamp_value: Unix timestamp as string, int, or float
            
        Returns:
            Datetime object or None
        """
        if not timestamp_value:
            return None
            
        try:
            # Convert to int if string
            if isinstance(timestamp_value, str):
                timestamp_value = int(timestamp_value)
            
            # Convert unix timestamp to datetime
            return datetime.fromtimestamp(timestamp_value)
        except (ValueError, OSError, TypeError):
            return None
    
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
