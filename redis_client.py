"""
Redis client module for retrieving trip data.
Handles connections to Redis and retrieval of trip completion data and stream data.
"""

import redis
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class RedisClient:
    """Redis client for trip data retrieval."""
    
    def __init__(self, host: str, port: int, password: str = None, db: int = 0):
        """
        Initialize Redis client.
        
        Args:
            host: Redis host
            port: Redis port
            password: Redis password (optional)
            db: Redis database number
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.client = None
        
    def connect(self):
        """Establish connection to Redis."""
        try:
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password if self.password else None,
                db=self.db,
                decode_responses=True
            )
            # Test connection
            self.client.ping()
            logger.debug(f"Successfully connected to Redis at {self.host}:{self.port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def disconnect(self):
        """Close Redis connection."""
        if self.client:
            self.client.close()
            logger.debug("Disconnected from Redis")
    
    def get_trip_completion_keys(self, pattern: str = "trip:*:*:completion") -> List[str]:
        """
        Get all trip completion keys matching the pattern.
        Pattern now includes start_date: trip:{trip_id}:{start_date}:completion
        
        Args:
            pattern: Redis key pattern to match
            
        Returns:
            List of matching keys
        """
        try:
            keys = self.client.keys(pattern)
            logger.debug(f"Found {len(keys)} trip completion keys")
            return keys
        except Exception as e:
            logger.error(f"Error retrieving trip completion keys: {e}")
            return []
    
    def get_completed_trip_status_keys(self, min_days_old: int = 0) -> List[tuple]:
        """
        Get all completed trips from trip:*:status keys that are at least N days old.
        
        Args:
            min_days_old: Only return trips completed at least this many days ago (default: 0 for all)
        
        Returns:
            List of tuples (trip_id, start_date, status_key) for completed trips
        """
        try:
            from datetime import datetime, timedelta
            
            # Find all status keys
            status_keys = self.client.keys("trip:*:*:status")
            logger.debug(f"Found {len(status_keys)} trip status keys")
            
            # Calculate cutoff date if filtering by age
            cutoff_date = None
            if min_days_old > 0:
                cutoff_date = (datetime.now() - timedelta(days=min_days_old)).strftime('%Y%m%d')
                logger.debug(f"Filtering for trips older than {min_days_old} days (before {cutoff_date})")
            
            completed_trips = []
            for key in status_keys:
                # Get the status value
                status = self.client.get(key)
                if status and status.lower() == 'completed':
                    # Extract trip_id and start_date from key: trip:{trip_id}:{start_date}:status
                    parts = key.split(':')
                    if len(parts) == 4 and parts[0] == 'trip' and parts[-1] == 'status':
                        trip_id = parts[1]
                        start_date = parts[2]
                        
                        # Apply age filter if specified
                        if cutoff_date and start_date >= cutoff_date:
                            continue  # Skip trips that are too recent
                        
                        completed_trips.append((trip_id, start_date, key))
            
            logger.debug(f"Found {len(completed_trips)} completed trips matching criteria")
            return completed_trips
        except Exception as e:
            logger.error(f"Error retrieving completed trip status keys: {e}")
            return []
    
    def get_trip_completion_data(self, trip_id: str, start_date: str) -> Optional[Dict[str, Any]]:
        """
        Get trip completion data for a specific trip.
        Handles both string and hash data types.
        
        Args:
            trip_id: Trip identifier
            start_date: Trip start date (format: YYYYMMDD)
            
        Returns:
            Dictionary containing trip completion data or None
        """
        key = f"trip:{trip_id}:{start_date}:completion"
        try:
            # Check the type of the key
            key_type = self.client.type(key)
            
            if key_type == 'hash':
                # It's a hash, use HGETALL
                data = self.client.hgetall(key)
                return data if data else None
            elif key_type == 'string':
                # It's a string, use GET
                data = self.client.get(key)
                if data:
                    try:
                        return json.loads(data)
                    except json.JSONDecodeError:
                        return {"raw_data": data}
                return None
            else:
                logger.warning(f"No data found or unexpected type for key: {key}")
                return None
        except Exception as e:
            logger.error(f"Error retrieving trip completion data for {trip_id}: {e}")
            return None
    
    def get_trip_completion_data_by_key(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get trip completion data by full key.
        Handles both string and hash data types.
        
        Args:
            key: Full Redis key
            
        Returns:
            Dictionary containing trip completion data or None
        """
        try:
            # Check the type of the key
            key_type = self.client.type(key)
            
            if key_type == 'hash':
                # It's a hash, use HGETALL
                data = self.client.hgetall(key)
                return data if data else None
            elif key_type == 'string':
                # It's a string, use GET
                data = self.client.get(key)
                if data:
                    try:
                        return json.loads(data)
                    except json.JSONDecodeError:
                        return {"raw_data": data}
            else:
                logger.warning(f"Unexpected key type '{key_type}' for key {key}")
            
            return None
        except Exception as e:
            logger.error(f"Error retrieving data for key {key}: {e}")
            return None
    
    def get_stream_data(self, stream_key: str, start_id: str = '0', 
                       end_id: str = '+', count: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get data from a Redis stream.
        
        Args:
            stream_key: Name of the Redis stream
            start_id: Starting message ID (default: '0' for beginning)
            end_id: Ending message ID (default: '+' for end)
            count: Maximum number of messages to retrieve (optional)
            
        Returns:
            List of stream entries with their data
        """
        try:
            if count:
                messages = self.client.xrange(stream_key, min=start_id, max=end_id, count=count)
            else:
                messages = self.client.xrange(stream_key, min=start_id, max=end_id)
            
            logger.debug(f"Retrieved {len(messages)} messages from stream {stream_key}")
            
            # Format the data
            formatted_data = []
            for msg_id, msg_data in messages:
                formatted_data.append({
                    'message_id': msg_id,
                    'timestamp': self._extract_timestamp_from_id(msg_id),
                    'data': msg_data
                })
            
            return formatted_data
        except Exception as e:
            logger.error(f"Error retrieving stream data from {stream_key}: {e}")
            return []
    
    def find_trip_stream(self, trip_id: str, start_date: str) -> Optional[str]:
        """
        Find the stream key for a specific trip.
        Common patterns: trip:{trip_id}:{start_date}:track, trip:{trip_id}:{start_date}:stream, etc.
        
        Args:
            trip_id: Trip identifier
            start_date: Trip start date (format: YYYYMMDD)
            
        Returns:
            Stream key if found, None otherwise
        """
        patterns = [
            f"trip:{trip_id}:{start_date}:track",
            f"trip:{trip_id}:{start_date}:stream",
            f"trip:{trip_id}:{start_date}:location",
            f"trip:{trip_id}:{start_date}:gps"
        ]
        
        for pattern in patterns:
            if self.client.exists(pattern):
                stream_type = self.client.type(pattern)
                if stream_type == 'stream':
                    logger.debug(f"Found stream for trip {trip_id}: {pattern}")
                    return pattern
        
        # Try searching with wildcard
        keys = self.client.keys(f"*{trip_id}*")
        for key in keys:
            if self.client.type(key) == 'stream':
                logger.debug(f"Found stream for trip {trip_id}: {key}")
                return key
        
        logger.warning(f"No stream found for trip {trip_id}")
        return None
    
    @staticmethod
    def _extract_timestamp_from_id(message_id: str) -> Optional[datetime]:
        """
        Extract timestamp from Redis stream message ID.
        Message ID format: <millisecondsTime>-<sequenceNumber>
        
        Args:
            message_id: Redis stream message ID
            
        Returns:
            Datetime object or None
        """
        try:
            timestamp_ms = int(message_id.split('-')[0])
            return datetime.fromtimestamp(timestamp_ms / 1000.0)
        except (ValueError, IndexError):
            return None
    
    def delete_trip_data(self, trip_id: str, start_date: str) -> bool:
        """
        Delete all Redis data for a trip (completion hash, track stream, and status).
        
        Args:
            trip_id: Trip identifier
            start_date: Trip start date (format: YYYYMMDD)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            deleted_count = 0
            
            # Delete completion hash
            completion_key = f"trip:{trip_id}:{start_date}:completion"
            if self.client.exists(completion_key):
                self.client.delete(completion_key)
                deleted_count += 1
                logger.debug(f"Deleted completion key: {completion_key}")
            
            # Delete status key
            status_key = f"trip:{trip_id}:{start_date}:status"
            if self.client.exists(status_key):
                self.client.delete(status_key)
                deleted_count += 1
                logger.debug(f"Deleted status key: {status_key}")
            
            # Find and delete stream
            stream_key = self.find_trip_stream(trip_id, start_date)
            if stream_key:
                self.client.delete(stream_key)
                deleted_count += 1
                logger.debug(f"Deleted stream key: {stream_key}")
            
            if deleted_count > 0:
                logger.debug(f"Deleted {deleted_count} Redis keys for trip {trip_id}")
                return True
            else:
                logger.warning(f"No Redis keys found to delete for trip {trip_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting trip data for {trip_id}: {e}")
            return False
    
    def extract_trip_id_from_key(self, key: str) -> Optional[tuple]:
        """
        Extract trip ID and start_date from a completion key.
        
        Args:
            key: Redis key (e.g., trip:12345:20250901:completion)
            
        Returns:
            Tuple of (trip_id, start_date) or None
        """
        try:
            parts = key.split(':')
            if len(parts) == 4 and parts[0] == 'trip' and parts[-1] == 'completion':
                return (parts[1], parts[2])
            return None
        except Exception:
            return None
