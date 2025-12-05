#!/usr/bin/env python3
"""
Main script to process trip data from Redis.
Extracts trip completion data to MySQL and trip track data to Parquet files.
Supports multithreaded processing for improved performance.
"""

import os
import sys
import argparse
import logging
from dotenv import load_dotenv
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from redis_client import RedisClient
from postgres_client import PostgreSQLClient
from parquet_writer import ParquetWriter
from s3_client import S3Client

# Load environment variables first
load_dotenv()

# Configure logging with level from environment
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('trip_processing.log')
    ],
    force=True  # Force reconfiguration to override any previous settings
)
logger = logging.getLogger(__name__)


class TripProcessor:
    """Main processor for trip data extraction and storage."""
    
    def __init__(self, max_workers: int = None, archive_days_threshold: int = None):
        """
        Initialize the trip processor with configuration from environment.
        
        Args:
            max_workers: Maximum number of concurrent threads (defaults to env var or 5)
            archive_days_threshold: Number of days after which to archive trips (defaults to env var or 7)
        """
        load_dotenv()
        
        self.max_workers = max_workers or int(os.getenv('MAX_WORKERS', 5))
        self.archive_days_threshold = archive_days_threshold or int(os.getenv('ARCHIVE_DAYS_THRESHOLD', 7))
        self.stats_lock = threading.Lock()
        
        logger.info(f"Initialized TripProcessor with max_workers={self.max_workers}, "
                   f"archive_days_threshold={self.archive_days_threshold} days")
        
        # Store configuration for thread-local clients
        self.redis_config = {
            'host': os.getenv('REDIS_HOST'),
            'port': int(os.getenv('REDIS_PORT', 6379)),
            'password': os.getenv('REDIS_PASSWORD') or None,
            'db': int(os.getenv('REDIS_DB', 0))
        }
        
        self.postgres_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', ''),
            'database': os.getenv('POSTGRES_DATABASE', 'trip_data')
        }
        
        self.s3_config = {
            'endpoint_url': os.getenv('S3_ENDPOINT_URL', ''),
            'access_key_id': os.getenv('S3_ACCESS_KEY_ID', ''),
            'secret_access_key': os.getenv('S3_SECRET_ACCESS_KEY', ''),
            'bucket_name': os.getenv('S3_BUCKET_NAME', ''),
            'region': os.getenv('S3_REGION', 'us-east-1')
        }
        
        self.parquet_output_dir = os.getenv('PARQUET_OUTPUT_DIR', './output/parquet')
        
        # Check if S3 is enabled
        self.s3_enabled = bool(
            self.s3_config['access_key_id'] and 
            self.s3_config['secret_access_key'] and 
            self.s3_config['bucket_name']
        )
        
        if not self.s3_enabled:
            logger.info("S3 upload disabled (no credentials configured)")
        
        # Initialize main Redis client for listing trips
        self.redis_client = RedisClient(**self.redis_config)
        
        logger.info(f"Initialized TripProcessor with max_workers={max_workers}")
        
    def connect(self):
        """Connect to Redis for listing trips."""
        logger.debug("Connecting to Redis...")
        self.redis_client.connect()
        logger.debug("Successfully connected to Redis")
        
        # Clean up any leftover Parquet files from previous runs
        logger.debug("Cleaning up leftover Parquet files...")
        temp_writer = ParquetWriter(output_dir=self.parquet_output_dir)
        temp_writer.cleanup_output_dir()
        
    def disconnect(self):
        """Disconnect from Redis."""
        logger.debug("Disconnecting from Redis...")
        self.redis_client.disconnect()
        logger.debug("Disconnected from Redis")
    
    def _create_thread_clients(self):
        """
        Create thread-local clients for Redis, PostgreSQL, S3, and Parquet writer.
        Each thread gets its own connections to avoid threading issues.
        
        Returns:
            Tuple of (redis_client, postgres_client, parquet_writer, s3_client)
        """
        # Create thread-local Redis client
        redis_client = RedisClient(**self.redis_config)
        redis_client.connect()
        
        # Create thread-local PostgreSQL client
        postgres_client = PostgreSQLClient(**self.postgres_config)
        postgres_client.connect()
        
        # Create thread-local Parquet writer
        parquet_writer = ParquetWriter(output_dir=self.parquet_output_dir)
        
        # Create thread-local S3 client if enabled
        s3_client = None
        if self.s3_enabled:
            s3_client = S3Client(**self.s3_config)
            s3_client.connect()
        
        return redis_client, postgres_client, parquet_writer, s3_client
    
    def _cleanup_thread_clients(self, redis_client, postgres_client, s3_client):
        """
        Clean up thread-local clients.
        
        Args:
            redis_client: Redis client to disconnect
            postgres_client: PostgreSQL client to disconnect
            s3_client: S3 client to disconnect (can be None)
        """
        try:
            redis_client.disconnect()
        except Exception as e:
            logger.warning(f"Error disconnecting Redis client: {e}")
        
        try:
            postgres_client.disconnect()
        except Exception as e:
            logger.warning(f"Error disconnecting PostgreSQL client: {e}")
        
        if s3_client:
            try:
                s3_client.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting S3 client: {e}")
        
    def process_trip(self, trip_id: str, start_date: str, 
                    redis_client=None, postgres_client=None, 
                    parquet_writer=None, s3_client=None) -> bool:
        """
        Process a single trip: extract completion data and track data.
        Uses provided thread-local clients or creates new ones if not provided.
        
        Args:
            trip_id: Trip identifier
            start_date: Trip start date (format: YYYYMMDD) - required
            redis_client: Thread-local Redis client (optional)
            postgres_client: Thread-local PostgreSQL client (optional)
            parquet_writer: Thread-local Parquet writer (optional)
            s3_client: Thread-local S3 client (optional)
            
        Returns:
            True if successful, False otherwise
        """
        logger.debug(f"Processing trip: {trip_id} (start_date: {start_date})")
        
        try:
            # Get and store trip completion data
            completion_data = redis_client.get_trip_completion_data(trip_id, start_date)
            
            if not completion_data:
                logger.warning(f"No completion data found for trip {trip_id}")
                postgres_client.log_trip_processing(
                    trip_id, start_date, 'failed', 
                    error_message='No completion data found'
                )
                return False
            
            # Ensure trip_id and start_date are in the data
            if 'trip_id' not in completion_data and 'id' not in completion_data:
                completion_data['trip_id'] = trip_id
            if 'start_date' not in completion_data:
                completion_data['start_date'] = start_date
            
            # Store in PostgreSQL
            success = postgres_client.insert_trip_completion(completion_data)
            if not success:
                logger.error(f"Failed to store completion data for trip {trip_id}")
                postgres_client.log_trip_processing(
                    trip_id, start_date, 'failed',
                    error_message='Failed to store completion data'
                )
                return False
            
            logger.debug(f"Stored completion data for trip {trip_id} in PostgreSQL")
            
            # 2. Get and store trip track data from stream
            stream_key = redis_client.find_trip_stream(trip_id, start_date)
            
            if not stream_key:
                logger.debug(f"No stream found for trip {trip_id}")
                # Mark as completed even without stream data
                postgres_client.log_trip_processing(
                    trip_id, start_date, 'completed',
                    error_message='No stream data found'
                )
                return True
            
            # Get stream data
            track_data = redis_client.get_stream_data(stream_key)
            
            if not track_data:
                logger.debug(f"No track data in stream {stream_key}")
                postgres_client.log_trip_processing(
                    trip_id, start_date, 'completed',
                    error_message='Stream exists but no data found'
                )
                return True
            
            # Write to Parquet
            parquet_path = parquet_writer.write_trip_track_data(trip_id, track_data, start_date)
            
            if not parquet_path:
                logger.error(f"Failed to write Parquet file for trip {trip_id}")
                postgres_client.log_trip_processing(
                    trip_id, start_date, 'failed',
                    error_message='Failed to write Parquet file'
                )
                return False
            
            logger.debug(f"Stored track data for trip {trip_id} in {parquet_path}")
            
            # Upload to S3 if enabled
            s3_path = None
            s3_upload_success = False
            if s3_client:
                try:
                    # Extract trip date from completion data for S3 folder structure
                    from datetime import datetime
                    trip_date = None
                    if 'start_time' in completion_data:
                        try:
                            trip_date = datetime.fromisoformat(str(completion_data['start_time']).replace('Z', '+00:00'))
                        except:
                            pass
                    elif 'end_time' in completion_data:
                        try:
                            trip_date = datetime.fromisoformat(str(completion_data['end_time']).replace('Z', '+00:00'))
                        except:
                            pass
                    
                    s3_path = s3_client.upload_parquet_file(parquet_path, trip_date)
                    logger.debug(f"Uploaded track data to S3: {s3_path}")
                    s3_upload_success = True
                    
                    # Delete local file after successful upload
                    try:
                        os.remove(parquet_path)
                        logger.debug(f"Deleted local file: {parquet_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete local file {parquet_path}: {e}")
                        
                except Exception as e:
                    logger.error(f"Failed to upload to S3: {e}")
                    # If S3 upload fails, keep the local file and Redis data
                    postgres_client.log_trip_processing(
                        trip_id, start_date, 'failed',
                        error_message=f'S3 upload failed: {str(e)}',
                        parquet_file_path=parquet_path
                    )
                    return False
            
            # Log successful processing
            postgres_client.log_trip_processing(
                trip_id, start_date, 'completed',
                parquet_file_path=s3_path if s3_path else parquet_path
            )
            
            # Only delete Redis data after successful S3 upload (if S3 is enabled)
            # or if S3 is not enabled and local parquet was created successfully
            if s3_upload_success or not s3_client:
                logger.debug(f"Deleting Redis data for trip {trip_id} (start_date: {start_date})")
                if redis_client.delete_trip_data(trip_id, start_date):
                    logger.debug(f"Successfully deleted Redis data for trip {trip_id}")
                else:
                    logger.warning(f"Failed to delete some Redis data for trip {trip_id}")
            else:
                logger.debug(f"Keeping Redis data for trip {trip_id} due to S3 upload failure")
            
            logger.info(f"✓ Processed trip {trip_id} (start_date: {start_date})")
            return True
            
        except Exception as e:
            logger.error(f"Error processing trip {trip_id}: {e}", exc_info=True)
            postgres_client.log_trip_processing(
                trip_id, start_date, 'failed',
                error_message=str(e)
            )
            return False
    
    def _process_trip_worker(self, trip_info: tuple) -> dict:
        """
        Worker function to process a single trip in a thread.
        Creates thread-local clients and processes the trip.
        
        Args:
            trip_info: Tuple of (trip_id, start_date, status_key)
            
        Returns:
            Dict with trip_id, start_date, and success status
        """
        trip_id, start_date, status_key = trip_info
        
        # Create thread-local clients
        redis_client, postgres_client, parquet_writer, s3_client = self._create_thread_clients()
        
        try:
            # Process the trip with thread-local clients
            success = self.process_trip(
                trip_id, start_date, 
                redis_client, postgres_client, 
                parquet_writer, s3_client
            )
            
            return {
                'trip_id': trip_id,
                'start_date': start_date,
                'success': success
            }
        finally:
            # Always clean up thread-local clients
            self._cleanup_thread_clients(redis_client, postgres_client, s3_client)
    
    def process_all_trips(self) -> dict:
        """
        Process all completed trips found in Redis status keys using multithreading.
        Only processes trips older than the configured archive_days_threshold.
        
        Returns:
            Dictionary with processing statistics
        """
        logger.info(f"Starting to process completed trips older than {self.archive_days_threshold} days "
                   f"with {self.max_workers} workers...")
        
        # Get all completed trips from status keys, filtered by age
        completed_trips = self.redis_client.get_completed_trip_status_keys(
            min_days_old=self.archive_days_threshold
        )
        
        if not completed_trips:
            logger.warning("No completed trips found in Redis status keys")
            return {'total': 0, 'success': 0, 'failed': 0}
        
        stats = {
            'total': len(completed_trips),
            'success': 0,
            'failed': 0
        }
        
        logger.info(f"Found {stats['total']} completed trips to process")
        
        # Process trips in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="TripWorker") as executor:
            # Submit all trips for processing
            future_to_trip = {
                executor.submit(self._process_trip_worker, trip_info): trip_info 
                for trip_info in completed_trips
            }
            
            # Process completed futures as they finish
            for future in as_completed(future_to_trip):
                trip_info = future_to_trip[future]
                trip_id, start_date, _ = trip_info
                
                try:
                    result = future.result()
                    
                    if result['success']:
                        with self.stats_lock:
                            stats['success'] += 1
                        logger.debug(f"Worker completed trip {trip_id} [{stats['success']}/{stats['total']}]")
                    else:
                        with self.stats_lock:
                            stats['failed'] += 1
                        logger.error(f"✗ Failed to process trip {trip_id} [{stats['failed']} failures]")
                        
                except Exception as e:
                    with self.stats_lock:
                        stats['failed'] += 1
                    logger.error(f"✗ Exception processing trip {trip_id}: {e}", exc_info=True)
        
        logger.info(f"\nProcessing complete:")
        logger.info(f"  Total: {stats['total']}")
        logger.info(f"  Success: {stats['success']}")
        logger.info(f"  Failed: {stats['failed']}")
        
        return stats
    
    def list_trips_in_redis(self) -> List[tuple]:
        """
        List all trip IDs and start_dates available in Redis.
        
        Returns:
            List of tuples (trip_id, start_date)
        """
        keys = self.redis_client.get_trip_completion_keys()
        trips = []
        
        for key in keys:
            result = self.redis_client.extract_trip_id_from_key(key)
            if result:
                trips.append(result)
        
        return trips
    
    def show_trip_info(self, trip_id: str):
        """
        Display information about a specific trip.
        
        Args:
            trip_id: Trip identifier
        """
        logger.info(f"Retrieving information for trip {trip_id}")
        
        # Get from PostgreSQL
        trip_data = self.postgres_client.get_trip_by_id(trip_id)
        
        if trip_data:
            print("\n=== Trip Information (PostgreSQL) ===")
            for key, value in trip_data.items():
                if key != 'completion_data':
                    print(f"{key}: {value}")
        else:
            print(f"No trip data found in MySQL for trip {trip_id}")
        
        # Check for Parquet files
        files = self.parquet_writer.list_trip_files(trip_id)
        
        if files:
            print(f"\n=== Parquet Files ({len(files)}) ===")
            for file in files:
                info = self.parquet_writer.get_file_info(file)
                if info:
                    print(f"\nFile: {info['filename']}")
                    print(f"  Rows: {info['row_count']}")
                    print(f"  Columns: {info['column_count']}")
                    print(f"  Size: {info['size_bytes']} bytes")
                    print(f"  Created: {info['created']}")
        else:
            print(f"\nNo Parquet files found for trip {trip_id}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Archive trip data from Redis to PostgreSQL and S3 with multithreading support'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available trips in Redis'
    )
    
    parser.add_argument(
        '--info',
        type=str,
        metavar='TRIP_ID',
        help='Show information about a specific trip'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        help=f'Number of concurrent worker threads (default: from env MAX_WORKERS or 5)'
    )
    
    parser.add_argument(
        '--days',
        type=int,
        help='Archive trips older than N days (default: from env ARCHIVE_DAYS_THRESHOLD or 7)'
    )
    
    args = parser.parse_args()
    
    # Create processor with specified or default values
    processor = TripProcessor(max_workers=args.workers, archive_days_threshold=args.days)
    
    try:
        # Connect to services
        processor.connect()
        
        # Execute requested action
        if args.list:
            trips = processor.list_trips_in_redis()
            print(f"\nFound {len(trips)} trips in Redis:")
            for trip_id, start_date in trips:
                print(f"  - {trip_id} (start_date: {start_date})")
                
        elif args.info:
            processor.show_trip_info(args.info)
            
        else:
            # Default action: process all completed trips older than threshold
            stats = processor.process_all_trips()
            print(f"\nProcessing Summary:")
            print(f"  Total trips: {stats['total']}")
            print(f"  Successful: {stats['success']}")
            print(f"  Failed: {stats['failed']}")
            sys.exit(0 if stats['failed'] == 0 else 1)
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
        
    finally:
        # Always disconnect
        processor.disconnect()


if __name__ == '__main__':
    main()
