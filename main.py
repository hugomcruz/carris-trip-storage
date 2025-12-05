#!/usr/bin/env python3
"""
Main script to process trip data from Redis.
Extracts trip completion data to MySQL and trip track data to Parquet files.
"""

import os
import sys
import argparse
import logging
from dotenv import load_dotenv
from typing import List, Optional

from redis_client import RedisClient
from postgres_client import PostgreSQLClient
from parquet_writer import ParquetWriter
from s3_client import S3Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
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
    
    def __init__(self):
        """Initialize the trip processor with configuration from environment."""
        load_dotenv()
        
        # Initialize clients
        self.redis_client = RedisClient(
            host=os.getenv('REDIS_HOST'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            password=os.getenv('REDIS_PASSWORD') or None,
            db=int(os.getenv('REDIS_DB', 0))
        )
        
        self.postgres_client = PostgreSQLClient(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', ''),
            database=os.getenv('POSTGRES_DATABASE', 'trip_data')
        )
        
        self.parquet_writer = ParquetWriter(
            output_dir=os.getenv('PARQUET_OUTPUT_DIR', './output/parquet')
        )
        
        # Initialize S3 client (optional)
        s3_endpoint = os.getenv('S3_ENDPOINT_URL', '')
        s3_access_key = os.getenv('S3_ACCESS_KEY_ID', '')
        s3_secret_key = os.getenv('S3_SECRET_ACCESS_KEY', '')
        s3_bucket = os.getenv('S3_BUCKET_NAME', '')
        s3_region = os.getenv('S3_REGION', 'us-east-1')
        
        # Only initialize S3 client if credentials are provided
        self.s3_enabled = bool(s3_access_key and s3_secret_key and s3_bucket)
        if self.s3_enabled:
            self.s3_client = S3Client(
                endpoint_url=s3_endpoint,
                access_key_id=s3_access_key,
                secret_access_key=s3_secret_key,
                bucket_name=s3_bucket,
                region=s3_region
            )
        else:
            self.s3_client = None
            logger.info("S3 upload disabled (no credentials configured)")
        
    def connect(self):
        """Connect to Redis, PostgreSQL, and optionally S3."""
        logger.info("Connecting to Redis and PostgreSQL...")
        self.redis_client.connect()
        self.postgres_client.connect()
        if self.s3_enabled:
            logger.info("Connecting to S3...")
            self.s3_client.connect()
        logger.info("Successfully connected to all services")
        
    def disconnect(self):
        """Disconnect from Redis, PostgreSQL, and S3."""
        logger.info("Disconnecting from services...")
        self.redis_client.disconnect()
        self.postgres_client.disconnect()
        if self.s3_enabled:
            self.s3_client.disconnect()
        logger.info("Disconnected from all services")
        
    def process_trip(self, trip_id: str) -> bool:
        """
        Process a single trip: extract completion data and track data.
        
        Args:
            trip_id: Trip identifier
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(f"Processing trip: {trip_id}")
        
        # Log processing start
        self.postgres_client.log_trip_processing(trip_id, 'processing')
        
        try:
            # 1. Get and store trip completion data
            completion_data = self.redis_client.get_trip_completion_data(trip_id)
            
            if not completion_data:
                logger.warning(f"No completion data found for trip {trip_id}")
                self.postgres_client.log_trip_processing(
                    trip_id, 'failed', 
                    error_message='No completion data found'
                )
                return False
            
            # Ensure trip_id is in the data
            if 'trip_id' not in completion_data and 'id' not in completion_data:
                completion_data['trip_id'] = trip_id
            
            # Store in PostgreSQL
            success = self.postgres_client.insert_trip_completion(completion_data)
            if not success:
                logger.error(f"Failed to store completion data for trip {trip_id}")
                self.postgres_client.log_trip_processing(
                    trip_id, 'failed',
                    error_message='Failed to store completion data'
                )
                return False
            
            logger.info(f"Stored completion data for trip {trip_id} in PostgreSQL")
            
            # 2. Get and store trip track data from stream
            stream_key = self.redis_client.find_trip_stream(trip_id)
            
            if not stream_key:
                logger.warning(f"No stream found for trip {trip_id}")
                # Mark as completed even without stream data
                self.postgres_client.log_trip_processing(
                    trip_id, 'completed',
                    error_message='No stream data found'
                )
                return True
            
            # Get stream data
            track_data = self.redis_client.get_stream_data(stream_key)
            
            if not track_data:
                logger.warning(f"No track data in stream {stream_key}")
                self.postgres_client.log_trip_processing(
                    trip_id, 'completed',
                    error_message='Stream exists but no data found'
                )
                return True
            
            # Write to Parquet
            parquet_path = self.parquet_writer.write_trip_track_data(trip_id, track_data)
            
            if not parquet_path:
                logger.error(f"Failed to write Parquet file for trip {trip_id}")
                self.postgres_client.log_trip_processing(
                    trip_id, 'failed',
                    error_message='Failed to write Parquet file'
                )
                return False
            
            logger.info(f"Stored track data for trip {trip_id} in {parquet_path}")
            
            # Upload to S3 if enabled
            s3_path = None
            if self.s3_enabled:
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
                    
                    s3_path = self.s3_client.upload_parquet_file(parquet_path, trip_date)
                    logger.info(f"Uploaded track data to S3: {s3_path}")
                    
                    # Delete local file after successful upload
                    try:
                        os.remove(parquet_path)
                        logger.info(f"Deleted local file: {parquet_path}")
                    except Exception as e:
                        logger.warning(f"Failed to delete local file {parquet_path}: {e}")
                        
                except Exception as e:
                    logger.error(f"Failed to upload to S3: {e}")
                    # If S3 upload fails, keep the local file
            
            # Log successful processing
            self.postgres_client.log_trip_processing(
                trip_id, 'completed',
                parquet_file_path=s3_path if s3_path else parquet_path
            )
            
            # Delete trip data from Redis after successful processing
            logger.info(f"Deleting Redis data for trip {trip_id}")
            if self.redis_client.delete_trip_data(trip_id):
                logger.info(f"Successfully deleted Redis data for trip {trip_id}")
            else:
                logger.warning(f"Failed to delete some Redis data for trip {trip_id}")
            
            logger.info(f"Successfully processed trip {trip_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing trip {trip_id}: {e}", exc_info=True)
            self.postgres_client.log_trip_processing(
                trip_id, 'failed',
                error_message=str(e)
            )
            return False
    
    def process_all_trips(self) -> dict:
        """
        Process all trips found in Redis.
        
        Returns:
            Dictionary with processing statistics
        """
        logger.info("Starting to process all trips...")
        
        # Get all completion keys
        keys = self.redis_client.get_trip_completion_keys()
        
        if not keys:
            logger.warning("No trip completion keys found in Redis")
            return {'total': 0, 'success': 0, 'failed': 0}
        
        stats = {
            'total': len(keys),
            'success': 0,
            'failed': 0
        }
        
        for key in keys:
            # Extract trip ID from key
            trip_id = self.redis_client.extract_trip_id_from_key(key)
            
            if not trip_id:
                logger.warning(f"Could not extract trip ID from key: {key}")
                stats['failed'] += 1
                continue
            
            # Process the trip
            if self.process_trip(trip_id):
                stats['success'] += 1
            else:
                stats['failed'] += 1
        
        logger.info(f"Processing complete. Total: {stats['total']}, "
                   f"Success: {stats['success']}, Failed: {stats['failed']}")
        
        return stats
    
    def list_trips_in_redis(self) -> List[str]:
        """
        List all trip IDs available in Redis.
        
        Returns:
            List of trip IDs
        """
        keys = self.redis_client.get_trip_completion_keys()
        trip_ids = []
        
        for key in keys:
            trip_id = self.redis_client.extract_trip_id_from_key(key)
            if trip_id:
                trip_ids.append(trip_id)
        
        return trip_ids
    
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
        description='Process trip data from Redis to MySQL and Parquet files'
    )
    
    parser.add_argument(
        '--trip-id',
        type=str,
        help='Process a specific trip ID'
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
        '--all',
        action='store_true',
        help='Process all trips found in Redis'
    )
    
    args = parser.parse_args()
    
    # Create processor
    processor = TripProcessor()
    
    try:
        # Connect to services
        processor.connect()
        
        # Execute requested action
        if args.list:
            trip_ids = processor.list_trips_in_redis()
            print(f"\nFound {len(trip_ids)} trips in Redis:")
            for trip_id in trip_ids:
                print(f"  - {trip_id}")
                
        elif args.info:
            processor.show_trip_info(args.info)
            
        elif args.trip_id:
            success = processor.process_trip(args.trip_id)
            sys.exit(0 if success else 1)
            
        elif args.all:
            stats = processor.process_all_trips()
            print(f"\nProcessing Summary:")
            print(f"  Total trips: {stats['total']}")
            print(f"  Successful: {stats['success']}")
            print(f"  Failed: {stats['failed']}")
            sys.exit(0 if stats['failed'] == 0 else 1)
            
        else:
            # Default: process all trips
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
