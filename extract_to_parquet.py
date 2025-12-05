#!/usr/bin/env python3
"""
Script to extract trip data from Redis and save to Parquet files.
Works without MySQL - just focuses on extracting and storing track data.
"""

import os
import argparse
import logging
from dotenv import load_dotenv
from redis_client import RedisClient
from parquet_writer import ParquetWriter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Extract trip track data from Redis to Parquet')
    parser.add_argument('--trip-id', help='Process specific trip ID')
    parser.add_argument('--limit', type=int, default=10, help='Number of trips to process (default: 10)')
    parser.add_argument('--all', action='store_true', help='Process all trips')
    
    args = parser.parse_args()
    
    load_dotenv()
    
    # Initialize clients
    redis_client = RedisClient(
        host=os.getenv('REDIS_HOST'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        password=os.getenv('REDIS_PASSWORD') or None,
        db=int(os.getenv('REDIS_DB', 0))
    )
    
    parquet_writer = ParquetWriter(
        output_dir=os.getenv('PARQUET_OUTPUT_DIR', './output/parquet')
    )
    
    try:
        logger.info("Connecting to Redis...")
        redis_client.connect()
        
        if args.trip_id:
            # Process single trip
            process_single_trip(redis_client, parquet_writer, args.trip_id)
        else:
            # Process multiple trips
            keys = redis_client.get_trip_completion_keys()
            logger.info(f"Found {len(keys)} trips in Redis")
            
            limit = len(keys) if args.all else min(args.limit, len(keys))
            logger.info(f"Processing {limit} trips...")
            
            success_count = 0
            failed_count = 0
            
            for i, key in enumerate(keys[:limit], 1):
                trip_id = redis_client.extract_trip_id_from_key(key)
                if not trip_id:
                    continue
                    
                logger.info(f"[{i}/{limit}] Processing trip: {trip_id}")
                
                if process_single_trip(redis_client, parquet_writer, trip_id):
                    success_count += 1
                else:
                    failed_count += 1
            
            logger.info(f"\n{'='*60}")
            logger.info(f"SUMMARY: {success_count} succeeded, {failed_count} failed")
            logger.info(f"{'='*60}")
    
    finally:
        redis_client.disconnect()


def process_single_trip(redis_client, parquet_writer, trip_id):
    """Process a single trip and return success status."""
    try:
        # Get completion data
        completion_data = redis_client.get_trip_completion_data(trip_id)
        if not completion_data:
            logger.warning(f"  No completion data found for {trip_id}")
            return False
        
        logger.info(f"  Vehicle: {completion_data.get('vehicle_id')}, "
                   f"Positions: {completion_data.get('total_positions')}")
        
        # Find and read stream
        stream_key = redis_client.find_trip_stream(trip_id)
        if not stream_key:
            logger.warning(f"  No stream found for {trip_id}")
            return False
        
        track_data = redis_client.get_stream_data(stream_key)
        if not track_data:
            logger.warning(f"  No track data in stream for {trip_id}")
            return False
        
        logger.info(f"  Retrieved {len(track_data)} track points")
        
        # Write to Parquet
        parquet_path = parquet_writer.write_trip_track_data(trip_id, track_data)
        if parquet_path:
            logger.info(f"  ✅ Saved: {os.path.basename(parquet_path)}")
            return True
        else:
            logger.error(f"  ❌ Failed to write Parquet file")
            return False
            
    except Exception as e:
        logger.error(f"  ❌ Error: {e}")
        return False


if __name__ == '__main__':
    main()
