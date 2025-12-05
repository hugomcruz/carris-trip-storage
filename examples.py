#!/usr/bin/env python3
"""
Example script demonstrating how to use the trip storage system.
"""

from redis_client import RedisClient
from mysql_client import MySQLClient
from parquet_writer import ParquetWriter
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def example_1_process_single_trip():
    """Example: Process a single trip."""
    print("=== Example 1: Processing a single trip ===\n")
    
    # Initialize clients
    redis = RedisClient(
        host=os.getenv('REDIS_HOST'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        password=os.getenv('REDIS_PASSWORD') or None,
        db=int(os.getenv('REDIS_DB', 0))
    )
    
    mysql = MySQLClient(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'trip_data')
    )
    
    parquet = ParquetWriter(
        output_dir=os.getenv('PARQUET_OUTPUT_DIR', './output/parquet')
    )
    
    try:
        # Connect
        redis.connect()
        mysql.connect()
        
        # Get first trip
        keys = redis.get_trip_completion_keys()
        if not keys:
            print("No trips found in Redis")
            return
        
        result = redis.extract_trip_id_from_key(keys[0])
        if not result:
            print("Could not extract trip info from key")
            return
        
        trip_id, start_date = result
        print(f"Processing trip: {trip_id} (start_date: {start_date})\\n")
        
        # Get completion data
        completion = redis.get_trip_completion_data_by_key(keys[0])
        print(f"Completion data: {completion}\\n")
        
        # Store in MySQL
        if completion:
            if 'trip_id' not in completion:
                completion['trip_id'] = trip_id
            mysql.insert_trip_completion(completion)
            print("Stored in MySQL ✓\n")
        
        # Get stream data
        stream_key = redis.find_trip_stream(trip_id, start_date)
        if stream_key:
            print(f"Found stream: {stream_key}")
            track_data = redis.get_stream_data(stream_key, count=10)
            print(f"Retrieved {len(track_data)} track points\n")
            
            # Write to Parquet
            if track_data:
                file_path = parquet.write_trip_track_data(trip_id, track_data, start_date)
                print(f"Saved to Parquet: {file_path} ✓")
        
    finally:
        redis.disconnect()
        mysql.disconnect()


def example_2_list_all_trips():
    """Example: List all available trips."""
    print("\n=== Example 2: Listing all trips ===\n")
    
    redis = RedisClient(
        host=os.getenv('REDIS_HOST'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        password=os.getenv('REDIS_PASSWORD') or None,
        db=int(os.getenv('REDIS_DB', 0))
    )
    
    try:
        redis.connect()
        
        keys = redis.get_trip_completion_keys()
        print(f"Found {len(keys)} trips:\n")
        
        for i, key in enumerate(keys[:10], 1):  # Show first 10
            result = redis.extract_trip_id_from_key(key)
            if result:
                trip_id, start_date = result
                print(f"{i}. Trip ID: {trip_id}, Start Date: {start_date}")
            else:
                print(f"{i}. Key: {key} (could not parse)")
                continue
            
            # Get basic info
            data = redis.get_trip_completion_data_by_key(key)
            if data:
                status = data.get('status', 'unknown')
                vehicle = data.get('vehicle_id', 'N/A')
                print(f"   Status: {status}, Vehicle: {vehicle}")
        
        if len(keys) > 10:
            print(f"\n... and {len(keys) - 10} more trips")
            
    finally:
        redis.disconnect()


def example_3_read_parquet_file():
    """Example: Read and inspect a Parquet file."""
    print("\n=== Example 3: Reading Parquet files ===\n")
    
    parquet = ParquetWriter(
        output_dir=os.getenv('PARQUET_OUTPUT_DIR', './output/parquet')
    )
    
    files = parquet.list_trip_files()
    
    if not files:
        print("No Parquet files found")
        return
    
    print(f"Found {len(files)} Parquet files\n")
    
    # Read the first file
    first_file = files[0]
    info = parquet.get_file_info(first_file)
    
    if info:
        print(f"File: {info['filename']}")
        print(f"Rows: {info['row_count']}")
        print(f"Columns: {info['column_count']}")
        print(f"Size: {info['size_bytes']} bytes")
        print(f"\nColumn names:")
        for col in info['columns']:
            dtype = info['dtypes'].get(col, 'unknown')
            print(f"  - {col} ({dtype})")
        
        # Read actual data
        df = parquet.read_parquet_file(first_file)
        if df is not None:
            print(f"\nFirst 5 rows:")
            print(df.head())


def example_4_query_mysql():
    """Example: Query trip data from MySQL."""
    print("\n=== Example 4: Querying MySQL ===\n")
    
    mysql = MySQLClient(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'trip_data')
    )
    
    try:
        mysql.connect()
        
        # Query for a trip
        mysql.cursor.execute("SELECT trip_id, vehicle_id, status, start_time FROM trips LIMIT 5")
        trips = mysql.cursor.fetchall()
        
        if trips:
            print(f"Found {len(trips)} trips in MySQL:\n")
            for trip in trips:
                print(f"Trip ID: {trip['trip_id']}")
                print(f"  Vehicle: {trip['vehicle_id']}")
                print(f"  Status: {trip['status']}")
                print(f"  Start: {trip['start_time']}\n")
        else:
            print("No trips found in MySQL")
            
    finally:
        mysql.disconnect()


if __name__ == '__main__':
    print("Trip Storage System - Examples\n")
    print("=" * 50)
    
    # Run examples
    try:
        example_1_process_single_trip()
        example_2_list_all_trips()
        example_3_read_parquet_file()
        example_4_query_mysql()
    except Exception as e:
        print(f"\nError running examples: {e}")
        print("\nMake sure you have:")
        print("1. Configured .env file with correct credentials")
        print("2. Installed dependencies: pip install -r requirements.txt")
        print("3. Created MySQL database: mysql -u root -p < schema.sql")
