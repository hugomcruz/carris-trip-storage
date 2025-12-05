#!/usr/bin/env python3
"""Quick test of the full pipeline with one trip."""
import os
from dotenv import load_dotenv
from redis_client import RedisClient
from mysql_client import MySQLClient
from parquet_writer import ParquetWriter

load_dotenv()

# Initialize
redis = RedisClient(
    host=os.getenv('REDIS_HOST'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    password=os.getenv('REDIS_PASSWORD') or None,
    db=int(os.getenv('REDIS_DB', 0))
)

mysql = MySQLClient(
    host=os.getenv('MYSQL_HOST'),
    port=int(os.getenv('MYSQL_PORT')),
    user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'),
    database=os.getenv('MYSQL_DATABASE')
)

parquet = ParquetWriter(output_dir=os.getenv('PARQUET_OUTPUT_DIR', './output/parquet'))

try:
    print("Connecting to Redis...")
    redis.connect()
    print("✓ Redis connected")
    
    print("Connecting to MySQL...")
    mysql.connect()
    print("✓ MySQL connected")
    
    # Process one trip
    trip_id = '21520'
    start_date = '20250901'  # Adjust this to your actual trip start date
    print(f"\nProcessing trip: {trip_id} (start_date: {start_date})")
    
    # Get completion data
    print("  Getting completion data...")
    completion = redis.get_trip_completion_data(trip_id, start_date)
    print(f"  ✓ Got completion data: {list(completion.keys())}")
    
    # Store in MySQL
    print("  Storing in MySQL...")
    mysql.insert_trip_completion(completion)
    print("  ✓ Stored in MySQL")
    
    # Get stream data
    print("  Finding stream...")
    stream_key = redis.find_trip_stream(trip_id, start_date)
    print(f"  ✓ Found stream: {stream_key}")
    
    print("  Getting track data...")
    track_data = redis.get_stream_data(stream_key)
    print(f"  ✓ Got {len(track_data)} track points")
    
    # Write Parquet
    print("  Writing Parquet file...")
    file_path = parquet.write_trip_track_data(trip_id, track_data, start_date)
    print(f"  ✓ Wrote: {file_path}")
    
    # Log processing
    mysql.log_trip_processing(trip_id, 'completed', file_path)
    print("  ✓ Logged processing")
    
    print("\n✅ SUCCESS!")
    
finally:
    redis.disconnect()
    mysql.disconnect()
