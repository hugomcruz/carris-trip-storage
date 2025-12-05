#!/usr/bin/env python3
"""
Quick script to explore Redis data without MySQL dependency.
"""

import os
from dotenv import load_dotenv
from redis_client import RedisClient
import json

load_dotenv()

def main():
    # Initialize Redis client
    redis = RedisClient(
        host=os.getenv('REDIS_HOST'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        password=os.getenv('REDIS_PASSWORD') or None,
        db=int(os.getenv('REDIS_DB', 0))
    )
    
    try:
        print("🔌 Connecting to Redis...")
        redis.connect()
        print("✅ Connected!\n")
        
        # Get all completion keys
        print("📋 Searching for trip completion data...")
        keys = redis.get_trip_completion_keys()
        print(f"✅ Found {len(keys)} trips\n")
        
        if not keys:
            print("No trips found. Exiting.")
            return
        
        # Show first few trips
        print("=" * 60)
        print("TRIP COMPLETION DATA (first 5 trips)")
        print("=" * 60)
        
        for i, key in enumerate(keys[:5], 1):
            result = redis.extract_trip_id_from_key(key)
            if not result:
                print(f"\n[{i}] Could not parse key: {key}")
                continue
            
            trip_id, start_date = result
            print(f"\n[{i}] Trip ID: {trip_id}, Start Date: {start_date}")
            print(f"    Key: {key}")
            
            data = redis.get_trip_completion_data_by_key(key)
            if data:
                print(f"    Data: {json.dumps(data, indent=2, default=str)}")
                
                # Look for stream
                stream_key = redis.find_trip_stream(trip_id, start_date)
                if stream_key:
                    print(f"    📊 Stream found: {stream_key}")
                    track_data = redis.get_stream_data(stream_key, count=5)
                    print(f"    📍 Track points: {len(track_data)} (showing first 5)")
                    for j, point in enumerate(track_data[:3], 1):
                        print(f"        Point {j}: {point}")
                else:
                    print(f"    ⚠️  No stream found for this trip")
        
        if len(keys) > 5:
            print(f"\n... and {len(keys) - 5} more trips")
        
        print("\n" + "=" * 60)
        print(f"SUMMARY: {len(keys)} trips ready to process")
        print("=" * 60)
        
    finally:
        redis.disconnect()

if __name__ == '__main__':
    main()
