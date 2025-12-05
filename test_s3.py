#!/usr/bin/env python3
"""
Test S3 connection and upload.
"""

import os
from dotenv import load_dotenv
from s3_client import S3Client

load_dotenv()

def test_s3():
    """Test S3 connection."""
    print("Testing S3 connection...")
    
    # Get S3 config
    s3_endpoint = os.getenv('S3_ENDPOINT_URL', '')
    s3_access_key = os.getenv('S3_ACCESS_KEY_ID', '')
    s3_secret_key = os.getenv('S3_SECRET_ACCESS_KEY', '')
    s3_bucket = os.getenv('S3_BUCKET_NAME', '')
    s3_region = os.getenv('S3_REGION', 'nl-ams')
    
    print(f"Endpoint: {s3_endpoint}")
    print(f"Bucket: {s3_bucket}")
    print(f"Region: {s3_region}")
    print(f"Access Key: {s3_access_key[:10]}...")
    
    # Create S3 client
    client = S3Client(
        endpoint_url=s3_endpoint,
        access_key_id=s3_access_key,
        secret_access_key=s3_secret_key,
        bucket_name=s3_bucket,
        region=s3_region
    )
    
    # Test connection
    try:
        client.connect()
        print("✓ Successfully connected to S3")
        
        # List some files
        print("\nListing files in bucket...")
        files = client.list_files(prefix='trips/', max_files=10)
        if files:
            print(f"Found {len(files)} files:")
            for f in files[:5]:
                print(f"  - {f}")
        else:
            print("No files found")
            
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.disconnect()

if __name__ == "__main__":
    test_s3()
