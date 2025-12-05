#!/usr/bin/env python3
"""
Script to download and decode Parquet files from S3.
Can also read local Parquet files.
"""

import os
import sys
import argparse
import pandas as pd
import tempfile
import gzip
from dotenv import load_dotenv
from s3_client import S3Client

load_dotenv()

# GTFS-realtime VehicleStopStatus reverse mapping
STATUS_REVERSE_MAPPING = {
    0: 'INCOMING_AT',
    1: 'STOPPED_AT',
    2: 'IN_TRANSIT_TO',
    3: 'UNKNOWN',
}


def decode_parquet_from_s3(s3_path: str):
    """
    Download and decode a Parquet file from S3.
    
    Args:
        s3_path: S3 path (e.g., 'trips/20251204/trip-123-20251204-20251204_123456.parquet')
    """
    # Initialize S3 client
    s3_client = S3Client(
        endpoint_url=os.getenv('S3_ENDPOINT_URL', ''),
        access_key_id=os.getenv('S3_ACCESS_KEY_ID', ''),
        secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY', ''),
        bucket_name=os.getenv('S3_BUCKET_NAME', ''),
        region=os.getenv('S3_REGION', 'us-east-1')
    )
    
    try:
        # Connect to S3
        print(f"📡 Connecting to S3...")
        s3_client.connect()
        print(f"✓ Connected to S3\n")
        
        # Download to temporary file
        print(f"⬇️  Downloading from S3: {s3_path}")
        with tempfile.NamedTemporaryFile(suffix='.parquet.gz', delete=False) as tmp_file:
            temp_gz_path = tmp_file.name
        
        # Download the compressed file
        s3_client.s3_client.download_file(
            s3_client.bucket_name,
            s3_path,
            temp_gz_path
        )
        print(f"✓ Downloaded compressed file\n")
        
        # Decompress the file
        temp_path = temp_gz_path.replace('.gz', '')
        with gzip.open(temp_gz_path, 'rb') as f_in:
            with open(temp_path, 'wb') as f_out:
                f_out.write(f_in.read())
        
        print(f"✓ Decompressed file\n")
        
        # Decode the parquet file
        decode_local_parquet(temp_path)
        
        # Clean up
        os.remove(temp_gz_path)
        os.remove(temp_path)
        print(f"\n✓ Cleaned up temporary files")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)
    finally:
        s3_client.disconnect()


def decode_local_parquet(file_path: str):
    """
    Decode and display a local Parquet file.
    Handles both .parquet and .parquet.gz files.
    
    Args:
        file_path: Path to local Parquet file (can be .parquet or .parquet.gz)
    """
    try:
        # Check if file is gzip-compressed
        if file_path.endswith('.gz'):
            print(f"📂 Decompressing and reading Parquet file: {file_path}\n")
            # Decompress to temporary file
            temp_path = file_path.replace('.gz', '')
            with gzip.open(file_path, 'rb') as f_in:
                with open(temp_path, 'wb') as f_out:
                    f_out.write(f_in.read())
            
            # Read the decompressed file
            df = pd.read_parquet(temp_path, engine='pyarrow')
            
            # Clean up temporary file
            os.remove(temp_path)
        else:
            print(f"📂 Reading Parquet file: {file_path}\n")
            df = pd.read_parquet(file_path, engine='pyarrow')
        
        # Convert status codes back to strings
        if 'status' in df.columns:
            df['status'] = df['status'].map(STATUS_REVERSE_MAPPING).fillna('UNKNOWN')
        
        # Display file information
        print("=" * 80)
        print("FILE INFORMATION")
        print("=" * 80)
        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # Display column information
        print("\n" + "=" * 80)
        print("COLUMNS")
        print("=" * 80)
        for col in df.columns:
            dtype = df[col].dtype
            non_null = df[col].notna().sum()
            null_count = df[col].isna().sum()
            print(f"  {col:25s} | {str(dtype):15s} | Non-null: {non_null:5d} | Null: {null_count:5d}")
        
        # Display first few rows
        print("\n" + "=" * 80)
        print("ALL ROWS")
        print("=" * 80)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        pd.set_option('display.max_rows', None)
        print(df)
        
        # Display sample of unique values for categorical columns
        categorical_cols = df.select_dtypes(include=['object', 'string']).columns
        if len(categorical_cols) > 0:
            print("\n" + "=" * 80)
            print("CATEGORICAL COLUMN SAMPLES (up to 10 unique values)")
            print("=" * 80)
            for col in categorical_cols:
                unique_vals = df[col].unique()
                print(f"\n{col}:")
                print(f"  Unique values: {len(unique_vals)}")
                if len(unique_vals) <= 10:
                    print(f"  Values: {list(unique_vals)}")
                else:
                    print(f"  Sample values: {list(unique_vals[:10])} ...")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"✗ Error reading Parquet file: {e}")
        sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Download and decode Parquet files from S3 or local storage'
    )
    
    parser.add_argument(
        'path',
        type=str,
        help='S3 path (e.g., trips/20251204/file.parquet) or local file path'
    )
    
    parser.add_argument(
        '--local',
        action='store_true',
        help='Treat path as local file instead of S3 path'
    )
    
    args = parser.parse_args()
    
    if args.local or os.path.exists(args.path):
        # Local file
        if not os.path.exists(args.path):
            print(f"✗ Error: File not found: {args.path}")
            sys.exit(1)
        decode_local_parquet(args.path)
    else:
        # S3 file
        decode_parquet_from_s3(args.path)


if __name__ == '__main__':
    main()
