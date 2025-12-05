"""
Script to check field types in Parquet files.
"""

import pyarrow.parquet as pq
import sys
import os

def check_parquet_schema(filepath):
    """Display the schema and data types of a Parquet file."""
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return
    
    # Read the schema
    parquet_file = pq.read_table(filepath)
    schema = parquet_file.schema
    
    print(f"\nParquet Schema for: {os.path.basename(filepath)}")
    print("=" * 60)
    print(f"Total fields: {len(schema)}")
    print(f"Total rows: {len(parquet_file)}")
    print("\nField Types:")
    print("-" * 60)
    
    for field in schema:
        print(f"{field.name:20s} : {field.type}")
    
    print("-" * 60)
    
    # Calculate approximate size per row
    file_size = os.path.getsize(filepath)
    rows = len(parquet_file)
    if rows > 0:
        print(f"\nFile size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
        print(f"Bytes per row: {file_size/rows:.2f}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        # Find latest parquet file in output directory
        output_dir = "output/parquet"
        if os.path.exists(output_dir):
            files = [f for f in os.listdir(output_dir) if f.endswith('.parquet')]
            if files:
                filepath = os.path.join(output_dir, sorted(files)[-1])
            else:
                print("No parquet files found in output/parquet")
                sys.exit(1)
        else:
            print("Please provide a parquet file path")
            sys.exit(1)
    
    check_parquet_schema(filepath)
