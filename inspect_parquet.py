#!/usr/bin/env python3
"""
Script to inspect Parquet files.
"""

import sys
import pandas as pd
from parquet_writer import ParquetWriter
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    parquet_writer = ParquetWriter(
        output_dir=os.getenv('PARQUET_OUTPUT_DIR', './output/parquet')
    )
    
    # List all files
    files = parquet_writer.list_trip_files()
    
    if not files:
        print("No Parquet files found!")
        return
    
    print(f"Found {len(files)} Parquet files:\n")
    
    for i, file in enumerate(files, 1):
        info = parquet_writer.get_file_info(file)
        if info:
            print(f"[{i}] {info['filename']}")
            print(f"    Rows: {info['row_count']}, Size: {info['size_bytes']:,} bytes")
    
    # Read and display the first file in detail
    if files:
        print(f"\n{'='*80}")
        print("DETAILED VIEW OF FIRST FILE")
        print('='*80)
        
        first_file = files[0]
        info = parquet_writer.get_file_info(first_file)
        
        print(f"\nFile: {info['filename']}")
        print(f"Size: {info['size_bytes']:,} bytes")
        print(f"Rows: {info['row_count']}")
        print(f"Columns: {', '.join(info['columns'])}")
        
        df = parquet_writer.read_parquet_file(first_file)
        if df is not None:
            print(f"\n{'='*80}")
            print("FIRST 10 ROWS:")
            print('='*80)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 200)
            print(df.head(10))
            
            print(f"\n{'='*80}")
            print("DATA TYPES:")
            print('='*80)
            print(df.dtypes)
            
            print(f"\n{'='*80}")
            print("STATISTICS:")
            print('='*80)
            print(df.describe())

if __name__ == '__main__':
    main()
