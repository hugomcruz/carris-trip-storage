"""
Parquet writer module for storing trip track data.
Handles writing trip tracking data to Parquet files.
"""

import pandas as pd
import logging
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)

# GTFS-realtime VehicleStopStatus mapping
# Maps status strings to uint8 values
STATUS_MAPPING = {
    'INCOMING_AT': 0,
    'STOPPED_AT': 1,
    'IN_TRANSIT_TO': 2,
    # Numeric values are kept as-is
    '0': 0,
    '1': 1,
    '2': 2,
    # Unknown/default
    'UNKNOWN': 3,
    '3': 3,
}


class ParquetWriter:
    """Writer for trip track data in Parquet format."""
    
    def __init__(self, output_dir: str):
        """
        Initialize Parquet writer.
        
        Args:
            output_dir: Directory to store Parquet files
        """
        self.output_dir = output_dir
        self._ensure_output_dir()
        
    def _ensure_output_dir(self):
        """Create output directory if it doesn't exist. Thread-safe."""
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            if not os.path.exists(self.output_dir):
                logger.debug(f"Created output directory: {self.output_dir}")
        except FileExistsError:
            # Race condition - another thread created it, which is fine
            pass
    
    def cleanup_output_dir(self):
        """Clean up any leftover Parquet files from previous runs."""
        try:
            if os.path.exists(self.output_dir):
                parquet_files = [f for f in os.listdir(self.output_dir) if f.endswith('.parquet')]
                if parquet_files:
                    logger.warning(f"Found {len(parquet_files)} leftover Parquet files from previous run - deleting:")
                    for filename in parquet_files:
                        filepath = os.path.join(self.output_dir, filename)
                        try:
                            os.remove(filepath)
                            logger.warning(f"  Deleted: {filename}")
                        except Exception as e:
                            logger.error(f"  Failed to delete {filename}: {e}")
                    logger.warning(f"Cleanup complete - {len(parquet_files)} files deleted")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def write_trip_track_data(self, trip_id: str, track_data: List[Dict[str, Any]], start_date: str) -> Optional[str]:
        """
        Write trip track data to a Parquet file.
        
        Args:
            trip_id: Trip identifier
            track_data: List of track data points from Redis stream
            start_date: Trip start date (format: YYYYMMDD) - required
            
        Returns:
            Path to the created Parquet file or None if failed
        """
        if not track_data:
            logger.warning(f"No track data to write for trip {trip_id}")
            return None
            
        try:
            # Convert track data to DataFrame
            df = self._prepare_dataframe(track_data)
            
            if df.empty:
                logger.warning(f"Empty DataFrame for trip {trip_id}")
                return None
            
            # Generate filename with timestamp and start_date
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"trip-{trip_id}-{start_date}-{timestamp}.parquet"
            filepath = os.path.join(self.output_dir, filename)
            
            # Write to Parquet with maximum compression
            # zstd provides better compression than gzip with similar speed
            # use_dictionary=True enables dictionary encoding for repeated values
            df.to_parquet(
                filepath, 
                engine='pyarrow', 
                compression='zstd',
                compression_level=22,  # Maximum compression (1-22)
                index=False,
                use_dictionary=True,
                write_statistics=False  # Skip statistics to reduce size
            )
            
            logger.debug(f"Successfully wrote {len(df)} track points to {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error writing Parquet file for trip {trip_id}: {e}")
            return None
    
    def _prepare_dataframe(self, track_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Prepare DataFrame from track data.
        
        Args:
            track_data: List of track data points
            
        Returns:
            Pandas DataFrame
        """
        records = []
        
        for entry in track_data:
            # Extract data fields directly, no wrapper record
            data = entry.get('data', {})
            
            # Common fields in tracking data
            if isinstance(data, dict):
                record = {}
                # Flatten the data dictionary
                for key, value in data.items():
                    # Handle nested JSON strings
                    if isinstance(value, str):
                        try:
                            parsed = json.loads(value)
                            if isinstance(parsed, dict):
                                for nested_key, nested_value in parsed.items():
                                    record[f"{key}_{nested_key}"] = nested_value
                            else:
                                record[key] = value
                        except (json.JSONDecodeError, TypeError):
                            record[key] = value
                    else:
                        record[key] = value
            else:
                record = {'data': str(data)}
            
            records.append(record)
        
        df = pd.DataFrame(records)
        
        # Convert stop_id to integer
        if 'stop_id' in df.columns:
            df['stop_id'] = pd.to_numeric(df['stop_id'], errors='coerce').fillna(0).astype('uint16')
        
        # Convert vehicle_id to integer
        if 'vehicle_id' in df.columns:
            df['vehicle_id'] = pd.to_numeric(df['vehicle_id'], errors='coerce').fillna(0).astype('uint16')
        
        # Convert stop_sequence to integer
        if 'stop_sequence' in df.columns:
            df['stop_sequence'] = pd.to_numeric(df['stop_sequence'], errors='coerce').fillna(0).astype('uint8')
        
        # Convert bearing to integer (0-360 degrees)
        if 'bearing' in df.columns:
            df['bearing'] = pd.to_numeric(df['bearing'], errors='coerce').fillna(0).astype('uint16')
        
        # Convert speed to integer
        if 'speed' in df.columns:
            df['speed'] = pd.to_numeric(df['speed'], errors='coerce').fillna(0).astype('uint8')
        
        # Convert status to integer using GTFS-realtime mapping
        if 'status' in df.columns:
            df['status'] = df['status'].apply(lambda x: self._map_status(x))
        
        # Preserve lat/lon as float64 for precision
        if 'lat' in df.columns:
            df['lat'] = pd.to_numeric(df['lat'], errors='coerce').astype('float64')
        if 'lon' in df.columns:
            df['lon'] = pd.to_numeric(df['lon'], errors='coerce').astype('float64')
        
        # Convert timestamp fields to uint32
        if 'ts' in df.columns:
            df['ts'] = pd.to_numeric(df['ts'], errors='coerce').fillna(0).astype('uint32')
        if 'service_date' in df.columns:
            df['service_date'] = pd.to_numeric(df['service_date'], errors='coerce').fillna(0).astype('uint32')
        
        # Ensure numeric columns are properly typed
        self._optimize_dtypes(df)
        
        return df
    
    def _map_status(self, status_value: Any) -> int:
        """
        Map status string or numeric value to GTFS-realtime uint8 code.
        
        Args:
            status_value: Status as string or number
            
        Returns:
            uint8 status code (0-3)
        """
        if pd.isna(status_value) or status_value == '':
            return 3  # Unknown
        
        # Convert to string and uppercase for mapping
        status_str = str(status_value).upper().strip()
        
        # Return mapped value or default to 3 (Unknown)
        return STATUS_MAPPING.get(status_str, 3)
    
    def _optimize_dtypes(self, df: pd.DataFrame):
        """
        Optimize DataFrame data types for better storage.
        Uses smallest possible integer types to minimize file size.
        
        Args:
            df: Pandas DataFrame to optimize (modified in place)
        """
        # Columns to exclude from optimization (keep original types)
        exclude_columns = {'lat', 'lon', 'ts', 'service_date'}  # Preserve float precision for coordinates and explicit uint32 for timestamps
        
        for col in df.columns:
            # Skip timestamp columns
            if df[col].dtype == 'datetime64[ns]':
                continue
            
            # Skip excluded columns (lat/lon coordinates)
            if col in exclude_columns:
                continue
            
            # Convert numeric columns to smallest possible type
            if df[col].dtype == 'object':
                try:
                    # Try converting to numeric
                    converted = pd.to_numeric(df[col], errors='coerce')
                    if converted.notna().sum() / len(df) > 0.5:
                        # Determine best integer type
                        if converted.min() >= 0:
                            # Unsigned integers for positive values
                            if converted.max() <= 255:
                                df[col] = converted.astype('uint8')
                            elif converted.max() <= 65535:
                                df[col] = converted.astype('uint16')
                            elif converted.max() <= 4294967295:
                                df[col] = converted.astype('uint32')
                            else:
                                df[col] = converted.astype('uint64')
                        else:
                            # Signed integers for negative values
                            if converted.min() >= -128 and converted.max() <= 127:
                                df[col] = converted.astype('int8')
                            elif converted.min() >= -32768 and converted.max() <= 32767:
                                df[col] = converted.astype('int16')
                            elif converted.min() >= -2147483648 and converted.max() <= 2147483647:
                                df[col] = converted.astype('int32')
                            else:
                                df[col] = converted.astype('int64')
                except (ValueError, TypeError):
                    pass
            elif df[col].dtype in ['int64', 'float64']:
                # Downcast existing numeric types
                try:
                    if df[col].dtype == 'float64':
                        # Check if can be integer
                        if (df[col] == df[col].astype('int64')).all():
                            df[col] = df[col].astype('int64')
                    
                    # Downcast integers
                    if df[col].dtype in ['int64', 'int32', 'int16']:
                        df[col] = pd.to_numeric(df[col], downcast='integer')
                    elif df[col].dtype == 'float64':
                        df[col] = pd.to_numeric(df[col], downcast='float')
                except (ValueError, TypeError):
                    pass
        
        return df
    
    def read_parquet_file(self, filepath: str) -> Optional[pd.DataFrame]:
        """
        Read a Parquet file.
        
        Args:
            filepath: Path to Parquet file
            
        Returns:
            Pandas DataFrame or None if failed
        """
        try:
            df = pd.read_parquet(filepath, engine='pyarrow')
            logger.info(f"Successfully read Parquet file: {filepath}")
            return df
        except Exception as e:
            logger.error(f"Error reading Parquet file {filepath}: {e}")
            return None
    
    def list_trip_files(self, trip_id: Optional[str] = None) -> List[str]:
        """
        List all Parquet files for a specific trip or all trips.
        
        Args:
            trip_id: Trip identifier (optional, if None lists all)
            
        Returns:
            List of file paths
        """
        try:
            files = []
            for filename in os.listdir(self.output_dir):
                if filename.endswith('.parquet'):
                    if trip_id:
                        if filename.startswith(f"trip_{trip_id}_"):
                            files.append(os.path.join(self.output_dir, filename))
                    else:
                        files.append(os.path.join(self.output_dir, filename))
            
            return sorted(files)
        except Exception as e:
            logger.error(f"Error listing Parquet files: {e}")
            return []
    
    def get_file_info(self, filepath: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a Parquet file.
        
        Args:
            filepath: Path to Parquet file
            
        Returns:
            Dictionary with file information or None
        """
        try:
            if not os.path.exists(filepath):
                return None
                
            df = pd.read_parquet(filepath, engine='pyarrow')
            
            info = {
                'filepath': filepath,
                'filename': os.path.basename(filepath),
                'size_bytes': os.path.getsize(filepath),
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'created': datetime.fromtimestamp(os.path.getctime(filepath)),
                'modified': datetime.fromtimestamp(os.path.getmtime(filepath))
            }
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting file info for {filepath}: {e}")
            return None
