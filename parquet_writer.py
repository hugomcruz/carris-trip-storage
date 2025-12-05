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
        """Create output directory if it doesn't exist."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")
    
    def write_trip_track_data(self, trip_id: str, track_data: List[Dict[str, Any]]) -> Optional[str]:
        """
        Write trip track data to a Parquet file.
        
        Args:
            trip_id: Trip identifier
            track_data: List of track data points from Redis stream
            
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
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"trip_{trip_id}_{timestamp}.parquet"
            filepath = os.path.join(self.output_dir, filename)
            
            # Write to Parquet
            df.to_parquet(filepath, engine='pyarrow', compression='snappy', index=False)
            
            logger.info(f"Successfully wrote {len(df)} track points to {filepath}")
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
            record = {
                'message_id': entry.get('message_id'),
                'timestamp': entry.get('timestamp'),
            }
            
            # Extract data fields
            data = entry.get('data', {})
            
            # Common fields in tracking data
            if isinstance(data, dict):
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
                record['data'] = str(data)
            
            records.append(record)
        
        df = pd.DataFrame(records)
        
        # Convert timestamp column to datetime if it exists
        if 'timestamp' in df.columns and df['timestamp'].notna().any():
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Ensure numeric columns are properly typed
        self._optimize_dtypes(df)
        
        return df
    
    def _optimize_dtypes(self, df: pd.DataFrame):
        """
        Optimize DataFrame data types for better storage.
        
        Args:
            df: Pandas DataFrame to optimize (modified in place)
        """
        for col in df.columns:
            # Skip timestamp columns
            if df[col].dtype == 'datetime64[ns]':
                continue
                
            # Try converting to numeric if possible
            if df[col].dtype == 'object':
                try:
                    # Try integer first
                    converted = pd.to_numeric(df[col], errors='coerce')
                    if converted.notna().all() or converted.notna().sum() / len(df) > 0.5:
                        df[col] = converted
                        continue
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
