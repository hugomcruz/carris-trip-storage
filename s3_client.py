"""
S3 client module for uploading Parquet files.
Handles uploads to S3-compatible storage with year/month/day folder organization.
"""

import boto3
from botocore.exceptions import ClientError
import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class S3Client:
    """S3 client for uploading Parquet files."""
    
    def __init__(self, endpoint_url: str, access_key_id: str, secret_access_key: str, 
                 bucket_name: str, region: str = 'us-east-1'):
        """
        Initialize S3 client.
        
        Args:
            endpoint_url: S3 endpoint URL (leave empty for AWS S3)
            access_key_id: S3 access key ID
            secret_access_key: S3 secret access key
            bucket_name: S3 bucket name
            region: S3 region (default: us-east-1)
        """
        self.endpoint_url = endpoint_url if endpoint_url else None
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = None
        
    def connect(self):
        """Establish connection to S3."""
        try:
            # Create S3 client
            config_args = {
                'aws_access_key_id': self.access_key_id,
                'aws_secret_access_key': self.secret_access_key,
                'region_name': self.region
            }
            
            if self.endpoint_url:
                config_args['endpoint_url'] = self.endpoint_url
            
            self.s3_client = boto3.client('s3', **config_args)
            
            # Test connection by checking if bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket {self.bucket_name} does not exist")
            elif error_code == '403':
                logger.error(f"Access denied to bucket {self.bucket_name}")
            else:
                logger.error(f"Failed to connect to S3: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to S3: {str(e)}")
            raise
    
    def disconnect(self):
        """Close S3 connection."""
        self.s3_client = None
        logger.info("Disconnected from S3")
    
    def upload_parquet_file(self, local_file_path: str, trip_date: Optional[datetime] = None) -> str:
        """
        Upload a Parquet file to S3 organized by year/month/day.
        
        Args:
            local_file_path: Path to the local Parquet file
            trip_date: Date of the trip (defaults to current date if not provided)
            
        Returns:
            S3 key (path) of the uploaded file
        """
        try:
            # Use provided date or current date
            if trip_date is None:
                trip_date = datetime.now()
            
            # Extract filename from path
            filename = os.path.basename(local_file_path)
            
            # Create S3 key with year/month/day structure
            year = trip_date.strftime('%Y')
            month = trip_date.strftime('%m')
            day = trip_date.strftime('%d')
            
            s3_key = f"trips/{year}/{month}/{day}/{filename}"
            
            # Upload file
            self.s3_client.upload_file(
                local_file_path,
                self.bucket_name,
                s3_key
            )
            
            logger.info(f"Successfully uploaded {filename} to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except ClientError as e:
            logger.error(f"Failed to upload {local_file_path} to S3: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error uploading file to S3: {str(e)}")
            raise
    
    def file_exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3.
        
        Args:
            s3_key: S3 key (path) to check
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking if file exists: {str(e)}")
                raise
    
    def delete_file(self, s3_key: str):
        """
        Delete a file from S3.
        
        Args:
            s3_key: S3 key (path) to delete
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logger.info(f"Deleted file from S3: {s3_key}")
        except ClientError as e:
            logger.error(f"Failed to delete {s3_key} from S3: {str(e)}")
            raise
    
    def list_files(self, prefix: str = '', max_files: int = 1000) -> list:
        """
        List files in S3 bucket with optional prefix.
        
        Args:
            prefix: S3 key prefix to filter results
            max_files: Maximum number of files to return
            
        Returns:
            List of S3 keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_files
            )
            
            if 'Contents' not in response:
                return []
            
            return [obj['Key'] for obj in response['Contents']]
            
        except ClientError as e:
            logger.error(f"Failed to list files from S3: {str(e)}")
            raise
