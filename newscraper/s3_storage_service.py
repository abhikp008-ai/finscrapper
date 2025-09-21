import os
import json
import pandas as pd
import logging
import tempfile
import time
import random
import io
from datetime import datetime
from typing import List, Dict, Any, Optional
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)

def simple_retry(max_attempts=3, delay_base=2):
    """Simple retry decorator without external dependencies"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise e
                    delay = delay_base * (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay:.1f}s: {e}")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

class S3StorageService:
    """AWS S3 storage service for financial news CSV files"""
    
    def __init__(self):
        self.bucket_name = os.getenv('AWS_S3_BUCKET')
        self.region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.prefix = os.getenv('AWS_S3_PREFIX', 'finscrap')
        self.env = os.getenv('ENVIRONMENT', 'development')
        
        if not self.bucket_name:
            raise ValueError("AWS_S3_BUCKET environment variable is required")
        
        # Configure boto3 client with retries
        self.config = Config(
            retries={
                'max_attempts': 5,
                'mode': 'standard'
            },
            region_name=self.region
        )
        
        self._s3_client = None
        self._ensure_s3_client()
    
    def _ensure_s3_client(self):
        """Initialize S3 client with credentials"""
        try:
            access_key = os.getenv('AWS_ACCESS_KEY_ID')
            secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            
            if not access_key or not secret_key:
                raise NoCredentialsError()
            
            self._s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=self.config
            )
            
            # Test connection
            self._test_bucket_access()
            logger.info(f"S3 client initialized for bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def _test_bucket_access(self):
        """Test if we can access the S3 bucket"""
        try:
            self._s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                raise Exception(f"S3 bucket '{self.bucket_name}' not found")
            elif error_code == '403':
                raise Exception(f"Access denied to S3 bucket '{self.bucket_name}'")
            else:
                raise Exception(f"S3 bucket access failed: {e}")
    
    def _get_latest_key(self, source: str) -> str:
        """Get S3 key for latest CSV file"""
        return f"{self.prefix}/{self.env}/latest/{source.lower()}/{source.lower()}_news_data.csv"
    
    def _get_history_key(self, source: str, timestamp: str = None) -> str:
        """Get S3 key for historical CSV file"""
        if not timestamp:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{self.prefix}/{self.env}/history/{source.lower()}/{source.lower()}_news_{timestamp}.csv"
    
    @simple_retry(max_attempts=3, delay_base=2)
    def _download_csv_from_s3(self, key: str) -> Optional[pd.DataFrame]:
        """Download CSV file from S3 and return as DataFrame"""
        try:
            response = self._s3_client.get_object(Bucket=self.bucket_name, Key=key)
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_content))
            logger.info(f"Downloaded {key} from S3 ({len(df)} records)")
            return df
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info(f"No existing file found at {key}")
                return None
            else:
                logger.error(f"Failed to download {key}: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to read CSV from {key}: {e}")
            raise
    
    @simple_retry(max_attempts=3, delay_base=2)
    def _upload_csv_to_s3(self, df: pd.DataFrame, key: str):
        """Upload DataFrame as CSV to S3"""
        try:
            # Convert DataFrame to CSV string
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Upload to S3 with server-side encryption
            self._s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=csv_content,
                ContentType='text/csv',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Successfully uploaded {key} to S3 ({len(df)} records)")
            
        except Exception as e:
            logger.error(f"Failed to upload {key} to S3: {e}")
            raise
    
    @simple_retry(max_attempts=3, delay_base=4)
    def store_news_data(self, data: List[Dict[str, Any]], source: str) -> int:
        """Store news data as CSV file in S3"""
        try:
            if not data:
                return 0
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Add scraped_at timestamp
            df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Download existing data if available
            latest_key = self._get_latest_key(source)
            existing_df = self._download_csv_from_s3(latest_key)
            
            if existing_df is not None:
                # Merge with existing data, avoiding duplicates based on URL
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['url'], keep='last')
            else:
                combined_df = df
            
            # Upload the merged CSV to latest
            self._upload_csv_to_s3(combined_df, latest_key)
            
            # Also upload current batch to history
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            history_key = self._get_history_key(source, timestamp)
            self._upload_csv_to_s3(df, history_key)
            
            new_records = len(df)
            total_records = len(combined_df)
            
            logger.info(f"Successfully uploaded {new_records} new articles to S3")
            logger.info(f"Total records in {latest_key}: {total_records}")
            
            return new_records
            
        except Exception as e:
            logger.error(f"Failed to store news data to S3: {e}")
            # Fallback to local storage
            self._fallback_to_local_storage(data, source)
            raise
    
    def _fallback_to_local_storage(self, data: List[Dict[str, Any]], source: str):
        """Fallback to local storage when S3 upload fails"""
        try:
            # Create local backup directory
            backup_dir = '.data/s3_backup'
            os.makedirs(backup_dir, exist_ok=True)
            
            # Save to local CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{source.lower()}_news_{timestamp}.csv"
            filepath = os.path.join(backup_dir, filename)
            
            df = pd.DataFrame(data)
            df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df.to_csv(filepath, index=False)
            
            logger.info(f"Saved {len(data)} articles to local backup: {filepath}")
            
        except Exception as e:
            logger.error(f"Fallback storage also failed: {e}")
    
    def get_all_news_data(self) -> List[Dict[str, Any]]:
        """Download and combine all news data from S3 CSV files"""
        try:
            all_data = []
            sources = ['moneycontrol', 'livemint', 'financialexpress']
            
            for source in sources:
                latest_key = self._get_latest_key(source)
                df = self._download_csv_from_s3(latest_key)
                if df is not None and not df.empty:
                    # Add source if not present
                    if 'source' not in df.columns:
                        df['source'] = source.title()
                    all_data.append(df)
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                # Sort by date (newest first)
                if 'date' in combined_df.columns:
                    combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce')
                    combined_df = combined_df.sort_values('date', ascending=False, na_position='last')
                
                return combined_df.to_dict('records')
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to retrieve news data from S3: {e}")
            return []
    
    def get_filtered_data(self, source: str = None, search_query: str = None) -> List[Dict[str, Any]]:
        """Get filtered news data from S3"""
        try:
            all_data = self.get_all_news_data()
            
            if source:
                all_data = [item for item in all_data if str(item.get('source', '')).lower() == source.lower()]
            
            if search_query:
                search_query = search_query.lower()
                all_data = [
                    item for item in all_data 
                    if search_query in str(item.get('title', '')).lower() or 
                       search_query in str(item.get('content', '')).lower()
                ]
            
            return all_data
            
        except Exception as e:
            logger.error(f"Failed to filter S3 data: {e}")
            return []
    
    def export_to_csv(self, data: List[Dict[str, Any]], filename: str = None) -> str:
        """Export data to CSV file and return file path"""
        try:
            if not filename:
                filename = f"finscrap_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # Create temporary file
            temp_file = os.path.join(tempfile.gettempdir(), filename)
            
            # Convert to DataFrame and save
            df = pd.DataFrame(data)
            df.to_csv(temp_file, index=False)
            
            return temp_file
            
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            raise
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about stored files in S3"""
        try:
            files = []
            total_size = 0
            
            # List objects under the prefix
            paginator = self._s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=f"{self.prefix}/{self.env}/"
            )
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.csv'):
                            size = obj['Size']
                            total_size += size
                            
                            # Estimate record count (approximate)
                            estimated_records = max(0, (size - 100) // 150)
                            
                            files.append({
                                'filename': obj['Key'].split('/')[-1],
                                'key': obj['Key'],
                                'size': size,
                                'size_mb': round(size / (1024 * 1024), 2),
                                'records': estimated_records,
                                'location': f's3://{self.bucket_name}',
                                'modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                            })
            
            return {
                'total_files': len(files),
                'files': files,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'storage_type': 'AWS S3 Cloud Storage',
                'bucket_name': self.bucket_name,
                'prefix': self.prefix,
                'storage_path': f's3://{self.bucket_name}/{self.prefix}',
                'region': self.region,
                'authenticated': True
            }
            
        except Exception as e:
            logger.error(f"Failed to get S3 storage info: {e}")
            return {'total_files': 0, 'files': [], 'total_size_mb': 0, 'authenticated': False}
    
    def test_connection(self) -> bool:
        """Test connection to S3"""
        try:
            self._s3_client.head_bucket(Bucket=self.bucket_name)
            return True
        except Exception as e:
            logger.error(f"S3 connection test failed: {e}")
            return False