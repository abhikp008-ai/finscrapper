import os
import json
import pandas as pd
import logging
import subprocess
import tempfile
import time
import random
from datetime import datetime
from typing import List, Dict, Any, Optional

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

class MegaRcloneStorageService:
    """MEGA storage service using rclone for reliable cloud operations"""
    
    def __init__(self):
        self.remote_name = 'mega'
        self.upload_folder = os.getenv('MEGA_UPLOAD_FOLDER', 'finscrap')
        self.config_file = os.path.join('.secrets', 'rclone', 'rclone.conf')
        self._ensure_config_dir()
        self._ensure_rclone_config()
    
    def _ensure_config_dir(self):
        """Ensure rclone config directory exists"""
        try:
            config_dir = os.path.dirname(self.config_file)
            os.makedirs(config_dir, exist_ok=True)
            logger.info(f"Rclone config directory ready: {config_dir}")
        except Exception as e:
            logger.warning(f"Could not create rclone config directory: {e}")
    
    def _ensure_rclone_config(self):
        """Ensure rclone is configured for MEGA"""
        try:
            # Check if rclone is installed
            result = subprocess.run(['rclone', 'version'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                raise Exception("Rclone is not installed or not in PATH")
            
            logger.info(f"Rclone version: {result.stdout.split()[1] if result.stdout else 'Unknown'}")
            
            # Check if MEGA remote exists
            result = subprocess.run(['rclone', 'listremotes', '--config', self.config_file], 
                                  capture_output=True, text=True, timeout=10)
            
            if f"{self.remote_name}:" not in result.stdout:
                # Configure MEGA remote automatically
                self._configure_mega_remote()
            else:
                logger.info(f"MEGA remote '{self.remote_name}' already configured")
            
        except subprocess.TimeoutExpired:
            logger.error("Rclone command timed out")
            raise Exception("Rclone configuration failed - timeout")
        except Exception as e:
            logger.error(f"Rclone setup failed: {e}")
            raise Exception(f"Rclone service unavailable: {str(e)}")
    
    def _configure_mega_remote(self):
        """Configure MEGA remote in rclone"""
        try:
            email = os.getenv('MEGA_EMAIL')
            password = os.getenv('MEGA_PASSWORD')
            
            if not email or not password:
                raise Exception("MEGA_EMAIL and MEGA_PASSWORD environment variables required")
            
            # Create rclone config for MEGA
            config_content = f"""[{self.remote_name}]
type = mega
user = {email}
pass = {self._obscure_password(password)}
"""
            
            # Write config file
            os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
            with open(self.config_file, 'w') as f:
                f.write(config_content)
            
            # Set config file permissions
            os.chmod(self.config_file, 0o600)
            
            logger.info(f"MEGA remote configured in: {self.config_file}")
            
            # Test the configuration
            self._test_connection()
            
        except Exception as e:
            logger.error(f"Failed to configure MEGA remote: {e}")
            raise
    
    def _obscure_password(self, password: str) -> str:
        """Obscure password for rclone config (basic encoding)"""
        try:
            # Use rclone obscure command if available
            result = subprocess.run(['rclone', 'obscure', password], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return result.stdout.strip()
            else:
                raise Exception("rclone obscure command failed")
        except Exception as e:
            logger.error(f"Failed to obscure password: {e}")
            raise Exception("Could not secure password for rclone config")
    
    def _test_connection(self):
        """Test MEGA connection"""
        try:
            result = subprocess.run([
                'rclone', 'lsd', f'{self.remote_name}:',
                '--config', self.config_file
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                logger.info("MEGA connection test successful")
            else:
                logger.warning(f"MEGA connection test failed: {result.stderr}")
                
        except Exception as e:
            logger.error(f"MEGA connection test failed: {e}")
    
    @simple_retry(max_attempts=3, delay_base=4)
    def store_news_data(self, data: List[Dict[str, Any]], source: str) -> int:
        """Store news data as CSV file and upload to MEGA"""
        try:
            if not data:
                return 0
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Add scraped_at timestamp
            df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Create CSV filename
            filename = f"{source.lower()}_news_data.csv"
            
            # Download existing data if available
            existing_df = self._download_csv_from_mega(filename)
            
            if existing_df is not None:
                # Merge with existing data, avoiding duplicates based on URL
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['url'], keep='last')
            else:
                combined_df = df
            
            # Upload the merged CSV
            self._upload_csv_to_mega(combined_df, filename)
            
            # Also upload a timestamped version for history
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            timestamped_filename = f"{source.lower()}_news_{timestamp}.csv"
            self._upload_csv_to_mega(df, timestamped_filename)
            
            new_records = len(df)
            total_records = len(combined_df)
            
            logger.info(f"Successfully uploaded {new_records} new articles to MEGA via rclone")
            logger.info(f"Total records in {filename}: {total_records}")
            
            return new_records
            
        except Exception as e:
            logger.error(f"Failed to store news data to MEGA: {e}")
            # Fallback to local storage
            self._fallback_to_local_storage(data, source)
            raise
    
    def _download_csv_from_mega(self, filename: str) -> Optional[pd.DataFrame]:
        """Download existing CSV from MEGA if it exists"""
        try:
            # Create remote path
            remote_path = f"{self.remote_name}:{self.upload_folder}/{filename}"
            temp_file = os.path.join(tempfile.gettempdir(), f"download_{filename}")
            
            # Try to download the file
            result = subprocess.run([
                'rclone', 'copyto', remote_path, temp_file,
                '--config', self.config_file
            ], capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                # Read the CSV file
                df = pd.read_csv(temp_file)
                logger.info(f"Downloaded existing {filename} from MEGA ({len(df)} records)")
                
                # Clean up temp file
                try:
                    os.remove(temp_file)
                except:
                    pass
                
                return df
            else:
                logger.info(f"No existing {filename} found in MEGA (or download failed)")
                return None
                
        except Exception as e:
            logger.warning(f"Could not download existing {filename}: {e}")
            return None
    
    def _upload_csv_to_mega(self, df: pd.DataFrame, filename: str):
        """Upload CSV DataFrame to MEGA using rclone"""
        try:
            # Create temporary CSV file
            temp_file = os.path.join(tempfile.gettempdir(), filename)
            df.to_csv(temp_file, index=False)
            
            # Create remote path
            remote_path = f"{self.remote_name}:{self.upload_folder}/{filename}"
            
            # Upload to MEGA
            result = subprocess.run([
                'rclone', 'copyto', temp_file, remote_path,
                '--config', self.config_file
            ], capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                logger.info(f"Successfully uploaded {filename} to MEGA")
            else:
                error_msg = result.stderr.strip()
                logger.error(f"Failed to upload {filename}: {error_msg}")
                raise Exception(f"Upload failed: {error_msg}")
            
            # Clean up temp file
            try:
                os.remove(temp_file)
            except:
                pass
            
        except Exception as e:
            logger.error(f"Failed to upload {filename} to MEGA: {e}")
            raise
    
    def _fallback_to_local_storage(self, data: List[Dict[str, Any]], source: str):
        """Fallback to local storage when MEGA upload fails"""
        try:
            from .mega_manual_upload_service import MegaManualUploadService
            
            logger.info("Falling back to local storage service")
            fallback_service = MegaManualUploadService()
            fallback_service.store_news_data(data, source)
            
        except Exception as e:
            logger.error(f"Fallback storage also failed: {e}")
    
    def get_all_news_data(self) -> List[Dict[str, Any]]:
        """Download and combine all news data from MEGA CSV files"""
        try:
            all_data = []
            sources = ['moneycontrol', 'livemint', 'financialexpress']
            
            for source in sources:
                filename = f"{source}_news_data.csv"
                df = self._download_csv_from_mega(filename)
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
            logger.error(f"Failed to retrieve news data from MEGA: {e}")
            # Fallback to local data
            try:
                from .mega_manual_upload_service import MegaManualUploadService
                fallback_service = MegaManualUploadService()
                return fallback_service.get_all_news_data()
            except:
                return []
    
    def get_filtered_data(self, source: str = None, search_query: str = None) -> List[Dict[str, Any]]:
        """Get filtered news data from MEGA"""
        try:
            all_data = self.get_all_news_data()
            
            if source:
                all_data = [item for item in all_data if item.get('source', '').lower() == source.lower()]
            
            if search_query:
                search_query = search_query.lower()
                all_data = [
                    item for item in all_data 
                    if search_query in item.get('title', '').lower() or 
                       search_query in item.get('content', '').lower()
                ]
            
            return all_data
            
        except Exception as e:
            logger.error(f"Failed to filter MEGA data: {e}")
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
        """Get information about stored files in MEGA"""
        try:
            files = []
            total_size = 0
            
            # List files in MEGA upload folder
            remote_path = f"{self.remote_name}:{self.upload_folder}"
            result = subprocess.run([
                'rclone', 'lsjson', remote_path,
                '--config', self.config_file
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                try:
                    file_list = json.loads(result.stdout)
                    
                    for file_info in file_list:
                        if file_info.get('Name', '').endswith('.csv'):
                            size = file_info.get('Size', 0)
                            total_size += size
                            
                            # Estimate record count (approximate)
                            estimated_records = max(0, (size - 100) // 150)
                            
                            files.append({
                                'filename': file_info.get('Name', 'unknown'),
                                'size': size,
                                'size_mb': round(size / (1024 * 1024), 2),
                                'records': estimated_records,
                                'location': 'MEGA Cloud Storage',
                                'modified': file_info.get('ModTime', 'unknown')
                            })
                            
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse rclone output: {e}")
            
            return {
                'total_files': len(files),
                'files': files,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'storage_type': 'MEGA Cloud Storage (via rclone)',
                'upload_folder': self.upload_folder,
                'remote_name': self.remote_name,
                'storage_path': f'{self.remote_name}:{self.upload_folder}',
                'authenticated': True
            }
            
        except Exception as e:
            logger.error(f"Failed to get MEGA storage info: {e}")
            return {'total_files': 0, 'files': [], 'total_size_mb': 0, 'authenticated': False}
    
    def test_connection(self) -> bool:
        """Test connection to MEGA"""
        try:
            result = subprocess.run([
                'rclone', 'lsd', f'{self.remote_name}:',
                '--config', self.config_file
            ], capture_output=True, text=True, timeout=30)
            
            return result.returncode == 0
            
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False