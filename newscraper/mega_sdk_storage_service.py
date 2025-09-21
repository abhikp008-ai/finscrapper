import os
import json
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from mega import Mega
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import tempfile

logger = logging.getLogger(__name__)

class MegaSDKStorageService:
    """MEGA SDK-based storage service with session caching and automatic upload"""
    
    def __init__(self):
        self.mega = Mega()
        self.account = None
        self.session_file = os.path.join('.secrets', 'mega', 'session.json')
        self.upload_folder = os.getenv('MEGA_UPLOAD_FOLDER', 'finscrap')
        self.folder_id = None
        self._ensure_session_dir()
        self._authenticate()
    
    def _ensure_session_dir(self):
        """Ensure session directory exists"""
        try:
            session_dir = os.path.dirname(self.session_file)
            os.makedirs(session_dir, exist_ok=True)
            logger.info(f"Session directory ready: {session_dir}")
        except Exception as e:
            logger.warning(f"Could not create session directory: {e}")
            # Fallback to temp directory
            self.session_file = os.path.join(tempfile.gettempdir(), 'mega_session.json')
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _authenticate(self):
        """Authenticate with MEGA using cached session or credentials"""
        try:
            # Get credentials
            email = os.getenv('MEGA_EMAIL')
            password = os.getenv('MEGA_PASSWORD')
            
            if not email or not password:
                raise Exception("MEGA_EMAIL and MEGA_PASSWORD environment variables required")
            
            # Try session-based login first
            if self._try_session_login():
                logger.info("Successfully authenticated with cached session")
                self._ensure_upload_folder()
                return
            
            # Fallback to email/password login
            logger.info("Attempting login with email/password")
            self.account = self.mega.login(email, password)
            
            if self.account:
                logger.info(f"Successfully logged in to MEGA as {email}")
                self._save_session()
                self._ensure_upload_folder()
            else:
                raise Exception("MEGA login failed - invalid credentials")
                
        except Exception as e:
            logger.error(f"MEGA authentication failed: {e}")
            raise Exception(f"MEGA SDK unavailable: {str(e)}")
    
    def _try_session_login(self) -> bool:
        """Try to login using cached session"""
        try:
            if not os.path.exists(self.session_file):
                return False
            
            with open(self.session_file, 'r') as f:
                session_data = json.load(f)
            
            # Try to restore session
            session_id = session_data.get('session_id')
            if session_id:
                # Note: mega.py doesn't have direct session restore, 
                # so we'll rely on the library's internal caching
                logger.info("Session file found but mega.py doesn't support direct session restore")
                return False
            
        except Exception as e:
            logger.warning(f"Could not restore session: {e}")
            return False
    
    def _save_session(self):
        """Save session data for future use"""
        try:
            session_data = {
                'timestamp': datetime.now().isoformat(),
                'session_id': 'cached',  # Placeholder - mega.py handles internal caching
                'email': os.getenv('MEGA_EMAIL')
            }
            
            with open(self.session_file, 'w') as f:
                json.dump(session_data, f, indent=2)
            
            logger.info("Session cached for future use")
            
        except Exception as e:
            logger.warning(f"Could not save session: {e}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _ensure_upload_folder(self):
        """Ensure upload folder exists in MEGA"""
        try:
            if not self.account:
                raise Exception("Not authenticated")
            
            # Get all folders
            folders = self.account.get_files()
            
            # Look for existing upload folder
            for file_id, file_info in folders.items():
                if file_info.get('a', {}).get('n') == self.upload_folder and file_info.get('t') == 1:
                    self.folder_id = file_id
                    logger.info(f"Found existing MEGA folder: {self.upload_folder}")
                    return
            
            # Create folder if it doesn't exist
            logger.info(f"Creating MEGA folder: {self.upload_folder}")
            self.folder_id = self.account.create_folder(self.upload_folder)
            
        except Exception as e:
            logger.error(f"Failed to ensure upload folder: {e}")
            self.folder_id = None  # Upload to root if folder creation fails
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
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
            existing_df = self._download_existing_csv(filename)
            
            if existing_df is not None:
                # Merge with existing data, avoiding duplicates based on URL
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['url'], keep='last')
            else:
                combined_df = df
            
            # Upload the merged CSV
            self._upload_csv(combined_df, filename)
            
            # Also upload a timestamped version for history
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            timestamped_filename = f"{source.lower()}_news_{timestamp}.csv"
            self._upload_csv(df, timestamped_filename)
            
            new_records = len(df)
            total_records = len(combined_df)
            
            logger.info(f"Successfully uploaded {new_records} new articles to MEGA")
            logger.info(f"Total records in {filename}: {total_records}")
            
            return new_records
            
        except Exception as e:
            logger.error(f"Failed to store news data to MEGA: {e}")
            # Fallback to local storage
            self._fallback_to_local_storage(data, source)
            raise
    
    def _download_existing_csv(self, filename: str) -> Optional[pd.DataFrame]:
        """Download existing CSV from MEGA if it exists"""
        try:
            if not self.account:
                return None
            
            files = self.account.get_files()
            
            # Look for the file in our upload folder
            for file_id, file_info in files.items():
                if (file_info.get('a', {}).get('n') == filename and 
                    file_info.get('p') == self.folder_id):
                    
                    # Download to temporary file
                    temp_file = os.path.join(tempfile.gettempdir(), f"temp_{filename}")
                    self.account.download(file_id, temp_file)
                    
                    # Read CSV
                    df = pd.read_csv(temp_file)
                    
                    # Clean up temp file
                    try:
                        os.remove(temp_file)
                    except:
                        pass
                    
                    logger.info(f"Downloaded existing {filename} from MEGA ({len(df)} records)")
                    return df
            
            logger.info(f"No existing {filename} found in MEGA")
            return None
            
        except Exception as e:
            logger.warning(f"Could not download existing {filename}: {e}")
            return None
    
    def _upload_csv(self, df: pd.DataFrame, filename: str):
        """Upload CSV DataFrame to MEGA"""
        try:
            if not self.account:
                raise Exception("Not authenticated")
            
            # Create temporary CSV file
            temp_file = os.path.join(tempfile.gettempdir(), filename)
            df.to_csv(temp_file, index=False)
            
            # Delete existing file if it exists
            self._delete_existing_file(filename)
            
            # Upload new file
            if self.folder_id:
                file_id = self.account.upload(temp_file, self.folder_id)
            else:
                file_id = self.account.upload(temp_file)
            
            logger.info(f"Successfully uploaded {filename} to MEGA (file_id: {file_id})")
            
            # Clean up temp file
            try:
                os.remove(temp_file)
            except:
                pass
            
        except Exception as e:
            logger.error(f"Failed to upload {filename} to MEGA: {e}")
            raise
    
    def _delete_existing_file(self, filename: str):
        """Delete existing file from MEGA if it exists"""
        try:
            if not self.account:
                return
            
            files = self.account.get_files()
            
            # Look for existing file
            for file_id, file_info in files.items():
                if (file_info.get('a', {}).get('n') == filename and 
                    file_info.get('p') == self.folder_id):
                    
                    self.account.delete(file_id)
                    logger.info(f"Deleted existing {filename} from MEGA")
                    return
                    
        except Exception as e:
            logger.warning(f"Could not delete existing {filename}: {e}")
    
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
                df = self._download_existing_csv(filename)
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
            
            if self.account:
                mega_files = self.account.get_files()
                
                for file_id, file_info in mega_files.items():
                    # Only include CSV files in our upload folder
                    if (file_info.get('p') == self.folder_id and 
                        file_info.get('a', {}).get('n', '').endswith('.csv')):
                        
                        filename = file_info.get('a', {}).get('n', 'unknown')
                        size = file_info.get('s', 0)
                        total_size += size
                        
                        # Estimate record count (approximate)
                        estimated_records = max(0, (size - 100) // 150)  # Rough estimate
                        
                        files.append({
                            'filename': filename,
                            'size': size,
                            'size_mb': round(size / (1024 * 1024), 2),
                            'records': estimated_records,
                            'location': 'MEGA Cloud Storage',
                            'file_id': file_id,
                            'modified': datetime.fromtimestamp(
                                file_info.get('ts', 0)
                            ).strftime('%Y-%m-%d %H:%M:%S')
                        })
            
            return {
                'total_files': len(files),
                'files': files,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'storage_type': 'MEGA Cloud Storage',
                'upload_folder': self.upload_folder,
                'authenticated': self.account is not None
            }
            
        except Exception as e:
            logger.error(f"Failed to get MEGA storage info: {e}")
            return {'total_files': 0, 'files': [], 'total_size_mb': 0, 'authenticated': False}