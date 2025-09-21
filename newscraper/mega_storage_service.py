import os
import pandas as pd
import logging
import tempfile
from datetime import datetime
from typing import List, Dict, Any, Optional
from mega import Mega

logger = logging.getLogger(__name__)

class MegaStorageService:
    def __init__(self):
        self.mega = None
        self.m = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with MEGA using environment credentials"""
        try:
            email = os.getenv('MEGA_EMAIL')
            password = os.getenv('MEGA_PASSWORD')
            
            if not email or not password:
                raise Exception(
                    "MEGA credentials not found. Please set MEGA_EMAIL and MEGA_PASSWORD environment variables."
                )
            
            self.mega = Mega()
            self.m = self.mega.login(email, password)
            
            if self.m is None:
                raise Exception("MEGA login failed - invalid credentials")
                
            logger.info("MEGA authentication successful")
            
        except Exception as e:
            logger.error(f"MEGA authentication failed: {e}")
            self.m = None
            raise Exception(f"MEGA service unavailable: {str(e)}")
    
    def store_news_data(self, data: List[Dict[str, Any]], source: str) -> int:
        """Store news data as CSV file on MEGA"""
        try:
            if not data:
                return 0
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Add scraped_at timestamp
            df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Create CSV filename
            filename = f"{source.lower()}_news_data.csv"
            temp_file = os.path.join(tempfile.gettempdir(), filename)
            
            # Check if file exists on MEGA and download it to merge data
            existing_df = self._download_existing_data(filename)
            
            if existing_df is not None:
                # Merge with existing data, avoiding duplicates based on URL
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['url'], keep='last')
            else:
                combined_df = df
            
            # Save to temporary file
            combined_df.to_csv(temp_file, index=False)
            
            # Upload to MEGA
            self._upload_file(temp_file, filename)
            
            # Clean up temporary file
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            new_records = len(df)
            logger.info(f"Successfully stored {new_records} new articles for {source}")
            return new_records
            
        except Exception as e:
            logger.error(f"Failed to store news data: {e}")
            raise
    
    def get_all_news_data(self) -> List[Dict[str, Any]]:
        """Download and combine all news data from MEGA"""
        try:
            all_data = []
            sources = ['moneycontrol', 'livemint', 'financialexpress']
            
            for source in sources:
                filename = f"{source}_news_data.csv"
                df = self._download_existing_data(filename)
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
            logger.error(f"Failed to retrieve news data: {e}")
            return []
    
    def get_filtered_data(self, source: str = None, search_query: str = None) -> List[Dict[str, Any]]:
        """Get filtered news data"""
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
            logger.error(f"Failed to filter data: {e}")
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
    
    def _download_existing_data(self, filename: str) -> Optional[pd.DataFrame]:
        """Download existing CSV file from MEGA if it exists"""
        try:
            files = self.m.get_files()
            
            for file_id, file_info in files.items():
                if file_info['a']['n'] == filename:
                    # Download to temporary location
                    temp_dir = tempfile.gettempdir()
                    temp_file = os.path.join(temp_dir, filename)
                    
                    # Remove file if it already exists
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                    
                    self.m.download(file_info, temp_dir)
                    
                    if os.path.exists(temp_file):
                        df = pd.read_csv(temp_file)
                        os.remove(temp_file)  # Clean up
                        return df
            
            return None
            
        except Exception as e:
            logger.warning(f"Could not download existing data for {filename}: {e}")
            return None
    
    def _upload_file(self, local_file_path: str, filename: str = None):
        """Upload file to MEGA, replacing existing if it exists"""
        try:
            if self.m is None:
                raise Exception("MEGA service not authenticated")
                
            if not filename:
                filename = os.path.basename(local_file_path)
            
            # Check if file already exists and delete it
            files = self.m.get_files()
            for file_id, file_info in files.items():
                if file_info['a']['n'] == filename:
                    self.m.delete(file_id)
                    logger.info(f"Deleted existing file: {filename}")
                    break
            
            # Upload new file
            self.m.upload(local_file_path)
            logger.info(f"Successfully uploaded: {filename}")
            
        except Exception as e:
            logger.error(f"Failed to upload {local_file_path}: {e}")
            raise
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about stored files"""
        try:
            files = self.m.get_files()
            news_files = []
            total_size = 0
            
            for file_id, file_info in files.items():
                filename = file_info['a']['n']
                if filename.endswith('_news_data.csv'):
                    size = file_info['s']
                    total_size += size
                    news_files.append({
                        'filename': filename,
                        'size': size,
                        'size_mb': round(size / (1024 * 1024), 2)
                    })
            
            return {
                'total_files': len(news_files),
                'files': news_files,
                'total_size_mb': round(total_size / (1024 * 1024), 2)
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage info: {e}")
            return {'total_files': 0, 'files': [], 'total_size_mb': 0}