import os
import pandas as pd
import logging
import tempfile
import json
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
import base64

logger = logging.getLogger(__name__)

class SimpleFileStorageService:
    """Simple CSV-based storage service using local filesystem with optional cloud backup"""
    
    def __init__(self):
        self.storage_dir = self._get_storage_directory()
        self._ensure_storage_directory()
    
    def _get_storage_directory(self) -> str:
        """Get the storage directory path"""
        # Use a persistent directory in the project
        storage_dir = os.path.join(os.getcwd(), '.data', 'csv_storage')
        return storage_dir
    
    def _ensure_storage_directory(self):
        """Ensure storage directory exists"""
        try:
            os.makedirs(self.storage_dir, exist_ok=True)
            logger.info(f"Storage directory ready: {self.storage_dir}")
        except Exception as e:
            logger.error(f"Failed to create storage directory: {e}")
            # Fallback to temp directory
            self.storage_dir = tempfile.mkdtemp(prefix='finscrap_')
            logger.info(f"Using temporary storage: {self.storage_dir}")
    
    def store_news_data(self, data: List[Dict[str, Any]], source: str) -> int:
        """Store news data as CSV file"""
        try:
            if not data:
                return 0
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Add scraped_at timestamp
            df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Create CSV filename
            filename = f"{source.lower()}_news_data.csv"
            file_path = os.path.join(self.storage_dir, filename)
            
            # Load existing data if file exists
            existing_df = None
            if os.path.exists(file_path):
                try:
                    existing_df = pd.read_csv(file_path)
                except Exception as e:
                    logger.warning(f"Could not read existing file {filename}: {e}")
            
            if existing_df is not None:
                # Merge with existing data, avoiding duplicates based on URL
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['url'], keep='last')
            else:
                combined_df = df
            
            # Save to CSV file
            combined_df.to_csv(file_path, index=False)
            
            new_records = len(df)
            total_records = len(combined_df)
            logger.info(f"Stored {new_records} new articles for {source} (total: {total_records})")
            return new_records
            
        except Exception as e:
            logger.error(f"Failed to store news data: {e}")
            raise
    
    def get_all_news_data(self) -> List[Dict[str, Any]]:
        """Load and combine all news data from CSV files"""
        try:
            all_data = []
            sources = ['moneycontrol', 'livemint', 'financialexpress']
            
            for source in sources:
                filename = f"{source}_news_data.csv"
                file_path = os.path.join(self.storage_dir, filename)
                
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        if not df.empty:
                            # Add source if not present
                            if 'source' not in df.columns:
                                df['source'] = source.title()
                            all_data.append(df)
                    except Exception as e:
                        logger.warning(f"Could not read {filename}: {e}")
            
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
            
            # Create file in temporary directory for download
            temp_file = os.path.join(tempfile.gettempdir(), filename)
            
            # Convert to DataFrame and save
            df = pd.DataFrame(data)
            df.to_csv(temp_file, index=False)
            
            return temp_file
            
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            raise
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about stored files"""
        try:
            files = []
            total_size = 0
            
            if os.path.exists(self.storage_dir):
                for filename in os.listdir(self.storage_dir):
                    if filename.endswith('_news_data.csv'):
                        file_path = os.path.join(self.storage_dir, filename)
                        size = os.path.getsize(file_path)
                        total_size += size
                        
                        # Get record count
                        try:
                            df = pd.read_csv(file_path)
                            record_count = len(df)
                        except:
                            record_count = 0
                        
                        files.append({
                            'filename': filename,
                            'size': size,
                            'size_mb': round(size / (1024 * 1024), 2),
                            'records': record_count,
                            'modified': datetime.fromtimestamp(
                                os.path.getmtime(file_path)
                            ).strftime('%Y-%m-%d %H:%M:%S')
                        })
            
            return {
                'total_files': len(files),
                'files': files,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'storage_path': self.storage_dir
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage info: {e}")
            return {'total_files': 0, 'files': [], 'total_size_mb': 0}
    
    def backup_to_mega(self) -> bool:
        """Optional: Backup CSV files to MEGA if credentials are available"""
        try:
            # This is a placeholder for MEGA backup functionality
            # Can be implemented later if needed
            logger.info("MEGA backup not implemented yet")
            return False
        except Exception as e:
            logger.error(f"MEGA backup failed: {e}")
            return False
    
    def clear_all_data(self) -> bool:
        """Clear all stored data (use with caution)"""
        try:
            if os.path.exists(self.storage_dir):
                for filename in os.listdir(self.storage_dir):
                    if filename.endswith('.csv'):
                        file_path = os.path.join(self.storage_dir, filename)
                        os.remove(file_path)
                        logger.info(f"Deleted: {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to clear data: {e}")
            return False