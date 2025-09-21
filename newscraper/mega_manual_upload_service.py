import os
import pandas as pd
import logging
import tempfile
from datetime import datetime
from typing import List, Dict, Any, Optional
import shutil

logger = logging.getLogger(__name__)

class MegaManualUploadService:
    """Service that creates CSV files ready for manual upload to MEGA"""
    
    def __init__(self):
        self.upload_ready_dir = os.path.join(os.getcwd(), 'mega_upload_ready')
        self.backup_dir = os.path.join(os.getcwd(), '.data', 'csv_storage')
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure upload and backup directories exist"""
        try:
            os.makedirs(self.upload_ready_dir, exist_ok=True)
            os.makedirs(self.backup_dir, exist_ok=True)
            logger.info(f"Upload directory ready: {self.upload_ready_dir}")
            logger.info(f"Backup directory ready: {self.backup_dir}")
        except Exception as e:
            logger.error(f"Failed to create directories: {e}")
            # Fallback to temp directory
            self.upload_ready_dir = tempfile.mkdtemp(prefix='mega_upload_')
            logger.info(f"Using temporary upload directory: {self.upload_ready_dir}")
    
    def store_news_data(self, data: List[Dict[str, Any]], source: str) -> int:
        """Store news data as CSV files ready for MEGA upload"""
        try:
            if not data:
                return 0
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Add scraped_at timestamp
            df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Create CSV filename with timestamp for uniqueness
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{source.lower()}_news_data.csv"
            filename_with_timestamp = f"{source.lower()}_news_{timestamp}.csv"
            
            # Paths for different storage locations
            upload_file = os.path.join(self.upload_ready_dir, filename)
            backup_file = os.path.join(self.backup_dir, filename)
            timestamped_file = os.path.join(self.upload_ready_dir, filename_with_timestamp)
            
            # Load existing data for merging
            existing_df = self._load_existing_data(backup_file)
            
            if existing_df is not None:
                # Merge with existing data, avoiding duplicates based on URL
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['url'], keep='last')
            else:
                combined_df = df
            
            # Save files in multiple locations
            # 1. Main upload-ready file (for manual MEGA upload)
            combined_df.to_csv(upload_file, index=False)
            
            # 2. Backup file (for app functionality)
            combined_df.to_csv(backup_file, index=False)
            
            # 3. Timestamped file (for version history)
            df.to_csv(timestamped_file, index=False)
            
            # Create upload instructions
            self._create_upload_instructions()
            
            new_records = len(df)
            total_records = len(combined_df)
            
            logger.info(f"Successfully prepared {new_records} new articles for MEGA upload")
            logger.info(f"Total records: {total_records}")
            logger.info(f"Files ready for MEGA upload in: {self.upload_ready_dir}")
            
            return new_records
            
        except Exception as e:
            logger.error(f"Failed to prepare news data: {e}")
            raise
    
    def _load_existing_data(self, file_path: str) -> Optional[pd.DataFrame]:
        """Load existing CSV data if available"""
        try:
            if os.path.exists(file_path):
                return pd.read_csv(file_path)
            return None
        except Exception as e:
            logger.warning(f"Could not load existing data from {file_path}: {e}")
            return None
    
    def get_all_news_data(self) -> List[Dict[str, Any]]:
        """Get all news data from CSV files"""
        try:
            all_data = []
            sources = ['moneycontrol', 'livemint', 'financialexpress']
            
            for source in sources:
                filename = f"{source}_news_data.csv"
                
                # Try upload directory first, then backup
                file_paths = [
                    os.path.join(self.upload_ready_dir, filename),
                    os.path.join(self.backup_dir, filename)
                ]
                
                for file_path in file_paths:
                    if os.path.exists(file_path):
                        try:
                            df = pd.read_csv(file_path)
                            if not df.empty:
                                # Add source if not present
                                if 'source' not in df.columns:
                                    df['source'] = source.title()
                                all_data.append(df)
                                break  # Use first found file
                        except Exception as e:
                            logger.warning(f"Could not read {file_path}: {e}")
            
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
        """Export data to CSV file for download"""
        try:
            if not filename:
                filename = f"finscrap_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # Create file in upload directory for easy access
            export_file = os.path.join(self.upload_ready_dir, filename)
            
            # Convert to DataFrame and save
            df = pd.DataFrame(data)
            df.to_csv(export_file, index=False)
            
            return export_file
            
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            raise
    
    def _create_upload_instructions(self):
        """Create instructions file for MEGA upload"""
        try:
            instructions_file = os.path.join(self.upload_ready_dir, 'MEGA_UPLOAD_INSTRUCTIONS.txt')
            
            instructions = f"""
=== MEGA UPLOAD INSTRUCTIONS ===

Your CSV files are ready for upload to MEGA cloud storage!

FILES TO UPLOAD:
{self.upload_ready_dir}/

STEPS TO UPLOAD TO MEGA:
1. Go to https://mega.nz and log in to your account
2. Click "Upload" or drag and drop the CSV files
3. Upload all .csv files from the folder: {self.upload_ready_dir}
4. Your news data will then be available in your MEGA cloud storage

CSV FILES CREATED:
- moneycontrol_news_data.csv (main data file)
- Individual timestamped files for version history

WHAT'S IN THE CSV FILES:
- Title: News article headline
- URL: Link to original article
- Date: Publication date
- Content: Full article content
- Source: News source (MoneyControl, LiveMint, etc.)
- Scraped_At: When the data was collected

AUTOMATIC FEATURES:
- Duplicate articles are automatically removed
- New data is merged with existing data
- Files are ready for immediate MEGA upload

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            
            with open(instructions_file, 'w') as f:
                f.write(instructions)
            
            logger.info(f"Upload instructions created: {instructions_file}")
            
        except Exception as e:
            logger.error(f"Failed to create instructions: {e}")
    
    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about stored files"""
        try:
            files = []
            total_size = 0
            
            # Check upload directory
            for directory, label in [(self.upload_ready_dir, 'Ready for MEGA Upload'), 
                                   (self.backup_dir, 'Local Backup')]:
                if os.path.exists(directory):
                    for filename in os.listdir(directory):
                        if filename.endswith('.csv'):
                            file_path = os.path.join(directory, filename)
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
                                'location': label,
                                'full_path': file_path,
                                'modified': datetime.fromtimestamp(
                                    os.path.getmtime(file_path)
                                ).strftime('%Y-%m-%d %H:%M:%S')
                            })
            
            return {
                'total_files': len(files),
                'files': files,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'upload_directory': self.upload_ready_dir,
                'storage_type': 'Ready for MEGA Upload'
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage info: {e}")
            return {'total_files': 0, 'files': [], 'total_size_mb': 0}
    
    def get_mega_upload_path(self) -> str:
        """Get the path where files are ready for MEGA upload"""
        return self.upload_ready_dir