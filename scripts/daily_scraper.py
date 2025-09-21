#!/usr/bin/env python3
"""
Daily scraper script for FinScrap
Runs all news scrapers and uploads data to S3
Designed to be run as a cron job at 7:00 AM IST daily
"""
import os
import sys
import logging
from datetime import datetime
import subprocess

# Add the project directory to Python path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'finscrap.settings')

# Configure logging
log_file = os.path.join(project_dir, 'logs', f'daily_scraper_{datetime.now().strftime("%Y%m%d")}.log')
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_scraper():
    """Run the comprehensive news scraper"""
    try:
        logger.info("🚀 Starting daily news scraping at 7:00 AM IST")
        logger.info("=" * 60)
        
        # Change to project directory
        os.chdir(project_dir)
        
        # Run the scraper command
        cmd = [sys.executable, 'manage.py', 'scrape_all_sources', '--max-pages', '3']
        logger.info(f"Running command: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes timeout
        )
        
        if result.returncode == 0:
            logger.info("✅ Scraping completed successfully")
            logger.info("STDOUT:")
            logger.info(result.stdout)
        else:
            logger.error("❌ Scraping failed")
            logger.error("STDERR:")
            logger.error(result.stderr)
            logger.error("STDOUT:")
            logger.error(result.stdout)
        
        # Check S3 status
        try:
            import django
            django.setup()
            
            from newscraper.s3_storage_service import S3StorageService
            storage = S3StorageService()
            info = storage.get_storage_info()
            all_data = storage.get_all_news_data()
            
            logger.info("📊 S3 Storage Status:")
            logger.info(f"   - Total articles: {len(all_data)}")
            logger.info(f"   - CSV files: {info.get('total_files', 0)}")
            logger.info(f"   - Storage size: {info.get('total_size_mb', 0)} MB")
            logger.info(f"   - Storage path: {info.get('storage_path', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"⚠️ Could not check S3 status: {e}")
        
        logger.info("🎯 Daily scraping task completed")
        logger.info("=" * 60)
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        logger.error("❌ Scraping timed out after 30 minutes")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error during scraping: {e}")
        return False

if __name__ == "__main__":
    success = run_scraper()
    sys.exit(0 if success else 1)