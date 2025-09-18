"""
Configuration for Google Sheets integration
"""
import os
import json
from django.conf import settings

# Google Sheets configuration
SPREADSHEET_NAME = "Financial News Scraper Data"
CONFIG_FILE = "sheets_config.json"

def get_or_create_spreadsheet_id():
    """Get existing spreadsheet ID from config file or environment"""
    # First check environment variable
    spreadsheet_id = os.environ.get('FINANCIAL_NEWS_SPREADSHEET_ID')
    if spreadsheet_id:
        return spreadsheet_id
    
    # Then check config file
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                return config.get('spreadsheet_id')
    except Exception:
        pass
    
    return None

def save_spreadsheet_id(spreadsheet_id):
    """Save spreadsheet ID to config file"""
    try:
        config = {}
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
        
        config['spreadsheet_id'] = spreadsheet_id
        
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f)
        
        # Also set in environment for current process
        os.environ['FINANCIAL_NEWS_SPREADSHEET_ID'] = spreadsheet_id
    except Exception as e:
        print(f"Warning: Could not save spreadsheet ID to config: {e}")