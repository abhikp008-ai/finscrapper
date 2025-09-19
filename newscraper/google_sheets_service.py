import json
import logging
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
from datetime import datetime

logger = logging.getLogger(__name__)

# Scopes for Google Sheets and Drive APIs
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]

class GoogleSheetsService:
    def __init__(self):
        self.service = None
        self.drive_service = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate using environment-managed credentials"""
        try:
            # Try to load from environment or secure location
            creds = self._load_credentials()
            
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    try:
                        creds.refresh(Request())
                        self._save_credentials(creds)
                    except Exception as e:
                        logger.error(f"Failed to refresh token: {e}")
                        raise Exception(
                            "Google Sheets authentication expired. Please run the setup command: "
                            "python manage.py setup_google_auth"
                        )
                else:
                    raise Exception(
                        "Google Sheets authentication not found. Please run the setup command: "
                        "python manage.py setup_google_auth"
                    )
            
            self.service = build('sheets', 'v4', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            logger.info("Google Sheets service initialized successfully")
            
        except Exception as e:
            logger.error(f"Google Sheets authentication failed: {e}")
            raise Exception(
                f"Google Sheets service unavailable: {str(e)}. "
                "Please ensure authentication is properly configured."
            )
    
    def _load_credentials(self):
        """Load credentials from secure storage"""
        # Try environment variable first, then fallback to secure location
        token_file = os.getenv('GOOGLE_TOKEN_FILE', '/tmp/secure/token.pickle')
        
        # If environment variable not set, also try the secure location
        if not os.path.exists(token_file) and token_file == 'token.pickle':
            token_file = '/tmp/secure/token.pickle'
        
        if os.path.exists(token_file):
            try:
                with open(token_file, 'rb') as token:
                    return pickle.load(token)
            except Exception as e:
                logger.error(f"Failed to load credentials from {token_file}: {e}")
                return None
        
        logger.error(f"Token file not found at {token_file}")
        return None
    
    def _save_credentials(self, creds):
        """Save credentials to secure storage"""
        # Use secure location as default
        token_file = os.getenv('GOOGLE_TOKEN_FILE', '/tmp/secure/token.pickle')
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(token_file), exist_ok=True)
            
            with open(token_file, 'wb') as token:
                pickle.dump(creds, token)
            # Secure the file permissions
            os.chmod(token_file, 0o600)
            logger.info(f"Credentials saved to {token_file}")
        except Exception as e:
            logger.error(f"Failed to save credentials to {token_file}: {e}")
    
    def create_spreadsheet(self, title):
        """Create a new Google Spreadsheet"""
        try:
            spreadsheet = {
                'properties': {
                    'title': title
                }
            }
            
            spreadsheet = self.service.spreadsheets().create(
                body=spreadsheet,
                fields='spreadsheetId'
            ).execute()
            
            spreadsheet_id = spreadsheet.get('spreadsheetId')
            logger.info(f'Created spreadsheet with ID: {spreadsheet_id}')
            return spreadsheet_id
        
        except Exception as e:
            logger.error(f"Error creating spreadsheet: {e}")
            raise
    
    def create_sheet_with_headers(self, spreadsheet_id, sheet_name, headers):
        """Create a new sheet with headers"""
        try:
            # Add new sheet
            requests = [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }]
            
            self.service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()
            
            # Add headers
            self.append_data(spreadsheet_id, sheet_name, [headers])
            logger.info(f'Created sheet "{sheet_name}" with headers')
            
        except Exception as e:
            logger.error(f"Error creating sheet: {e}")
            raise
    
    def append_data(self, spreadsheet_id, sheet_name, data):
        """Append data to a sheet"""
        try:
            range_name = f"{sheet_name}!A:Z"
            
            body = {
                'values': data
            }
            
            result = self.service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f'Appended {len(data)} rows to {sheet_name}')
            return result
        
        except Exception as e:
            logger.error(f"Error appending data: {e}")
            raise
    
    def read_sheet_data(self, spreadsheet_id, sheet_name, range_name=None):
        """Read data from a sheet"""
        try:
            if not range_name:
                range_name = f"{sheet_name}!A:Z"
            
            result = self.service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name
            ).execute()
            
            values = result.get('values', [])
            logger.info(f'Read {len(values)} rows from {sheet_name}')
            return values
        
        except Exception as e:
            logger.error(f"Error reading sheet data: {e}")
            raise
    
    def get_sheet_url(self, spreadsheet_id):
        """Get the URL of the spreadsheet"""
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
    
    def store_news_data(self, spreadsheet_id, news_data, source=None):
        """Store news data in the appropriate sheet with deduplication"""
        try:
            sheet_name = source or 'News_Data'
            headers = ['Title', 'URL', 'Date', 'Content', 'Source', 'Scraped_At']
            
            # Check if sheet exists, create if not
            try:
                existing_data = self.read_sheet_data(spreadsheet_id, sheet_name)
            except:
                self.create_sheet_with_headers(spreadsheet_id, sheet_name, headers)
                existing_data = [headers]  # Just headers
            
            # Get existing URLs for deduplication
            existing_urls = set()
            if len(existing_data) > 1:  # More than just headers
                url_column_index = 1  # URL is the second column (index 1)
                for row in existing_data[1:]:  # Skip header row
                    if len(row) > url_column_index and row[url_column_index]:
                        existing_urls.add(row[url_column_index].strip())
            
            # Prepare data rows, filtering out duplicates
            new_rows = []
            skipped_count = 0
            
            for item in news_data:
                url = item.get('url', '').strip()
                if url and url not in existing_urls:
                    row = [
                        item.get('title', ''),
                        url,
                        item.get('date', ''),
                        item.get('content', ''),
                        item.get('source', source or ''),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ]
                    new_rows.append(row)
                    existing_urls.add(url)  # Add to set to prevent duplicates in this batch
                else:
                    skipped_count += 1
            
            if new_rows:
                self.append_data(spreadsheet_id, sheet_name, new_rows)
                logger.info(f'Stored {len(new_rows)} new items in {sheet_name}')
                if skipped_count > 0:
                    logger.info(f'Skipped {skipped_count} duplicate items')
            else:
                logger.info(f'No new items to store in {sheet_name} (all duplicates)')
            
            return len(new_rows)
            
        except Exception as e:
            logger.error(f"Error storing news data: {e}")
            raise
    
    def get_all_news_data(self, spreadsheet_id):
        """Get all news data from all sheets"""
        try:
            # Get list of all sheets
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()
            
            sheets = spreadsheet.get('sheets', [])
            all_news = []
            
            for sheet in sheets:
                sheet_name = sheet['properties']['title']
                if sheet_name == 'Sheet1':  # Skip default sheet
                    continue
                
                try:
                    data = self.read_sheet_data(spreadsheet_id, sheet_name)
                    if data and len(data) > 1:  # Skip if only headers or empty
                        headers = data[0]
                        for row in data[1:]:
                            if row:  # Skip empty rows
                                news_item = {}
                                for i, header in enumerate(headers):
                                    if i < len(row):
                                        news_item[header.lower().replace(' ', '_')] = row[i]
                                    else:
                                        news_item[header.lower().replace(' ', '_')] = ''
                                all_news.append(news_item)
                except Exception as e:
                    logger.error(f"Error reading sheet {sheet_name}: {e}")
                    continue
            
            return all_news
        
        except Exception as e:
            logger.error(f"Error getting all news data: {e}")
            raise