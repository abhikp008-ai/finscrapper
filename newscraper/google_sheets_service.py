import json
import logging
from googleapiclient.discovery import build
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class GoogleSheetsService:
    def __init__(self):
        self.api_key = os.getenv('GOOGLE_SHEETS_API_KEY')
        self.service = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate using API key"""
        if not self.api_key:
            raise Exception(
                "Google Sheets API key not found. Please set the GOOGLE_SHEETS_API_KEY "
                "environment variable with your Google API key."
            )
        
        try:
            # Build service with API key authentication
            self.service = build('sheets', 'v4', developerKey=self.api_key)
            logger.info("Google Sheets service initialized with API key")
        except Exception as e:
            logger.error(f"Failed to initialize Google Sheets service: {e}")
            raise
    
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
        """Store news data in the appropriate sheet"""
        try:
            sheet_name = source or 'News_Data'
            headers = ['Title', 'URL', 'Date', 'Content', 'Source', 'Scraped_At']
            
            # Check if sheet exists, create if not
            try:
                self.read_sheet_data(spreadsheet_id, sheet_name, f"{sheet_name}!A1")
            except:
                self.create_sheet_with_headers(spreadsheet_id, sheet_name, headers)
            
            # Prepare data rows
            rows = []
            for item in news_data:
                row = [
                    item.get('title', ''),
                    item.get('url', ''),
                    item.get('date', ''),
                    item.get('content', ''),
                    item.get('source', source or ''),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                rows.append(row)
            
            if rows:
                self.append_data(spreadsheet_id, sheet_name, rows)
                logger.info(f'Stored {len(rows)} news items in {sheet_name}')
            
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