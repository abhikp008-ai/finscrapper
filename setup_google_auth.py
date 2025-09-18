#!/usr/bin/env python3
"""
Google Sheets OAuth Setup Helper

This script helps set up Google Sheets authentication for the news scraper.
Run this script once to complete the OAuth flow and generate the token.pickle file.

Usage:
    python setup_google_auth.py

Requirements:
    - google_credentials.json file with your OAuth client credentials
    - Google API libraries installed
"""

import os
import sys
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Scopes for Google Sheets and Drive APIs
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]

def setup_oauth():
    """Set up OAuth authentication for Google Sheets"""
    credentials_file = 'google_credentials.json'
    token_file = 'token.pickle'
    
    print("Google Sheets OAuth Setup")
    print("=" * 40)
    
    # Check if credentials file exists
    if not os.path.exists(credentials_file):
        print("❌ Error: google_credentials.json not found!")
        print("\nPlease:")
        print("1. Go to Google Cloud Console (https://console.cloud.google.com/)")
        print("2. Create a new project or select existing one")
        print("3. Enable Google Sheets API and Google Drive API")
        print("4. Create OAuth 2.0 credentials (Desktop application type)")
        print("5. Download the credentials and save as 'google_credentials.json'")
        return False
    
    print("✅ Found credentials file")
    
    # Check if token already exists
    if os.path.exists(token_file):
        print("✅ Token file already exists")
        
        # Try to load and validate existing token
        try:
            with open(token_file, 'rb') as token:
                creds = pickle.load(token)
            
            if creds and creds.valid:
                print("✅ Existing token is valid")
                return True
            elif creds and creds.expired and creds.refresh_token:
                print("🔄 Refreshing expired token...")
                creds.refresh(Request())
                
                # Save refreshed token
                with open(token_file, 'wb') as token:
                    pickle.dump(creds, token)
                print("✅ Token refreshed successfully")
                return True
        except Exception as e:
            print(f"⚠️  Token file corrupted: {e}")
            print("🔄 Will create new token...")
    
    # Need to create new token
    print("🔄 Starting OAuth flow...")
    print("\nThis will open a browser window for authentication.")
    print("Please:")
    print("1. Sign in with your Google account")
    print("2. Grant permissions to the application")
    print("3. Return to this terminal")
    
    input("\nPress Enter to continue...")
    
    try:
        flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
        
        # Try to run local server first (works in development)
        try:
            creds = flow.run_local_server(port=0, open_browser=True)
        except Exception as e:
            print(f"Local server method failed: {e}")
            print("\n🔄 Trying manual method...")
            
            # Fallback to manual method
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            auth_url, _ = flow.authorization_url(prompt='consent')
            
            print(f"\n📋 Please visit this URL to authorize the application:")
            print(f"{auth_url}")
            
            code = input("\nEnter the authorization code: ").strip()
            flow.fetch_token(code=code)
            creds = flow.credentials
        
        # Save the credentials
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
        
        print("✅ Authentication successful!")
        print("✅ Token saved to token.pickle")
        return True
        
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return False

if __name__ == "__main__":
    print("Google Sheets Authentication Setup")
    print("=" * 50)
    
    if setup_oauth():
        print("\n🎉 Setup complete!")
        print("\nYou can now run the scrapers:")
        print("  python manage.py scrape_moneycontrol --max-pages=1")
        print("  python manage.py scrape_all --max-pages=1")
    else:
        print("\n❌ Setup failed. Please check the errors above.")
        sys.exit(1)