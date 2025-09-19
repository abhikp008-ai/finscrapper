#!/usr/bin/env python3
"""
Manual Google OAuth Authentication

This script generates an authorization URL for you to visit manually.
"""

from google_auth_oauthlib.flow import Flow
import pickle
import json

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file'
]

def generate_auth_url():
    """Generate OAuth authorization URL"""
    try:
        # Check if this is web or installed app credentials
        with open('google_credentials.json', 'r') as f:
            creds_data = json.load(f)
        
        if 'web' in creds_data:
            # Web application flow  
            flow = Flow.from_client_secrets_file(
                'google_credentials.json',
                scopes=SCOPES,
                redirect_uri='http://localhost'  # Use configured redirect URI
            )
        else:
            # Desktop application flow
            from google_auth_oauthlib.flow import InstalledAppFlow
            flow = InstalledAppFlow.from_client_secrets_file('google_credentials.json', SCOPES)
        
        auth_url, _ = flow.authorization_url(prompt='consent')
        
        print("=== Google Sheets OAuth Authorization ===")
        print()
        print("Please visit this URL in your browser:")
        print(f"{auth_url}")
        print()
        if 'web' in creds_data:
            print("After authorization, you'll see an authorization code.")
            print("Copy the authorization code and use it in the next step.")
        else:
            print("After authorization, you'll be redirected to a localhost URL like:")
            print("http://localhost:8080/?state=...&code=...")
            print("Copy the ENTIRE redirect URL and use it in the next step.")
        
        return flow
    except Exception as e:
        print(f"Error generating auth URL: {e}")
        return None

def complete_auth_with_code(code):
    """Complete authentication with authorization code"""
    try:
        # Check if this is web or installed app credentials
        with open('google_credentials.json', 'r') as f:
            creds_data = json.load(f)
        
        if 'web' in creds_data:
            # Web application flow
            flow = Flow.from_client_secrets_file(
                'google_credentials.json',
                scopes=SCOPES,
                redirect_uri='http://localhost'
            )
        else:
            # Desktop application flow
            from google_auth_oauthlib.flow import InstalledAppFlow
            flow = InstalledAppFlow.from_client_secrets_file('google_credentials.json', SCOPES)
        
        flow.fetch_token(code=code)
        
        # Save credentials
        with open('token.pickle', 'wb') as token:
            pickle.dump(flow.credentials, token)
        
        print("✅ Authentication successful!")
        print("✅ Token saved to token.pickle")
        return True
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return False

if __name__ == "__main__":
    print("Manual Google OAuth Setup")
    print("=" * 50)
    
    flow = generate_auth_url()
    if flow:
        print("\nNext steps:")
        print("1. Visit the URL above in your browser")
        print("2. Sign in and authorize the application")
        print("3. Copy the redirect URL")
        print("4. Extract the 'code' parameter from the URL")
        print("5. Run: python -c \"from manual_auth import complete_auth_with_code; complete_auth_with_code('YOUR_CODE_HERE')\"")