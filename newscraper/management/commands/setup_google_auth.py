"""
Django management command for setting up Google Sheets authentication.
This handles the OAuth flow securely without committing credentials.
"""

import os
import json
import pickle
from django.core.management.base import BaseCommand, CommandError
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from google.auth.transport.requests import Request


class Command(BaseCommand):
    help = 'Set up Google Sheets authentication via OAuth'

    def add_arguments(self, parser):
        parser.add_argument(
            '--credentials-json',
            type=str,
            help='Path to Google OAuth credentials JSON file'
        )
        parser.add_argument(
            '--auth-code',
            type=str,
            help='Authorization code from OAuth flow'
        )
        parser.add_argument(
            '--generate-url',
            action='store_true',
            help='Generate authorization URL for manual OAuth'
        )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.SUCCESS('=== Google Sheets Authentication Setup ===\n')
        )

        credentials_file = options.get('credentials_json')
        auth_code = options.get('auth_code')
        generate_url = options.get('generate_url')

        if generate_url:
            self.generate_auth_url(credentials_file)
        elif auth_code:
            self.complete_auth(credentials_file, auth_code)
        else:
            self.show_help()

    def show_help(self):
        """Show setup instructions"""
        self.stdout.write(
            self.style.WARNING(
                'Google Sheets Authentication Setup\n\n'
                'Step 1: Get OAuth Credentials\n'
                '- Go to Google Cloud Console\n'
                '- Create OAuth 2.0 Client ID (Web Application)\n'
                '- Download the JSON credentials file\n\n'
                'Step 2: Generate Authorization URL\n'
                'python manage.py setup_google_auth --generate-url --credentials-json path/to/credentials.json\n\n'
                'Step 3: Complete Authentication\n'
                'python manage.py setup_google_auth --auth-code YOUR_CODE --credentials-json path/to/credentials.json\n'
            )
        )

    def generate_auth_url(self, credentials_file):
        """Generate OAuth authorization URL"""
        if not credentials_file:
            raise CommandError(
                'Please provide credentials file with --credentials-json'
            )

        if not os.path.exists(credentials_file):
            raise CommandError(f'Credentials file not found: {credentials_file}')

        try:
            # Determine flow type based on credentials
            with open(credentials_file, 'r') as f:
                creds_data = json.load(f)

            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive.file'
            ]

            if 'web' in creds_data:
                # Web application flow
                flow = Flow.from_client_secrets_file(
                    credentials_file,
                    scopes=scopes,
                    redirect_uri='http://localhost'
                )
            else:
                # Desktop application flow
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, 
                    scopes
                )

            auth_url, _ = flow.authorization_url(prompt='consent')

            self.stdout.write(
                self.style.SUCCESS(
                    f'✅ Authorization URL generated!\n\n'
                    f'Please visit this URL in your browser:\n'
                    f'{auth_url}\n\n'
                    f'After authorization, copy the authorization code and run:\n'
                    f'python manage.py setup_google_auth --auth-code YOUR_CODE --credentials-json {credentials_file}\n'
                )
            )

        except Exception as e:
            raise CommandError(f'Failed to generate auth URL: {e}')

    def complete_auth(self, credentials_file, auth_code):
        """Complete OAuth authentication with authorization code"""
        if not credentials_file:
            raise CommandError(
                'Please provide credentials file with --credentials-json'
            )

        if not auth_code:
            raise CommandError('Please provide authorization code with --auth-code')

        if not os.path.exists(credentials_file):
            raise CommandError(f'Credentials file not found: {credentials_file}')

        try:
            # Determine flow type
            with open(credentials_file, 'r') as f:
                creds_data = json.load(f)

            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive.file'
            ]

            if 'web' in creds_data:
                # Web application flow
                flow = Flow.from_client_secrets_file(
                    credentials_file,
                    scopes=scopes,
                    redirect_uri='http://localhost'
                )
            else:
                # Desktop application flow
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, 
                    scopes
                )

            # Exchange authorization code for tokens
            flow.fetch_token(code=auth_code)

            # Save credentials securely
            token_file = os.getenv('GOOGLE_TOKEN_FILE', 'token.pickle')
            with open(token_file, 'wb') as token:
                pickle.dump(flow.credentials, token)

            # Secure file permissions
            os.chmod(token_file, 0o600)

            self.stdout.write(
                self.style.SUCCESS(
                    f'✅ Authentication successful!\n'
                    f'✅ Credentials saved to: {token_file}\n'
                    f'✅ File permissions secured (600)\n\n'
                    f'Google Sheets service is now ready to use!\n'
                )
            )

        except Exception as e:
            raise CommandError(f'Authentication failed: {e}')

    def test_authentication(self):
        """Test if authentication is working"""
        try:
            from newscraper.google_sheets_service import GoogleSheetsService
            service = GoogleSheetsService()
            self.stdout.write(
                self.style.SUCCESS('✅ Google Sheets authentication is working!')
            )
        except Exception as e:
            raise CommandError(f'Authentication test failed: {e}')