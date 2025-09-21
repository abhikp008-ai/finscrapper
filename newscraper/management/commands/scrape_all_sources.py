from django.core.management.base import BaseCommand
from django.core.management import call_command
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Run all news scrapers (MoneyControl, LiveMint, FinancialExpress)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--max-pages',
            type=int,
            default=2,
            help='Maximum number of pages to scrape per source',
        )

    def handle(self, *args, **options):
        max_pages = options['max_pages']
        
        self.stdout.write('🚀 Starting comprehensive news scraping...')
        self.stdout.write(f'📄 Max pages per source: {max_pages}')
        self.stdout.write('=' * 60)
        
        scrapers = [
            ('scrape_moneycontrol', 'MoneyControl'),
            ('scrape_livemint', 'LiveMint'),
            ('scrape_financialexpress', 'Financial Express')
        ]
        
        total_success = 0
        total_failed = 0
        
        for command_name, source_name in scrapers:
            try:
                self.stdout.write(f'\\n📰 Running {source_name} scraper...')
                call_command(command_name, max_pages=max_pages)
                self.stdout.write(self.style.SUCCESS(f'✅ {source_name} completed successfully'))
                total_success += 1
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ {source_name} failed: {e}'))
                logger.error(f'Failed to scrape {source_name}: {e}')
                total_failed += 1
        
        self.stdout.write('\\n' + '=' * 60)
        self.stdout.write(f'📊 Scraping Summary:')
        self.stdout.write(f'   ✅ Successful: {total_success}/{len(scrapers)}')
        self.stdout.write(f'   ❌ Failed: {total_failed}/{len(scrapers)}')
        
        if total_success > 0:
            self.stdout.write('\\n🔍 Checking S3 storage status...')
            try:
                from newscraper.s3_storage_service import S3StorageService
                storage = S3StorageService()
                info = storage.get_storage_info()
                all_data = storage.get_all_news_data()
                
                self.stdout.write(f'📊 Total articles in S3: {len(all_data)}')
                self.stdout.write(f'📄 CSV files in S3: {info.get("total_files", 0)}')
                self.stdout.write(f'💾 Total storage size: {info.get("total_size_mb", 0)} MB')
                
            except Exception as e:
                self.stdout.write(self.style.WARNING(f'⚠️ Could not check S3 status: {e}'))
        
        if total_success == len(scrapers):
            self.stdout.write(self.style.SUCCESS('\\n🎉 All scrapers completed successfully!'))
        elif total_success > 0:
            self.stdout.write(self.style.WARNING(f'\\n⚠️ Partial success: {total_success}/{len(scrapers)} scrapers completed'))
        else:
            self.stdout.write(self.style.ERROR('\\n💥 All scrapers failed!'))