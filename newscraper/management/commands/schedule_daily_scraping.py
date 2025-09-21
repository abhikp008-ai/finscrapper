from django.core.management.base import BaseCommand
from django.core.management import call_command
import time
import threading
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Run daily scraping scheduler (alternative to cron for Replit)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--target-hour',
            type=int,
            default=7,
            help='Target hour for daily scraping (IST, default: 7 for 7:00 AM)',
        )

    def handle(self, *args, **options):
        target_hour = options['target_hour']
        
        self.stdout.write(f'🕐 Starting daily scraping scheduler for {target_hour}:00 AM IST')
        self.stdout.write('⚠️ Note: This keeps running in the background. Use Ctrl+C to stop.')
        
        def run_daily_scraping():
            """Run the daily scraping task"""
            try:
                self.stdout.write(f'\\n🚀 Starting scheduled scraping at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
                call_command('scrape_all_sources', max_pages=3)
                self.stdout.write('✅ Scheduled scraping completed successfully')
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Scheduled scraping failed: {e}'))
                logger.error(f'Scheduled scraping failed: {e}')
        
        def schedule_next_run():
            """Calculate when to run next scraping"""
            now = datetime.now()
            
            # Calculate next target time (IST)
            # For simplicity, we'll run every 24 hours from first execution
            next_run = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
            
            # If target time has passed today, schedule for tomorrow
            if now >= next_run:
                next_run += timedelta(days=1)
            
            wait_seconds = (next_run - now).total_seconds()
            
            self.stdout.write(f'⏰ Next scraping scheduled for: {next_run.strftime("%Y-%m-%d %H:%M:%S")}')
            self.stdout.write(f'⏳ Waiting {wait_seconds/3600:.1f} hours until next run...')
            
            # Wait and then run
            time.sleep(wait_seconds)
            run_daily_scraping()
            
            # Schedule next run
            schedule_next_run()
        
        try:
            # Run immediately first time
            self.stdout.write('🎯 Running initial scraping...')
            run_daily_scraping()
            
            # Then schedule daily runs
            schedule_next_run()
            
        except KeyboardInterrupt:
            self.stdout.write('\\n⏹️ Daily scraping scheduler stopped by user')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'💥 Scheduler error: {e}'))
            logger.error(f'Scheduler error: {e}')