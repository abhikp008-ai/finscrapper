from django.core.management.base import BaseCommand
from django.core.management import call_command


class Command(BaseCommand):
    help = 'Scrape news articles from all sources'

    def add_arguments(self, parser):
        parser.add_argument(
            '--max-pages',
            type=int,
            default=3,
            help='Maximum number of pages to scrape per category',
        )

    def handle(self, *args, **options):
        max_pages = options['max_pages']
        
        scrapers = [
            'scrape_moneycontrol',
            'scrape_financialexpress',
            'scrape_livemint',
        ]
        
        self.stdout.write('Starting to scrape all financial news sources...')
        
        for scraper in scrapers:
            self.stdout.write(f'Running {scraper}...')
            try:
                call_command(scraper, max_pages=max_pages)
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully completed {scraper}')
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Failed to run {scraper}: {e}')
                )
        
        self.stdout.write(
            self.style.SUCCESS('Completed scraping all sources')
        )