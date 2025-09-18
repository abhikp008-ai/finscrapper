from django.core.management.base import BaseCommand
from django.utils import timezone
import httpx
import logging
from bs4 import BeautifulSoup
from newscraper.google_sheets_service import GoogleSheetsService
from newscraper.sheets_config import get_or_create_spreadsheet_id, save_spreadsheet_id, SPREADSHEET_NAME
import os

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Scrape news articles from Financial Express'

    def add_arguments(self, parser):
        parser.add_argument(
            '--max-pages',
            type=int,
            default=3,
            help='Maximum number of pages to scrape per category',
        )

    def handle(self, *args, **options):
        max_pages = options['max_pages']
        categories = [
            "business", "market", "industry", "economy", "personal-finance",
            "opinion", "investing", "mutual-funds", "money", "auto", "technology"
        ]
        
        try:
            # Initialize Google Sheets service
            sheets_service = GoogleSheetsService()
            
            # Get or create spreadsheet
            spreadsheet_id = get_or_create_spreadsheet_id()
            if not spreadsheet_id:
                spreadsheet_id = sheets_service.create_spreadsheet(SPREADSHEET_NAME)
                save_spreadsheet_id(spreadsheet_id)
                self.stdout.write(f'Created new spreadsheet: {sheets_service.get_sheet_url(spreadsheet_id)}')
            
            all_articles = []
            total_scraped = 0
            
            for category in categories:
                self.stdout.write(f'Scraping Financial Express category: {category}')
                articles = self.scrape_category(category, max_pages)
                all_articles.extend(articles)
                total_scraped += len(articles)
                self.stdout.write(f'Scraped {len(articles)} articles from {category}')
            
            # Store all articles in Google Sheets
            if all_articles:
                sheets_service.store_news_data(spreadsheet_id, all_articles, 'FinancialExpress')
            
            self.stdout.write(
                self.style.SUCCESS(f'Successfully scraped {total_scraped} articles from Financial Express to Google Sheets')
            )
            self.stdout.write(f'View data at: {sheets_service.get_sheet_url(spreadsheet_id)}')
            
        except Exception as e:
            logger.error(f'Failed to scrape Financial Express: {e}')
            self.stdout.write(
                self.style.ERROR(f'Failed to scrape Financial Express: {e}')
            )

    def scrape_category(self, category, max_pages):
        base_url = "https://www.financialexpress.com"
        articles = []
        
        for page in range(2, max_pages + 1):
            url = f"{base_url}/{category}/page/{page}/"
            
            try:
                resp = httpx.get(url, timeout=20)
                if resp.status_code != 200:
                    logger.warning(f"Page {page} returned {resp.status_code}")
                    break
                
                posts = self.extract_posts_from_page(resp.text, category)
                if not posts:
                    logger.info(f"No posts found on page {page}")
                    break
                
                articles.extend(posts)
                        
            except Exception as e:
                logger.error(f"Error fetching page {page} for {category}: {e}")
                break
                
        return articles

    def extract_posts_from_page(self, html, category):
        soup = BeautifulSoup(html, "html.parser")
        posts = []
        
        for h2 in soup.find_all("h2", class_="entry-title"):
            link_tag = h2.find("a")
            if not link_tag:
                continue
                
            url = link_tag["href"]
            if not url.startswith("http"):
                url = "https://www.financialexpress.com" + url
                
            title = link_tag.get_text(strip=True)
            content = self.extract_article_content(url)
            
            if content:
                posts.append({
                    "title": title,
                    "url": url,
                    "date": timezone.now().strftime('%Y-%m-%d'),
                    "content": content,
                    "source": "financialexpress"
                })
            
        return posts

    def extract_article_content(self, url):
        try:
            resp = httpx.get(url, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            
            article_div = soup.find("div", class_="article-section") \
                          or soup.find("div", class_="post-content") \
                          or soup.find("div", class_="entry-content")
            
            if article_div:
                paragraphs = [p.get_text(strip=True) for p in article_div.find_all("p")]
                return "\n\n".join(paragraphs)
                
            return ""
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {e}")
            return ""

