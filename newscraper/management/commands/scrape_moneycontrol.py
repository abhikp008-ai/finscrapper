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
    help = 'Scrape news articles from MoneyControl'

    def add_arguments(self, parser):
        parser.add_argument(
            '--max-pages',
            type=int,
            default=3,
            help='Maximum number of pages to scrape per category',
        )

    def handle(self, *args, **options):
        max_pages = options['max_pages']
        categories = ["business", "economy", "markets", "trends"]
        
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
                self.stdout.write(f'Scraping MoneyControl category: {category}')
                articles = self.scrape_category(category, max_pages)
                all_articles.extend(articles)
                total_scraped += len(articles)
                self.stdout.write(f'Scraped {len(articles)} articles from {category}')
            
            # Store all articles in Google Sheets
            if all_articles:
                sheets_service.store_news_data(spreadsheet_id, all_articles, 'MoneyControl')
            
            self.stdout.write(
                self.style.SUCCESS(f'Successfully scraped {total_scraped} articles from MoneyControl to Google Sheets')
            )
            self.stdout.write(f'View data at: {sheets_service.get_sheet_url(spreadsheet_id)}')
            
        except Exception as e:
            logger.error(f'Failed to scrape MoneyControl: {e}')
            self.stdout.write(
                self.style.ERROR(f'Failed to scrape MoneyControl: {e}')
            )

    def scrape_category(self, category, max_pages):
        base_url = "https://www.moneycontrol.com/news"
        articles = []
        
        for page in range(1, max_pages + 1):
            url = f"{base_url}/{category}/page-{page}/" if page > 1 else f"{base_url}/{category}/"
            
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                resp = httpx.get(url, timeout=20, headers=headers)
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
        
        for article in soup.select("li.clearfix"):
            title_tag = article.find("h2")
            link_tag = article.find("a")
            
            if not title_tag or not link_tag:
                continue
                
            url = link_tag["href"]
            title = title_tag.get_text(strip=True)
            
            # Fetch full article content
            content = self.extract_article_content(url)
            
            posts.append({
                "title": title,
                "url": url,
                "date": timezone.now().strftime('%Y-%m-%d'),
                "content": content,
                "source": "moneycontrol"
            })
            
        return posts

    def extract_article_content(self, url):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            resp = httpx.get(url, timeout=20, headers=headers)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Try different containers for article content
            article_div = soup.find("div", class_="article_page")
            if not article_div:
                article_div = soup.find("div", id="contentdata")
            
            if article_div:
                paragraphs = [p.get_text(strip=True) for p in article_div.find_all("p")]
                return "\n\n".join(paragraphs)
                
            return ""
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {e}")
            return ""

