from django.core.management.base import BaseCommand
from django.utils import timezone
import httpx
import logging
import time
import random
from bs4 import BeautifulSoup
from newscraper.google_sheets_service import GoogleSheetsService
from newscraper.sheets_config import get_or_create_spreadsheet_id, save_spreadsheet_id, SPREADSHEET_NAME
import os

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Scrape news articles from LiveMint'

    def add_arguments(self, parser):
        parser.add_argument(
            '--max-pages',
            type=int,
            default=3,
            help='Maximum number of pages to scrape',
        )

    def handle(self, *args, **options):
        max_pages = options['max_pages']
        
        try:
            # Initialize Google Sheets service
            sheets_service = GoogleSheetsService()
            
            # Get or create spreadsheet
            spreadsheet_id = get_or_create_spreadsheet_id()
            if not spreadsheet_id:
                spreadsheet_id = sheets_service.create_spreadsheet(SPREADSHEET_NAME)
                save_spreadsheet_id(spreadsheet_id)
                self.stdout.write(f'Created new spreadsheet: {sheets_service.get_sheet_url(spreadsheet_id)}')
            
            articles = self.scrape_news(max_pages)
            
            # Store articles in Google Sheets
            if articles:
                sheets_service.store_news_data(spreadsheet_id, articles, 'LiveMint')
            
            self.stdout.write(
                self.style.SUCCESS(f'Successfully scraped {len(articles)} articles from LiveMint to Google Sheets')
            )
            self.stdout.write(f'View data at: {sheets_service.get_sheet_url(spreadsheet_id)}')
            
        except Exception as e:
            logger.error(f'Failed to scrape LiveMint: {e}')
            self.stdout.write(
                self.style.ERROR(f'Failed to scrape LiveMint: {e}')
            )

    def scrape_news(self, max_pages):
        base_url = "https://www.livemint.com/latest-news"
        articles = []
        
        # User agents for rotation
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        ]
        
        for page in range(2, max_pages + 1):
            url = f"{base_url}/page-{page}" if page > 1 else base_url
            
            try:
                # Polite crawling
                time.sleep(random.uniform(1.5, 3.5))
                
                headers = {"User-Agent": random.choice(user_agents)}
                resp = httpx.get(url, timeout=30, headers=headers, follow_redirects=True)
                
                if resp.status_code != 200:
                    logger.warning(f"Page {page} returned {resp.status_code}")
                    break
                
                posts = self.extract_posts_from_page(resp.text)
                if not posts:
                    logger.info(f"No posts found on page {page}")
                    break
                
                articles.extend(posts)
                        
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
                
        return articles

    def extract_posts_from_page(self, html):
        soup = BeautifulSoup(html, "html.parser")
        posts = []
        
        for article in soup.find_all("div", class_="headlineSec"):
            link_tag = article.find("a")
            if not link_tag:
                continue
                
            url = link_tag["href"]
            if not url.startswith("http"):
                url = "https://www.livemint.com" + url
                
            title = link_tag.get_text(strip=True)
            content = self.extract_article_content(url)
            
            if content:
                posts.append({
                    "title": title,
                    "url": url,
                    "date": timezone.now().strftime('%Y-%m-%d'),
                    "content": content,
                    "source": "livemint"
                })
            
        return posts

    def extract_article_content(self, url):
        try:
            time.sleep(random.uniform(1.5, 3.5))
            
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15"
            ]
            
            headers = {"User-Agent": random.choice(user_agents)}
            resp = httpx.get(url, timeout=30, headers=headers, follow_redirects=True)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            article_div = soup.find("div", class_="story-content") \
                          or soup.find("div", class_="contentSec") \
                          or soup.find("div", {"id": "mainContent"})
            
            if article_div:
                paragraphs = [p.get_text(strip=True) for p in article_div.find_all("p")]
                return "\n\n".join(paragraphs)
                
            return ""
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {e}")
            return ""

