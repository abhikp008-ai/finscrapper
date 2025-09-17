from django.core.management.base import BaseCommand
from django.utils import timezone
import httpx
import logging
import time
import random
from bs4 import BeautifulSoup
from newscraper.models import Article, ScrapingJob

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
        
        job = ScrapingJob.objects.create(
            source='livemint',
            started_at=timezone.now(),
            status='running',
            created_by_id=1
        )
        
        try:
            scraped = self.scrape_news(max_pages)
            
            job.articles_scraped = scraped
            job.status = 'completed'
            job.completed_at = timezone.now()
            job.save()
            
            self.stdout.write(
                self.style.SUCCESS(f'Successfully scraped {scraped} articles from LiveMint')
            )
            
        except Exception as e:
            job.status = 'failed'
            job.error_message = str(e)
            job.completed_at = timezone.now()
            job.save()
            
            self.stdout.write(
                self.style.ERROR(f'Failed to scrape LiveMint: {e}')
            )

    def scrape_news(self, max_pages):
        base_url = "https://www.livemint.com/latest-news"
        scraped_count = 0
        
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
                
                for post in posts:
                    if self.save_article(post):
                        scraped_count += 1
                        
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
                
        return scraped_count

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
                    "category": "News",
                    "content": content
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

    def save_article(self, post_data):
        try:
            article, created = Article.objects.get_or_create(
                url=post_data['url'],
                defaults={
                    'title': post_data['title'],
                    'category': post_data['category'],
                    'content': post_data['content'],
                    'source': 'livemint',
                    'scraped_at': timezone.now(),
                }
            )
            return created
        except Exception as e:
            logger.error(f"Error saving article {post_data['url']}: {e}")
            return False