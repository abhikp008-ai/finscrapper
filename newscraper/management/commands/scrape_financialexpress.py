from django.core.management.base import BaseCommand
from django.utils import timezone
import httpx
import logging
from bs4 import BeautifulSoup
from newscraper.models import Article, ScrapingJob

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
        
        job = ScrapingJob.objects.create(
            source='financialexpress',
            started_at=timezone.now(),
            status='running',
            created_by_id=1
        )
        
        try:
            total_scraped = 0
            for category in categories:
                self.stdout.write(f'Scraping Financial Express category: {category}')
                scraped = self.scrape_category(category, max_pages)
                total_scraped += scraped
                self.stdout.write(f'Scraped {scraped} articles from {category}')
            
            job.articles_scraped = total_scraped
            job.status = 'completed'
            job.completed_at = timezone.now()
            job.save()
            
            self.stdout.write(
                self.style.SUCCESS(f'Successfully scraped {total_scraped} articles from Financial Express')
            )
            
        except Exception as e:
            job.status = 'failed'
            job.error_message = str(e)
            job.completed_at = timezone.now()
            job.save()
            
            self.stdout.write(
                self.style.ERROR(f'Failed to scrape Financial Express: {e}')
            )

    def scrape_category(self, category, max_pages):
        base_url = "https://www.financialexpress.com"
        scraped_count = 0
        
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
                
                for post in posts:
                    if self.save_article(post):
                        scraped_count += 1
                        
            except Exception as e:
                logger.error(f"Error fetching page {page} for {category}: {e}")
                break
                
        return scraped_count

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
                    "category": category,
                    "content": content
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

    def save_article(self, post_data):
        try:
            article, created = Article.objects.get_or_create(
                url=post_data['url'],
                defaults={
                    'title': post_data['title'],
                    'category': post_data['category'],
                    'content': post_data['content'],
                    'source': 'financialexpress',
                    'scraped_at': timezone.now(),
                }
            )
            return created
        except Exception as e:
            logger.error(f"Error saving article {post_data['url']}: {e}")
            return False