from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from django.db.models.signals import post_save
from django.dispatch import receiver


class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    can_monitor = models.BooleanField(default=True)
    can_download = models.BooleanField(default=False)
    created_at = models.DateTimeField(default=timezone.now)
    
    def __str__(self):
        return f"{self.user.username} - Monitor: {self.can_monitor}, Download: {self.can_download}"


class Article(models.Model):
    SOURCE_CHOICES = [
        ('moneycontrol', 'MoneyControl'),
        ('financialexpress', 'Financial Express'),
        ('livemint', 'LiveMint'),
        ('cnbc', 'CNBC'),
        ('businessstandard', 'Business Standard'),
    ]
    
    title = models.CharField(max_length=500)
    url = models.URLField(unique=True)
    category = models.CharField(max_length=100)
    content = models.TextField()
    source = models.CharField(max_length=20, choices=SOURCE_CHOICES)
    scraped_at = models.DateTimeField(default=timezone.now)
    published_at = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        ordering = ['-scraped_at']
        
    def __str__(self):
        return f"{self.title} - {self.source}"


class ScrapingJob(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    source = models.CharField(max_length=20, choices=Article.SOURCE_CHOICES)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    articles_scraped = models.IntegerField(default=0)
    error_message = models.TextField(blank=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    
    class Meta:
        ordering = ['-started_at']
        
    def __str__(self):
        return f"{self.source} - {self.status} ({self.articles_scraped} articles)"

class YouTubeScrapingJob(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('searching', 'Searching Videos'),
        ('fetching_transcripts', 'Fetching Transcripts'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    keyword = models.CharField(max_length=255)
    status = models.CharField(max_length=30, choices=STATUS_CHOICES, default='pending')
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    videos_found = models.IntegerField(default=0)
    transcripts_fetched = models.IntegerField(default=0)
    videos_csv_path = models.CharField(max_length=500, blank=True)
    transcripts_csv_path = models.CharField(max_length=500, blank=True)
    error_message = models.TextField(blank=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(default=timezone.now)
    
    class Meta:
        ordering = ['-created_at']
        
    def __str__(self):
        return f"{self.keyword} - {self.status} ({self.videos_found} videos, {self.transcripts_fetched} transcripts)"



@receiver(post_save, sender=User)
def ensure_user_profile(sender, instance, **kwargs):
    """Create UserProfile for user if it doesn't exist (idempotent)"""
    UserProfile.objects.get_or_create(user=instance)
