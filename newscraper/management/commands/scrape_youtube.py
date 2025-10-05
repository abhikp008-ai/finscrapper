import os
import csv
import logging
import pandas as pd
from datetime import datetime
from django.core.management.base import BaseCommand
from django.utils import timezone
from yt_dlp import YoutubeDL
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
from newscraper.models import YouTubeScrapingJob
from newscraper.s3_storage_service import S3StorageService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Scrape YouTube videos and transcripts for a given keyword'

    def add_arguments(self, parser):
        parser.add_argument(
            '--job-id',
            type=int,
            required=True,
            help='ID of the YouTubeScrapingJob to process'
        )

    def handle(self, *args, **options):
        job_id = options['job_id']
        
        try:
            job = YouTubeScrapingJob.objects.get(id=job_id)
        except YouTubeScrapingJob.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Job with ID {job_id} does not exist'))
            return

        self.stdout.write(self.style.SUCCESS(f'Starting YouTube scraping for keyword: "{job.keyword}"'))
        
        try:
            s3_service = S3StorageService()
            
            job.status = 'searching'
            job.started_at = timezone.now()
            job.save()
            
            videos_data = self.search_youtube_videos(job.keyword)
            
            if not videos_data:
                raise Exception(
                    'No videos found. YouTube may be blocking datacenter IPs. '
                    'This feature works best on local deployment or residential IP addresses. '
                    'For Replit, consider using Google YouTube Data API v3 instead of yt-dlp.'
                )
            
            videos_s3_key = self.upload_videos_to_s3(s3_service, job, videos_data)
            job.videos_csv_path = videos_s3_key
            job.videos_found = len(videos_data)
            job.save()
            
            self.stdout.write(self.style.SUCCESS(f'Found {len(videos_data)} videos, uploaded to S3: {videos_s3_key}'))
            
            job.status = 'fetching_transcripts'
            job.save()
            
            transcripts_data = self.fetch_transcripts(videos_data)
            
            transcripts_s3_key = self.upload_transcripts_to_s3(s3_service, job, transcripts_data)
            job.transcripts_csv_path = transcripts_s3_key
            job.transcripts_fetched = len(transcripts_data)
            job.status = 'completed'
            job.completed_at = timezone.now()
            job.save()
            
            self.stdout.write(self.style.SUCCESS(
                f'Completed! Fetched {len(transcripts_data)} transcripts, uploaded to S3: {transcripts_s3_key}'
            ))
            
        except Exception as e:
            logger.error(f'YouTube scraping failed for job {job_id}: {str(e)}')
            job.status = 'failed'
            job.error_message = str(e)
            job.completed_at = timezone.now()
            job.save()
            self.stdout.write(self.style.ERROR(f'Failed: {str(e)}'))

    def search_youtube_videos(self, keyword, max_results=20):
        """Search YouTube and return first 20 video results"""
        self.stdout.write(f'Searching YouTube for: {keyword}')
        
        try:
            ydl_opts = {
                'quiet': False,
                'no_warnings': False,
                'noplaylist': True,
                'extract_flat': 'in_playlist',
                'extractor_args': {
                    'youtube': {
                        'player_client': ['ios', 'android', 'web'],
                        'skip': ['dash', 'hls']
                    }
                },
                'http_headers': {
                    'User-Agent': 'com.google.ios.youtube/19.29.1 (iPhone16,2; U; CPU iOS 17_5_1 like Mac OS X;)',
                }
            }
            
            with YoutubeDL(ydl_opts) as ydl:
                self.stdout.write(f'Extracting video information...')
                result = ydl.extract_info(
                    f"ytsearch{max_results}:{keyword}",
                    download=False
                )
                
                videos_data = []
                if result and 'entries' in result:
                    self.stdout.write(f'Found {len(result["entries"])} entries')
                    for idx, video in enumerate(result['entries'], 1):
                        if video:
                            self.stdout.write(f'Processing video {idx}: {video.get("title", "Unknown")[:50]}...')
                            videos_data.append({
                                'title': video.get('title', 'N/A'),
                                'url': video.get('webpage_url') or video.get('url') or f"https://www.youtube.com/watch?v={video.get('id', '')}",
                                'video_id': video.get('id', 'N/A'),
                                'duration': self._format_duration(video.get('duration', 0)),
                                'channel': video.get('uploader', video.get('channel', 'N/A')),
                                'views': self._format_views(video.get('view_count', 0))
                            })
                else:
                    self.stdout.write('No entries found in result')
                
                return videos_data
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error in YouTube search: {str(e)}'))
            raise Exception(f'YouTube search failed: {str(e)}')
    
    def _format_duration(self, duration_seconds):
        """Format duration from seconds to readable format"""
        if not duration_seconds:
            return 'N/A'
        hours = duration_seconds // 3600
        minutes = (duration_seconds % 3600) // 60
        seconds = duration_seconds % 60
        if hours:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        return f"{minutes}:{seconds:02d}"
    
    def _format_views(self, view_count):
        """Format view count to readable format"""
        if not view_count:
            return 'N/A'
        if view_count >= 1000000:
            return f"{view_count / 1000000:.1f}M views"
        elif view_count >= 1000:
            return f"{view_count / 1000:.1f}K views"
        return f"{view_count} views"

    def upload_videos_to_s3(self, s3_service, job, videos_data):
        """Upload video list to S3 as CSV"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        df = pd.DataFrame(videos_data)
        df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['keyword'] = job.keyword
        df['job_id'] = job.id
        
        s3_key = f"{s3_service.prefix}/{s3_service.env}/youtube/videos/youtube_videos_{job.id}_{timestamp}.csv"
        
        s3_service._upload_csv_to_s3(df, s3_key)
        self.stdout.write(self.style.SUCCESS(f'Uploaded videos CSV to S3: {s3_key}'))
        
        return s3_key

    def fetch_transcripts(self, videos_data):
        """Fetch transcripts for each video"""
        transcripts_data = []
        
        for idx, video in enumerate(videos_data, 1):
            video_id = video['video_id']
            self.stdout.write(f'Fetching transcript {idx}/{len(videos_data)}: {video["title"][:50]}...')
            
            try:
                transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
                
                transcript_text = ' '.join([entry['text'] for entry in transcript_list])
                
                transcripts_data.append({
                    'video_title': video['title'],
                    'video_url': video['url'],
                    'video_id': video_id,
                    'transcript_text': transcript_text,
                    'transcript_length': len(transcript_text)
                })
                
                self.stdout.write(self.style.SUCCESS(f'  ✓ Transcript fetched ({len(transcript_text)} chars)'))
                
            except (TranscriptsDisabled, NoTranscriptFound) as e:
                self.stdout.write(self.style.WARNING(f'  ✗ Transcript not available: {str(e)}'))
                continue
            except Exception as e:
                self.stdout.write(self.style.WARNING(f'  ✗ Error: {str(e)}'))
                continue
        
        return transcripts_data

    def upload_transcripts_to_s3(self, s3_service, job, transcripts_data):
        """Upload transcripts to S3 as CSV"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        df = pd.DataFrame(transcripts_data)
        df['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['keyword'] = job.keyword
        df['job_id'] = job.id
        
        s3_key = f"{s3_service.prefix}/{s3_service.env}/youtube/transcripts/youtube_transcripts_{job.id}_{timestamp}.csv"
        
        s3_service._upload_csv_to_s3(df, s3_key)
        self.stdout.write(self.style.SUCCESS(f'Uploaded transcripts CSV to S3: {s3_key}'))
        
        return s3_key
