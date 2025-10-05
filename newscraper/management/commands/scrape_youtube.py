import os
import csv
import logging
from datetime import datetime
from django.core.management.base import BaseCommand
from django.utils import timezone
from yt_dlp import YoutubeDL
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import TranscriptsDisabled, NoTranscriptFound
from newscraper.models import YouTubeScrapingJob

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
            job.status = 'searching'
            job.started_at = timezone.now()
            job.save()
            
            videos_data = self.search_youtube_videos(job.keyword)
            
            if not videos_data:
                raise Exception('No videos found for the given keyword')
            
            videos_csv_path = self.save_videos_to_csv(job, videos_data)
            job.videos_csv_path = videos_csv_path
            job.videos_found = len(videos_data)
            job.save()
            
            self.stdout.write(self.style.SUCCESS(f'Found {len(videos_data)} videos, saved to {videos_csv_path}'))
            
            job.status = 'fetching_transcripts'
            job.save()
            
            transcripts_data = self.fetch_transcripts(videos_data)
            
            transcripts_csv_path = self.save_transcripts_to_csv(job, transcripts_data)
            job.transcripts_csv_path = transcripts_csv_path
            job.transcripts_fetched = len(transcripts_data)
            job.status = 'completed'
            job.completed_at = timezone.now()
            job.save()
            
            self.stdout.write(self.style.SUCCESS(
                f'Completed! Fetched {len(transcripts_data)} transcripts, saved to {transcripts_csv_path}'
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
                'quiet': True,
                'no_warnings': True,
                'noplaylist': True,
                'extract_flat': False
            }
            
            with YoutubeDL(ydl_opts) as ydl:
                result = ydl.extract_info(
                    f"ytsearch{max_results}:{keyword}",
                    download=False
                )
                
                videos_data = []
                if result and 'entries' in result:
                    for video in result['entries']:
                        if video:
                            videos_data.append({
                                'title': video.get('title', 'N/A'),
                                'url': f"https://www.youtube.com/watch?v={video.get('id', '')}",
                                'video_id': video.get('id', 'N/A'),
                                'duration': self._format_duration(video.get('duration', 0)),
                                'channel': video.get('uploader', 'N/A'),
                                'views': self._format_views(video.get('view_count', 0))
                            })
                
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

    def save_videos_to_csv(self, job, videos_data):
        """Save video list to CSV file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'youtube_videos_list_{job.id}_{timestamp}.csv'
        filepath = os.path.join('media', 'youtube_scrapes', filename)
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['title', 'url', 'video_id', 'duration', 'channel', 'views']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for video in videos_data:
                writer.writerow(video)
        
        return filepath

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

    def save_transcripts_to_csv(self, job, transcripts_data):
        """Save transcripts to CSV file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'youtube_video_transcripts_{job.id}_{timestamp}.csv'
        filepath = os.path.join('media', 'youtube_scrapes', filename)
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['video_title', 'video_url', 'video_id', 'transcript_text', 'transcript_length']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for transcript in transcripts_data:
                writer.writerow(transcript)
        
        return filepath
