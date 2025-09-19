from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.http import HttpResponse, JsonResponse
from django.core.paginator import Paginator
from django.utils import timezone
from django.core.management import call_command
from datetime import datetime, timedelta
import csv
import io
from .models import UserProfile
from typing import Any
from .google_sheets_service import GoogleSheetsService
from .sheets_config import get_or_create_spreadsheet_id, SPREADSHEET_NAME
from django.contrib.auth.models import User
import logging
import os

logger = logging.getLogger(__name__)


def home(request):
    if request.user.is_authenticated:
        return redirect('dashboard')
    return redirect('login')


def login_view(request):
    if request.method == 'POST':
        username = request.POST['username']
        password = request.POST['password']
        user = authenticate(request, username=username, password=password)
        if user:
            login(request, user)
            return redirect('dashboard')
        else:
            messages.error(request, 'Invalid credentials')
    return render(request, 'newscraper/login.html')


def can_monitor(user):
    """Check if user has monitor permission"""
    if user.is_superuser:
        return True
    try:
        return user.userprofile.can_monitor  # type: ignore
    except UserProfile.DoesNotExist:  # type: ignore
        return False


def can_download(user):
    """Check if user has download permission"""
    if user.is_superuser:
        return True
    try:
        return user.userprofile.can_download  # type: ignore
    except UserProfile.DoesNotExist:  # type: ignore
        return False


@login_required
@user_passes_test(can_monitor)
def dashboard(request):
    # Get filter parameters
    source = request.GET.get('source', '')
    category = request.GET.get('category', '')
    date_from = request.GET.get('date_from', '')
    date_to = request.GET.get('date_to', '')
    search = request.GET.get('search', '')
    
    try:
        # Get Google Sheets data
        sheets_service = GoogleSheetsService()
        spreadsheet_id = get_or_create_spreadsheet_id()
        
        if not spreadsheet_id:
            # No spreadsheet exists yet
            all_articles = []
            total_articles = 0
            available_sources = []
            available_categories = []
            filtered_articles = []
        else:
            all_articles = sheets_service.get_all_news_data(spreadsheet_id)
            
            # Apply filters
            filtered_articles = []
            for article in all_articles:
                # Source filter
                if source and article.get('source', '').lower() != source.lower():
                    continue
                
                # Category filter (if we had categories in sheets data)
                # Skip category filter for now as sheets data structure doesn't include it
                
                # Search filter
                if search:
                    search_lower = search.lower()
                    title = article.get('title', '').lower()
                    content = article.get('content', '').lower()
                    if search_lower not in title and search_lower not in content:
                        continue
                
                # Date filters
                article_date_str = article.get('date', '') or article.get('scraped_at', '')
                if date_from or date_to:
                    try:
                        if article_date_str:
                            # Try different date formats
                            article_date = None
                            for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']:
                                try:
                                    article_date = datetime.strptime(article_date_str, fmt).date()
                                    break
                                except ValueError:
                                    continue
                            
                            if article_date:
                                if date_from:
                                    try:
                                        date_from_parsed = datetime.strptime(date_from, '%Y-%m-%d').date()
                                        if article_date < date_from_parsed:
                                            continue
                                    except ValueError:
                                        pass
                                
                                if date_to:
                                    try:
                                        date_to_parsed = datetime.strptime(date_to, '%Y-%m-%d').date()
                                        if article_date > date_to_parsed:
                                            continue
                                    except ValueError:
                                        pass
                    except Exception:
                        # Skip articles with invalid dates if date filters are applied
                        if date_from or date_to:
                            continue
                
                filtered_articles.append(article)
            
            # Get statistics and filter options
            total_articles = len(all_articles)
            available_sources = list(set([article.get('source', '') for article in all_articles if article.get('source')]))
            available_categories = []  # Categories not used in sheets structure
        
        # Pagination
        paginator = Paginator(filtered_articles, 20)
        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)
        
        # Add spreadsheet URL for easy access
        spreadsheet_url = sheets_service.get_sheet_url(spreadsheet_id) if spreadsheet_id else None
        
    except Exception as e:
        logger.error(f"Error accessing Google Sheets data: {e}")
        messages.error(request, "Error loading data from Google Sheets. Please check your connection.")
        all_articles = []
        total_articles = 0
        available_sources = []
        available_categories = []
        spreadsheet_url = None
        filtered_articles = []
        
        # Create empty pagination
        paginator = Paginator(filtered_articles, 20)
        page_obj = paginator.get_page(1)
    
    context = {
        'page_obj': page_obj,
        'total_articles': total_articles,
        'spreadsheet_url': spreadsheet_url,
        'available_sources': available_sources,
        'available_categories': available_categories,
        'current_filters': {
            'source': source,
            'category': category,
            'date_from': date_from,
            'date_to': date_to,
            'search': search,
        },
        'can_download': can_download(request.user),
    }
    return render(request, 'newscraper/dashboard.html', context)


@login_required
@user_passes_test(can_download)
def download_articles(request):
    """Download articles as CSV from Google Sheets"""
    # Get filter parameters (same as dashboard)
    source = request.GET.get('source', '')
    category = request.GET.get('category', '')
    date_from = request.GET.get('date_from', '')
    date_to = request.GET.get('date_to', '')
    search = request.GET.get('search', '')
    
    try:
        # Get Google Sheets data with same filtering logic as dashboard
        sheets_service = GoogleSheetsService()
        spreadsheet_id = get_or_create_spreadsheet_id()
        
        if not spreadsheet_id:
            messages.error(request, "No data available. Please run scrapers first.")
            return redirect('dashboard')
        
        all_articles = sheets_service.get_all_news_data(spreadsheet_id)
        
        # Apply same filters as dashboard
        filtered_articles = []
        for article in all_articles:
            # Source filter
            if source and article.get('source', '').lower() != source.lower():
                continue
            
            # Search filter
            if search:
                search_lower = search.lower()
                title = article.get('title', '').lower()
                content = article.get('content', '').lower()
                if search_lower not in title and search_lower not in content:
                    continue
            
            # Date filters
            article_date_str = article.get('date', '') or article.get('scraped_at', '')
            if date_from or date_to:
                try:
                    if article_date_str:
                        article_date = None
                        for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S']:
                            try:
                                article_date = datetime.strptime(article_date_str, fmt).date()
                                break
                            except ValueError:
                                continue
                        
                        if article_date:
                            if date_from:
                                try:
                                    date_from_parsed = datetime.strptime(date_from, '%Y-%m-%d').date()
                                    if article_date < date_from_parsed:
                                        continue
                                except ValueError:
                                    pass
                            
                            if date_to:
                                try:
                                    date_to_parsed = datetime.strptime(date_to, '%Y-%m-%d').date()
                                    if article_date > date_to_parsed:
                                        continue
                                except ValueError:
                                    pass
                except Exception:
                    if date_from or date_to:
                        continue
            
            filtered_articles.append(article)
        
        # Create CSV response
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="finscrap_articles_{timezone.now().strftime("%Y%m%d_%H%M%S")}.csv"'
        
        writer = csv.writer(response)
        writer.writerow(['Title', 'URL', 'Source', 'Date', 'Scraped At', 'Content'])
        
        for i, article in enumerate(filtered_articles, 1):
            writer.writerow([
                article.get('title', ''),
                article.get('url', ''),
                article.get('source', ''),
                article.get('date', ''),
                article.get('scraped_at', ''),
                (article.get('content', '')[:500] + '...') if len(article.get('content', '')) > 500 else article.get('content', '')
            ])
        
        return response
        
    except Exception as e:
        logger.error(f"Error downloading articles from Google Sheets: {e}")
        messages.error(request, "Error downloading data. Please try again.")
        return redirect('dashboard')


@login_required
@user_passes_test(lambda u: u.is_superuser)
def manage_users(request):
    """User management for admin"""
    users = User.objects.all().select_related('userprofile')
    
    context = {
        'users': users,
    }
    return render(request, 'newscraper/manage_users.html', context)


@login_required  
@user_passes_test(lambda u: u.is_superuser)
def edit_user_permissions(request, user_id):
    """Edit user permissions"""
    target_user = get_object_or_404(User, id=user_id)
    
    if request.method == 'POST':
        can_monitor = request.POST.get('can_monitor') == 'on'
        can_download = request.POST.get('can_download') == 'on'
        
        # Update user profile
        profile, created = UserProfile.objects.get_or_create(user=target_user)
        profile.can_monitor = can_monitor
        profile.can_download = can_download
        profile.save()
        
        messages.success(request, f'Permissions updated for {target_user.username}')
        return redirect('manage_users')
    
    context = {
        'target_user': target_user,
        'profile': getattr(target_user, 'userprofile', None),
    }
    return render(request, 'newscraper/edit_user_permissions.html', context)


@login_required
@user_passes_test(can_monitor)
def run_scraper(request):
    """Manual scraper execution - now saves to Google Sheets"""
    if request.method == 'POST':
        scraper = request.POST.get('scraper')
        max_pages = int(request.POST.get('max_pages', 1))
        
        try:
            # Execute the management command with proper kwargs
            call_command(scraper, max_pages=max_pages)
            messages.success(request, f'Successfully completed {scraper} scraping ({max_pages} pages). Data saved to Google Sheets.')
        except Exception as e:
            logger.error(f'Error running {scraper}: {e}')
            messages.error(request, f'Error running {scraper}: {str(e)}')
        
        return redirect('dashboard')
    
    scrapers = [
        ('scrape_moneycontrol', 'MoneyControl'),
        ('scrape_financialexpress', 'Financial Express'), 
        ('scrape_livemint', 'LiveMint'),
        ('scrape_all', 'All Sources'),
    ]
    
    context = {
        'scrapers': scrapers,
    }
    return render(request, 'newscraper/run_scraper.html', context)


def logout_view(request):
    logout(request)
    return redirect('login')


def privacy_policy(request):
    """Display privacy policy page for Google Cloud API compliance"""
    return render(request, 'newscraper/privacy_policy.html')
