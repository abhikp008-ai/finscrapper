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
from .s3_storage_service import S3StorageService
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
        # Get S3 cloud storage data
        storage_service = S3StorageService()
        all_articles = storage_service.get_all_news_data()
        
        # Apply filters
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
        available_categories = []  # Categories not used in CSV structure
        
        # Pagination
        paginator = Paginator(filtered_articles, 20)
        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)
        
        # Get storage info
        storage_info = storage_service.get_storage_info()
        storage_url = storage_info.get('storage_path', '')
        
    except Exception as e:
        logger.error(f"Error accessing MEGA CSV storage data: {e}")
        messages.error(request, "Error loading data from MEGA CSV storage. Please check your storage.")
        all_articles = []
        total_articles = 0
        available_sources = []
        available_categories = []
        storage_url = None
        filtered_articles = []
        
        # Create empty pagination
        paginator = Paginator(filtered_articles, 20)
        page_obj = paginator.get_page(1)
    
    context = {
        'page_obj': page_obj,
        'total_articles': total_articles,
        'storage_url': storage_url,
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
    """Download articles as CSV from storage"""
    # Get filter parameters (same as dashboard)
    source = request.GET.get('source', '')
    category = request.GET.get('category', '')
    date_from = request.GET.get('date_from', '')
    date_to = request.GET.get('date_to', '')
    search = request.GET.get('search', '')
    
    try:
        # Get S3 cloud storage data with same filtering logic as dashboard
        storage_service = S3StorageService()
        all_articles = storage_service.get_all_news_data()
        
        if not all_articles:
            messages.error(request, "No data available. Please run scrapers first.")
            return redirect('dashboard')
        
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
        logger.error(f"Error downloading articles from MEGA CSV storage: {e}")
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


def safe_str_strip(value):
    """Safely convert value to string and strip whitespace"""
    if value is None:
        return ''
    return str(value).strip()


def normalize_stock_data(stock_dict):
    """Normalize stock dictionary keys for template compatibility"""
    normalized = {}
    
    # Basic stock info from NSE list (handle both with and without leading spaces)
    normalized['symbol'] = stock_dict.get('SYMBOL', '')
    normalized['company_name'] = stock_dict.get('NAME OF COMPANY', '')
    normalized['series'] = safe_str_strip(stock_dict.get('SERIES', stock_dict.get(' SERIES', '')))
    normalized['date_of_listing'] = safe_str_strip(stock_dict.get('DATE OF LISTING', stock_dict.get(' DATE OF LISTING', '')))
    normalized['paid_up_value'] = safe_str_strip(stock_dict.get('PAID UP VALUE', stock_dict.get(' PAID UP VALUE', '')))
    normalized['market_lot'] = safe_str_strip(stock_dict.get('MARKET LOT', stock_dict.get(' MARKET LOT', '')))
    normalized['isin_number'] = safe_str_strip(stock_dict.get('ISIN NUMBER', stock_dict.get(' ISIN NUMBER', '')))
    normalized['face_value'] = safe_str_strip(stock_dict.get('FACE VALUE', stock_dict.get(' FACE VALUE', '')))
    
    # Detailed financial data (from yfinance scraper)
    normalized['latest_price'] = stock_dict.get('latest_price', '')
    normalized['market_cap'] = stock_dict.get('market_cap', '')
    normalized['fifty_two_week_high'] = stock_dict.get('price_52w_high', stock_dict.get('fifty_two_week_high', ''))
    normalized['fifty_two_week_low'] = stock_dict.get('price_52w_low', stock_dict.get('fifty_two_week_low', ''))
    normalized['pe_ratio'] = stock_dict.get('pe_ratio', '')
    normalized['dividend_yield'] = stock_dict.get('dividend_yield', '')
    normalized['beta'] = stock_dict.get('beta', '')
    normalized['avg_volume'] = stock_dict.get('avg_volume_10y', stock_dict.get('avg_volume', ''))
    normalized['sector'] = stock_dict.get('sector', '')
    normalized['industry'] = stock_dict.get('industry', '')
    normalized['business_summary'] = stock_dict.get('business_summary', '')
    normalized['first_trading_date'] = stock_dict.get('first_trading_date', '')
    normalized['last_trading_date'] = stock_dict.get('last_trading_date', '')
    
    return normalized


@login_required
@user_passes_test(can_monitor)
def nse_stocks(request):
    """NSE stocks list page with filtering and search"""
    # Get filter parameters
    search = request.GET.get('search', '')
    sector = request.GET.get('sector', '')
    series = request.GET.get('series', '')
    sort_by = request.GET.get('sort_by', 'symbol')
    
    try:
        # Get NSE stock data from S3
        storage_service = S3StorageService()
        stock_list_df = storage_service.get_nse_stock_list()
        detailed_df = storage_service.get_nse_detailed_data()
        
        if stock_list_df is None or stock_list_df.empty:
            messages.warning(request, "No NSE stock data found. Please run the NSE scraper first.")
            stocks_data = []
            total_stocks = 0
            available_sectors = []
            available_series = []
        else:
            # Convert DataFrame to list of dictionaries
            raw_stocks_data = stock_list_df.to_dict('records')
            
            # Merge with detailed data if available
            if detailed_df is not None and not detailed_df.empty:
                detailed_dict = {row['symbol']: row for row in detailed_df.to_dict('records')}
                for stock in raw_stocks_data:
                    symbol = stock.get('SYMBOL', '')
                    if symbol in detailed_dict:
                        stock.update(detailed_dict[symbol])
            
            # Normalize all stock data for template compatibility
            stocks_data = [normalize_stock_data(stock) for stock in raw_stocks_data]
            
            # Apply filters
            filtered_stocks = []
            for stock in stocks_data:
                # Search filter (symbol, company name)
                if search:
                    search_lower = search.lower()
                    symbol = str(stock.get('symbol', '')).lower()
                    company = str(stock.get('company_name', '')).lower()
                    if search_lower not in symbol and search_lower not in company:
                        continue
                
                # Sector filter (using company name as sector indicator)
                if sector:
                    company_name = str(stock.get('company_name', '')).lower()
                    if sector.lower() not in company_name:
                        continue
                
                # Series filter
                if series and str(stock.get('series', '')).upper() != series.upper():
                    continue
                
                filtered_stocks.append(stock)
            
            # Sort stocks
            sort_key_map = {
                'symbol': 'symbol',
                'company': 'company_name',
                'market_cap': 'market_cap',
                'price': 'latest_price'
            }
            
            if sort_by in sort_key_map:
                key = sort_key_map[sort_by]
                # Handle numeric sorting for market cap and price
                if sort_by in ['market_cap', 'price']:
                    filtered_stocks.sort(key=lambda x: float(x.get(key, 0) or 0), reverse=True)
                else:
                    filtered_stocks.sort(key=lambda x: str(x.get(key, '')).upper())
            
            stocks_data = filtered_stocks
            total_stocks = len(stock_list_df)
            
            # Get available filter options
            available_series = list(set([str(stock.get('SERIES', '')) for stock in raw_stocks_data if stock.get('SERIES')]))
            available_sectors = ['Banking', 'IT', 'Pharma', 'Auto', 'Energy', 'FMCG', 'Telecom']  # Common sectors
        
        # Pagination
        paginator = Paginator(stocks_data, 50)  # 50 stocks per page
        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)
        
    except Exception as e:
        logger.error(f"Error accessing NSE stock data: {e}")
        messages.error(request, "Error loading NSE stock data. Please try again.")
        stocks_data = []
        total_stocks = 0
        available_sectors = []
        available_series = []
        
        # Create empty pagination
        paginator = Paginator(stocks_data, 50)
        page_obj = paginator.get_page(1)
    
    context = {
        'page_obj': page_obj,
        'total_stocks': total_stocks,
        'available_sectors': available_sectors,
        'available_series': available_series,
        'current_filters': {
            'search': search,
            'sector': sector,
            'series': series,
            'sort_by': sort_by,
        },
        'can_download': can_download(request.user),
    }
    return render(request, 'newscraper/nse_stocks.html', context)


@login_required
@user_passes_test(can_monitor)
def nse_stock_detail(request, symbol):
    """Individual NSE stock detail page"""
    try:
        # Get NSE stock data from S3
        storage_service = S3StorageService()
        stock_list_df = storage_service.get_nse_stock_list()
        detailed_df = storage_service.get_nse_detailed_data()
        
        if stock_list_df is None or stock_list_df.empty:
            messages.error(request, "No NSE stock data found. Please run the NSE scraper first.")
            return redirect('nse_stocks')
        
        # Find the specific stock
        raw_stock_data = None
        for stock in stock_list_df.to_dict('records'):
            if stock.get('SYMBOL', '').upper() == symbol.upper():
                raw_stock_data = stock.copy()
                break
        
        if not raw_stock_data:
            messages.error(request, f"Stock {symbol} not found.")
            return redirect('nse_stocks')
        
        # Merge with detailed data if available
        if detailed_df is not None and not detailed_df.empty:
            for detailed_stock in detailed_df.to_dict('records'):
                if detailed_stock.get('symbol', '').upper() == symbol.upper():
                    raw_stock_data.update(detailed_stock)
                    break
        
        # Normalize stock data for template compatibility
        stock_data = normalize_stock_data(raw_stock_data)
        
        # Get historical OHLCV data files from S3
        storage_info = storage_service.get_storage_info()
        historical_files = []
        for file_info in storage_info.get('files', []):
            if f'ohlcv/{symbol.upper()}/' in file_info['key']:
                historical_files.append({
                    'filename': file_info['key'].split('/')[-1],
                    'size_mb': file_info['size_mb'],
                    'records': file_info.get('records', 'Unknown'),
                    'last_modified': file_info.get('last_modified', 'Unknown')
                })
        
        # Sort historical files by last modified date (newest first)
        historical_files.sort(key=lambda x: x.get('last_modified', ''), reverse=True)
        
    except Exception as e:
        logger.error(f"Error accessing NSE stock detail for {symbol}: {e}")
        messages.error(request, f"Error loading data for {symbol}. Please try again.")
        return redirect('nse_stocks')
    
    context = {
        'stock': stock_data,
        'symbol': symbol.upper(),
        'historical_files': historical_files,
        'can_download': can_download(request.user),
    }
    return render(request, 'newscraper/nse_stock_detail.html', context)


@login_required
@user_passes_test(can_download)
def download_nse_stocks(request):
    """Download NSE stocks data as CSV"""
    # Get filter parameters (same as nse_stocks view)
    search = request.GET.get('search', '')
    sector = request.GET.get('sector', '')
    series = request.GET.get('series', '')
    
    try:
        # Get NSE stock data from S3 with same filtering logic
        storage_service = S3StorageService()
        stock_list_df = storage_service.get_nse_stock_list()
        detailed_df = storage_service.get_nse_detailed_data()
        
        if stock_list_df is None or stock_list_df.empty:
            messages.error(request, "No NSE stock data available. Please run the NSE scraper first.")
            return redirect('nse_stocks')
        
        # Convert to records and merge with detailed data
        stocks_data = stock_list_df.to_dict('records')
        
        if detailed_df is not None and not detailed_df.empty:
            detailed_dict = {row['symbol']: row for row in detailed_df.to_dict('records')}
            for stock in stocks_data:
                symbol = stock.get('SYMBOL', '')
                if symbol in detailed_dict:
                    stock.update(detailed_dict[symbol])
        
        # Apply same filters as nse_stocks view
        filtered_stocks = []
        for stock in stocks_data:
            # Search filter
            if search:
                search_lower = search.lower()
                symbol = str(stock.get('SYMBOL', '')).lower()
                company = str(stock.get('NAME OF COMPANY', '')).lower()
                if search_lower not in symbol and search_lower not in company:
                    continue
            
            # Sector filter
            if sector:
                company_name = str(stock.get('NAME OF COMPANY', '')).lower()
                if sector.lower() not in company_name:
                    continue
            
            # Series filter
            if series and str(stock.get('SERIES', '')).upper() != series.upper():
                continue
            
            filtered_stocks.append(stock)
        
        # Create CSV response
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="nse_stocks_{timezone.now().strftime("%Y%m%d_%H%M%S")}.csv"'
        
        writer = csv.writer(response)
        
        # Write headers - include both basic and detailed data columns
        headers = ['Symbol', 'Company Name', 'Series', 'Date of Listing', 'Paid Up Value', 'Market Value', 'ISIN Number']
        if detailed_df is not None and not detailed_df.empty:
            headers.extend(['Latest Price', 'Market Cap', '52 Week High', '52 Week Low', 'PE Ratio', 'Dividend Yield', 'Beta'])
        
        writer.writerow(headers)
        
        # Normalize filtered stocks for consistent access
        normalized_stocks = [normalize_stock_data(stock) for stock in filtered_stocks]
        
        # Write data
        for stock in normalized_stocks:
            row = [
                stock.get('symbol', ''),
                stock.get('company_name', ''),
                stock.get('series', ''),
                stock.get('date_of_listing', ''),
                stock.get('paid_up_value', ''),
                stock.get('market_lot', ''),
                stock.get('isin_number', ''),
            ]
            
            # Add detailed data if available
            if detailed_df is not None and not detailed_df.empty:
                row.extend([
                    stock.get('latest_price', ''),
                    stock.get('market_cap', ''),
                    stock.get('fifty_two_week_high', ''),
                    stock.get('fifty_two_week_low', ''),
                    stock.get('pe_ratio', ''),
                    stock.get('dividend_yield', ''),
                    stock.get('beta', ''),
                ])
            
            writer.writerow(row)
        
        return response
        
    except Exception as e:
        logger.error(f"Error downloading NSE stocks data: {e}")
        messages.error(request, "Error downloading NSE data. Please try again.")
        return redirect('nse_stocks')


@login_required
def youtube_scraper(request):
    """Display YouTube scraper interface and job history"""
    from .models import YouTubeScrapingJob
    
    jobs = YouTubeScrapingJob.objects.filter(created_by=request.user).order_by('-created_at')[:20]
    
    context = {
        'jobs': jobs,
        'page_title': 'YouTube Video & Transcript Scraper'
    }
    
    return render(request, 'newscraper/youtube_scraper.html', context)


@login_required
def start_youtube_scraping(request):
    """Start YouTube scraping background job"""
    from .models import YouTubeScrapingJob
    import subprocess
    import sys
    
    if request.method == 'POST':
        keyword = request.POST.get('keyword', '').strip()
        
        if not keyword:
            messages.error(request, 'Please enter a keyword to search.')
            return redirect('youtube_scraper')
        
        job = YouTubeScrapingJob.objects.create(
            keyword=keyword,
            created_by=request.user,
            status='pending'
        )
        
        try:
            python_path = sys.executable
            manage_py_path = os.path.join(os.getcwd(), 'manage.py')
            
            subprocess.Popen(
                [python_path, manage_py_path, 'scrape_youtube', '--job-id', str(job.id)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )
            
            messages.success(
                request, 
                f'YouTube scraping started for "{keyword}". The process is running in the background. '
                f'Check the status below - it will update automatically.'
            )
            
        except Exception as e:
            logger.error(f'Error starting YouTube scraping job: {e}')
            job.status = 'failed'
            job.error_message = str(e)
            job.save()
            messages.error(request, f'Error starting scraping job: {str(e)}')
        
        return redirect('youtube_scraper')
    
    return redirect('youtube_scraper')


@login_required
def youtube_job_status(request, job_id):
    """Get YouTube scraping job status as JSON"""
    from .models import YouTubeScrapingJob
    
    try:
        job = get_object_or_404(YouTubeScrapingJob, id=job_id, created_by=request.user)
        
        data = {
            'id': job.id,
            'keyword': job.keyword,
            'status': job.status,
            'status_display': job.get_status_display(),
            'videos_found': job.videos_found,
            'transcripts_fetched': job.transcripts_fetched,
            'videos_csv_path': job.videos_csv_path,
            'transcripts_csv_path': job.transcripts_csv_path,
            'error_message': job.error_message,
            'created_at': job.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'completed_at': job.completed_at.strftime('%Y-%m-%d %H:%M:%S') if job.completed_at else None
        }
        
        return JsonResponse(data)
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=404)


@login_required
def download_youtube_csv(request, job_id, file_type):
    """Download YouTube scraping CSV files"""
    from .models import YouTubeScrapingJob
    
    try:
        job = get_object_or_404(YouTubeScrapingJob, id=job_id, created_by=request.user)
        
        if file_type == 'videos':
            filepath = job.videos_csv_path
            filename = os.path.basename(filepath) if filepath else 'youtube_videos_list.csv'
        elif file_type == 'transcripts':
            filepath = job.transcripts_csv_path
            filename = os.path.basename(filepath) if filepath else 'youtube_transcripts.csv'
        else:
            messages.error(request, 'Invalid file type')
            return redirect('youtube_scraper')
        
        if not filepath or not os.path.exists(filepath):
            messages.error(request, 'CSV file not found')
            return redirect('youtube_scraper')
        
        with open(filepath, 'rb') as f:
            response = HttpResponse(f.read(), content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="{filename}"'
            return response
            
    except Exception as e:
        logger.error(f'Error downloading YouTube CSV: {e}')
        messages.error(request, 'Error downloading file')
        return redirect('youtube_scraper')
