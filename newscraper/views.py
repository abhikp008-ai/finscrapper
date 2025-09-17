from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.http import HttpResponse, JsonResponse
from django.core.paginator import Paginator
from django.db.models import Q
from django.utils import timezone
from django.core.management import call_command
from datetime import datetime, timedelta
import csv
import io
from .models import Article, UserProfile, ScrapingJob
from django.contrib.auth.models import User


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
        return user.userprofile.can_monitor
    except UserProfile.DoesNotExist:
        return False


def can_download(user):
    """Check if user has download permission"""
    if user.is_superuser:
        return True
    try:
        return user.userprofile.can_download
    except UserProfile.DoesNotExist:
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
    
    # Build queryset with filters
    articles = Article.objects.all()
    
    if source:
        articles = articles.filter(source=source)
    if category:
        articles = articles.filter(category__icontains=category)
    if search:
        articles = articles.filter(
            Q(title__icontains=search) | Q(content__icontains=search)
        )
    if date_from:
        try:
            date_from_parsed = datetime.strptime(date_from, '%Y-%m-%d').date()
            articles = articles.filter(scraped_at__date__gte=date_from_parsed)
        except ValueError:
            pass
    if date_to:
        try:
            date_to_parsed = datetime.strptime(date_to, '%Y-%m-%d').date()
            articles = articles.filter(scraped_at__date__lte=date_to_parsed)
        except ValueError:
            pass
    
    # Pagination
    paginator = Paginator(articles, 20)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Statistics
    total_articles = Article.objects.count()
    recent_jobs = ScrapingJob.objects.all()[:5]
    
    # Get available sources and categories for filters
    available_sources = Article.objects.values_list('source', flat=True).distinct()
    available_categories = Article.objects.values_list('category', flat=True).distinct()
    
    context = {
        'page_obj': page_obj,
        'total_articles': total_articles,
        'recent_jobs': recent_jobs,
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
    """Download articles as CSV"""
    # Get filter parameters
    source = request.GET.get('source', '')
    category = request.GET.get('category', '')
    date_from = request.GET.get('date_from', '')
    date_to = request.GET.get('date_to', '')
    search = request.GET.get('search', '')
    
    # Build queryset with same filters as dashboard
    articles = Article.objects.all()
    
    if source:
        articles = articles.filter(source=source)
    if category:
        articles = articles.filter(category__icontains=category)
    if search:
        articles = articles.filter(
            Q(title__icontains=search) | Q(content__icontains=search)
        )
    if date_from:
        try:
            date_from_parsed = datetime.strptime(date_from, '%Y-%m-%d').date()
            articles = articles.filter(scraped_at__date__gte=date_from_parsed)
        except ValueError:
            pass
    if date_to:
        try:
            date_to_parsed = datetime.strptime(date_to, '%Y-%m-%d').date()
            articles = articles.filter(scraped_at__date__lte=date_to_parsed)
        except ValueError:
            pass
    
    # Create CSV response
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="finscrap_articles_{timezone.now().strftime("%Y%m%d_%H%M%S")}.csv"'
    
    writer = csv.writer(response)
    writer.writerow(['ID', 'Title', 'URL', 'Source', 'Category', 'Scraped At', 'Content'])
    
    for article in articles:
        writer.writerow([
            article.id,
            article.title,
            article.url,
            article.get_source_display(),
            article.category,
            article.scraped_at.strftime('%Y-%m-%d %H:%M:%S'),
            article.content[:500] + '...' if len(article.content) > 500 else article.content
        ])
    
    return response


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
    """Manual scraper execution"""
    if request.method == 'POST':
        scraper = request.POST.get('scraper')
        max_pages = int(request.POST.get('max_pages', 1))
        
        # Map scraper command names to source values
        scraper_to_source = {
            'scrape_moneycontrol': 'moneycontrol',
            'scrape_financialexpress': 'financialexpress',
            'scrape_livemint': 'livemint',
            'scrape_all': 'moneycontrol',  # Use first source as default for 'all'
        }
        
        # Create scraping job record
        job = ScrapingJob.objects.create(
            source=scraper_to_source.get(scraper, 'moneycontrol'),
            status='running',
            started_at=timezone.now(),
            created_by=request.user
        )
        
        try:
            # Execute the management command with proper kwargs
            call_command(scraper, max_pages=max_pages)
            job.status = 'completed'
            job.completed_at = timezone.now()
            job.save()
            messages.success(request, f'Successfully completed {scraper} scraping ({max_pages} pages)')
        except Exception as e:
            job.status = 'failed'
            job.error_message = str(e)
            job.completed_at = timezone.now()
            job.save()
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
