from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    path('dashboard/', views.dashboard, name='dashboard'),
    path('download/', views.download_articles, name='download_articles'),
    path('manage-users/', views.manage_users, name='manage_users'),
    path('edit-user/<int:user_id>/', views.edit_user_permissions, name='edit_user_permissions'),
    path('run-scraper/', views.run_scraper, name='run_scraper'),
    path('privacy-policy/', views.privacy_policy, name='privacy_policy'),
    # NSE Stocks URLs
    path('nse-stocks/', views.nse_stocks, name='nse_stocks'),
    path('nse-stocks/<str:symbol>/', views.nse_stock_detail, name='nse_stock_detail'),
    path('download-nse/', views.download_nse_stocks, name='download_nse_stocks'),
    # YouTube Scraper URLs
    path('youtube-scraper/', views.youtube_scraper, name='youtube_scraper'),
    path('start-youtube-scraping/', views.start_youtube_scraping, name='start_youtube_scraping'),
    path('youtube-job-status/<int:job_id>/', views.youtube_job_status, name='youtube_job_status'),
    path('download-youtube-csv/<int:job_id>/<str:file_type>/', views.download_youtube_csv, name='download_youtube_csv'),
]