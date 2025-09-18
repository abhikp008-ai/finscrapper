# FinScrap

## Overview

FinScrap is a Django-based financial news scraping and monitoring platform designed to collect, store, and present financial news articles from multiple sources. The system provides web scraping capabilities for major financial news websites, user management with role-based permissions, and a dashboard interface for monitoring scraped content. The platform targets financial news sources including MoneyControl, Financial Express, LiveMint, CNBC, and Business Standard.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Framework
- **Django 5.2**: Core web framework providing MVC architecture, ORM, admin interface, and authentication
- **Python Management Commands**: Custom Django commands for automated scraping operations (`scrape_moneycontrol`, `scrape_financialexpress`, `scrape_livemint`, `scrape_all`)
- **SQLite Database**: Default database for development with models for Articles, UserProfiles, and ScrapingJobs

### Web Scraping Engine
- **httpx**: HTTP client for making requests to target websites with proper headers and timeout handling
- **BeautifulSoup4**: HTML parsing and content extraction from financial news websites
- **Source-Specific Scrapers**: Dedicated scraping logic for each financial news source with category-based organization
- **Content Extraction**: Full article content retrieval with fallback mechanisms for different HTML structures

### Data Models
- **Article Model**: Stores scraped articles with fields for title, URL, content, source, category, and timestamps
- **UserProfile Model**: Extends Django's User model with permissions for monitoring and downloading capabilities
- **ScrapingJob Model**: Tracks scraping operations with status, progress, and error handling

### Authentication & Authorization
- **Django Authentication**: Built-in user authentication system with login/logout functionality
- **Role-Based Permissions**: Custom permission system with `can_monitor` and `can_download` flags
- **Admin Interface**: Django admin panel for user management and data administration

### Frontend Interface
- **Bootstrap 5**: Responsive CSS framework for modern UI components
- **Django Templates**: Server-side rendered HTML templates with template inheritance
- **Dashboard Interface**: Central hub for viewing articles, managing users, and running scrapers
- **Filtering & Pagination**: Article filtering by source, category, and date with paginated results

### Job Management
- **Scraping Jobs**: Tracked execution of scraping operations with status monitoring
- **Batch Processing**: Ability to scrape multiple categories and sources in single operations
- **Error Handling**: Comprehensive error tracking and logging for failed scraping attempts

### Data Export
- **CSV Export**: Download functionality for articles in CSV format (permission-based)
- **Filtered Exports**: Export articles based on applied filters and search criteria

### Deployment Configuration
- **Environment Variables**: Production-ready configuration for SECRET_KEY, DEBUG, and ALLOWED_HOSTS
- **Static Files**: Organized static assets for admin interface and Bootstrap styling
- **Health Check**: Health endpoint for deployment monitoring

## External Dependencies

### Web Scraping Libraries
- **httpx**: Modern HTTP client for async/await support and connection pooling
- **BeautifulSoup4**: HTML and XML parsing for content extraction
- **logging**: Python standard library for operation logging and debugging

### Django Extensions
- **django-extensions**: Additional Django management commands and utilities
- **django-bootstrap5**: Bootstrap 5 integration for Django forms and templates

### Target News Sources
- **MoneyControl**: Indian financial news website (business, economy, markets, trends categories)
- **Financial Express**: Business and financial news (multiple categories including personal finance)
- **LiveMint**: Financial and business news portal
- **CNBC**: International business news (planned integration)
- **Business Standard**: Indian business news (planned integration)

### Frontend Dependencies
- **Bootstrap 5.1.3**: CSS framework served via CDN
- **jQuery**: JavaScript library included with Django admin
- **Django Admin**: Built-in administrative interface with extensive static assets

### Development Tools
- **Django Debug**: Development debugging and error reporting
- **Django Static Files**: Static file handling and collection for production deployment