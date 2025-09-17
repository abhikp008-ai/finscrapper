from django.contrib import admin
from .models import Article, UserProfile, ScrapingJob


@admin.register(Article)
class ArticleAdmin(admin.ModelAdmin):
    list_display = ('title', 'source', 'category', 'scraped_at')
    list_filter = ('source', 'category', 'scraped_at')
    search_fields = ('title', 'content')
    readonly_fields = ('scraped_at',)
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related()


@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'can_monitor', 'can_download', 'created_at')
    list_filter = ('can_monitor', 'can_download', 'created_at')
    search_fields = ('user__username', 'user__email')


@admin.register(ScrapingJob)
class ScrapingJobAdmin(admin.ModelAdmin):
    list_display = ('source', 'status', 'articles_scraped', 'started_at', 'created_by')
    list_filter = ('source', 'status', 'started_at')
    readonly_fields = ('started_at', 'completed_at')
