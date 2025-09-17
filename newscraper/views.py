from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate, login
from django.contrib import messages
from .models import Article


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


@login_required
def dashboard(request):
    articles = Article.objects.all()[:50]
    total_articles = Article.objects.count()
    
    context = {
        'articles': articles,
        'total_articles': total_articles,
    }
    return render(request, 'newscraper/dashboard.html', context)
