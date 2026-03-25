# blog/views.py

from django.shortcuts import render, get_object_or_404, redirect
from django.core.paginator import Paginator
from django.db.models import F
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib import messages
from .models import Article
from .forms import ArticleForm

def blog_home(request):
    """
    Blog home with tabs: All Posts, Trending, Most Viewed
    """
    tab = request.GET.get('tab', 'all')  # all, trending, most_viewed
    
    # Base queryset - published articles only
    articles = Article.objects.filter(is_published=True)
    
    if tab == 'trending':
        # Trending: Sort by trending_score (requires annotation)
        from django.utils import timezone
        from datetime import timedelta
        
        # Articles from last 30 days, sorted by view count
        thirty_days_ago = timezone.now() - timedelta(days=30)
        articles = articles.filter(
            published_date__gte=thirty_days_ago
        ).order_by('-view_count', '-published_date')
        
    elif tab == 'most_viewed':
        # Most viewed of all time
        articles = articles.order_by('-view_count', '-published_date')
    else:
        # All posts (default)
        articles = articles.order_by('-published_date')
    
    # Pagination
    paginator = Paginator(articles, 12)  # 12 articles per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    
    # Featured article (for hero section)
    featured_article = Article.objects.filter(
        is_published=True,
        is_featured=True
    ).order_by('-published_date').first()
    
    context = {
        'page_obj': page_obj,
        'featured_article': featured_article,
        'active_tab': tab,
    }
    
    return render(request, 'blog/blog_home.html', context)


def article_detail(request, slug):
    """Article detail page with view tracking"""
    article = get_object_or_404(Article, slug=slug, is_published=True)
    
    # Increment view count
    article.increment_view_count()
    
    # Related articles (same type, exclude current)
    related_articles = Article.objects.filter(
        is_published=True,
        article_type=article.article_type
    ).exclude(id=article.id).order_by('-published_date')[:3]
    
    context = {
        'article': article,
        'related_articles': related_articles,
    }
    
    return render(request, 'blog/article_detail.html', context)


@staff_member_required
def create_article(request):
    """Admin-only article creation form"""
    if request.method == 'POST':
        form = ArticleForm(request.POST)
        if form.is_valid():
            article = form.save(commit=False)
            article.author = request.user
            article.save()
            messages.success(request, f'Article "{article.title}" created successfully!')
            return redirect('blog:article_detail', slug=article.slug)
    else:
        form = ArticleForm()
    
    return render(request, 'blog/create_article.html', {'form': form})


@staff_member_required
def edit_article(request, slug):
    """Admin-only article editing"""
    article = get_object_or_404(Article, slug=slug)
    
    if request.method == 'POST':
        form = ArticleForm(request.POST, instance=article)
        if form.is_valid():
            form.save()
            messages.success(request, f'Article "{article.title}" updated successfully!')
            return redirect('blog:article_detail', slug=article.slug)
    else:
        form = ArticleForm(instance=article)
    
    return render(request, 'blog/edit_article.html', {
        'form': form,
        'article': article
    })