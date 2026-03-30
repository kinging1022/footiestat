
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.sitemaps.views import sitemap
from football.sitemaps import StaticSitemap, FixtureSitemap
from blog.sitemaps import BlogHomeSitemap, ArticleSitemap


sitemaps = {
    'static': StaticSitemap,
    'fixtures': FixtureSitemap,
    'blog': BlogHomeSitemap,
    'articles': ArticleSitemap,
}

urlpatterns = [
    path('fst-manage-2026/', admin.site.urls),
    path('sitemap.xml', sitemap, {'sitemaps': sitemaps}, name='django.contrib.sitemaps.views.sitemap'),
    path('', include('football.urls')),
    path('', include('pages.urls')),
    path('', include('blog.urls')),
] 
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
