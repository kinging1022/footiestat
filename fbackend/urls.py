
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
    path('admin/', admin.site.urls),
    path('sitemap.xml', sitemap, {'sitemaps': sitemaps}, name='django.contrib.sitemaps.views.sitemap'),
    path('', include('football.urls')),
    path('', include('pages.urls')),
    path('', include('blog.urls')),
] + static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
