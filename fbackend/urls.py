
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.sitemaps.views import sitemap
from django.http import HttpResponse
from football.sitemaps import StaticSitemap, FixtureSitemap
from blog.sitemaps import BlogHomeSitemap, ArticleSitemap


sitemaps = {
    'static': StaticSitemap,
    'fixtures': FixtureSitemap,
    'blog': BlogHomeSitemap,
    'articles': ArticleSitemap,
}

def robots_txt(request):
    content = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /fst-manage-2026/\n"
        "\n"
        "Sitemap: https://footiestat.com/sitemap.xml\n"
    )
    return HttpResponse(content, content_type='text/plain')

urlpatterns = [
    path('fst-manage-2026/', admin.site.urls),
    path('sitemap.xml', sitemap, {'sitemaps': sitemaps}, name='django.contrib.sitemaps.views.sitemap'),
    path('robots.txt', robots_txt),
    path('', include('football.urls')),
    path('', include('pages.urls')),
    path('', include('blog.urls')),
] 
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
