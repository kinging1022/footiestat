from django.contrib.sitemaps import Sitemap
from django.urls import reverse

from blog.models import Article


class BlogHomeSitemap(Sitemap):
    changefreq = 'daily'
    priority = 0.9

    def items(self):
        return ['blog:home']

    def location(self, item):
        return reverse(item)


class ArticleSitemap(Sitemap):
    changefreq = 'weekly'
    priority = 0.7

    def items(self):
        return Article.objects.filter(is_published=True).order_by('-published_date')

    def location(self, article):
        return reverse('blog:article_detail', args=[article.slug])

    def lastmod(self, article):
        return article.updated_at
