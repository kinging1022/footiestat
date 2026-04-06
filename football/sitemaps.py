from django.contrib.sitemaps import Sitemap
from django.urls import reverse
from datetime import date, timedelta

from football.models import Fixture


class StaticSitemap(Sitemap):
    changefreq = 'daily'
    priority = 1.0

    def items(self):
        return ['home']

    def location(self, item):
        return reverse(item)


class FixtureSitemap(Sitemap):
    changefreq = 'daily'
    priority = 0.8

    def items(self):
        window_start = date.today() - timedelta(days=7)
        window_end = date.today() + timedelta(days=7)
        return Fixture.objects.filter(
            date__date__gte=window_start,
            date__date__lte=window_end,
        ).select_related('home_team', 'away_team', 'league').order_by('-date')
