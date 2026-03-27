from django.core.cache import cache
from django.utils import timezone

_SHOW = {'NS', 'TBD'}
_CACHE_TTL = 3600  # 1 hour — fixtures don't change frequently


def fixture_carousel(request):
    today = timezone.localdate()
    cache_key = f'carousel_{today}'
    cached = cache.get(cache_key)
    if cached is not None:
        return {'carousel_fixtures': cached}

    from football.models import Fixture

    qs = (
        Fixture.objects
        .filter(date__date=today, status__in=_SHOW)
        .select_related('home_team', 'away_team', 'league')
        .order_by('league__priority', 'date')[:60]
    )

    fixtures = list(qs)

    priority = [f for f in fixtures if f.league.priority <= 20]
    others   = [f for f in fixtures if f.league.priority > 20]
    chosen   = (priority + others)[:12]

    result = []
    for f in chosen:
        result.append({
            'id':          f.id,
            'slug':        f.slug,
            'status':      f.status,
            'date':        f.date,
            'league_name': f.league.name,
            'league_logo': f.league.logo,
            'home_name':   f.home_team.short_name or f.home_team.name,
            'home_logo':   f.home_team.logo,
            'away_name':   f.away_team.short_name or f.away_team.name,
            'away_logo':   f.away_team.logo,
        })

    cache.set(cache_key, result, timeout=_CACHE_TTL)
    return {'carousel_fixtures': result}
