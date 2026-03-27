"""
View-level integration tests: home, matches, fixture_stats, fixture_deep_stats.

Each test uses Django's test client. The cache is cleared before every test so
cached results from one test never bleed into the next.
"""
import pytest
from django.test import Client
from django.urls import reverse
from django.utils import timezone
from django.core.cache import cache

from football.models import (
    Country, League, Team, Fixture,
    HeadToHeadMatch, LeagueTableSnapshot,
    FixtureStatistics,
)


# ── Shared fixtures ────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


@pytest.fixture
def client():
    return Client()


@pytest.fixture
def country(db):
    return Country.objects.create(name="England", country_code="ENG")


@pytest.fixture
def league(country):
    return League.objects.create(
        id=39, name="Premier League", country=country,
        season=2024, priority=1, type="League"
    )


@pytest.fixture
def home_team(country):
    return Team.objects.create(id=33, name="Manchester United", country=country)


@pytest.fixture
def away_team(country):
    return Team.objects.create(id=40, name="Liverpool", country=country)


@pytest.fixture
def today_fixture(league, home_team, away_team):
    return Fixture.objects.create(
        id=1001,
        date=timezone.now(),
        league=league,
        home_team=home_team,
        away_team=away_team,
    )


# ── home view ─────────────────────────────────────────────────────────────────

@pytest.mark.django_db
def test_home_200(client):
    response = client.get(reverse("home"))
    assert response.status_code == 200


@pytest.mark.django_db
def test_home_uses_home_template(client):
    response = client.get(reverse("home"))
    assert "football/home.html" in [t.name for t in response.templates]


@pytest.mark.django_db
def test_home_invalid_date_falls_back_to_today(client):
    response = client.get(reverse("home") + "?date=not-a-date")
    assert response.status_code == 200


@pytest.mark.django_db
def test_home_with_date_filter(client, today_fixture):
    today_str = timezone.now().date().strftime("%Y-%m-%d")
    response = client.get(reverse("home") + f"?date={today_str}")
    assert response.status_code == 200
    leagues = response.context["leagues_with_fixtures"]
    assert any(lg["league_id"] == today_fixture.league.id for lg in leagues)


@pytest.mark.django_db
def test_home_league_filter(client, today_fixture):
    today_str = timezone.now().date().strftime("%Y-%m-%d")
    response = client.get(
        reverse("home") + f"?date={today_str}&league={today_fixture.league.id}"
    )
    assert response.status_code == 200


@pytest.mark.django_db
def test_home_htmx_returns_partial(client, today_fixture):
    today_str = timezone.now().date().strftime("%Y-%m-%d")
    response = client.get(
        reverse("home") + f"?date={today_str}",
        HTTP_HX_REQUEST="true",
    )
    assert response.status_code == 200
    assert "football/partials/home_htmx.html" in [t.name for t in response.templates]


@pytest.mark.django_db
def test_home_cache_hit_skips_queries(client, today_fixture, django_assert_num_queries):
    today_str = timezone.now().date().strftime("%Y-%m-%d")
    url = reverse("home") + f"?date={today_str}"
    # First hit — populates cache
    client.get(url)
    # Second hit — should serve from cache. We allow a small number of queries
    # (Django session/auth middleware may still run), but DB queries for fixture
    # data must NOT be re-executed.
    with django_assert_num_queries(0):
        r2 = client.get(url)
    assert r2.status_code == 200


# ── matches view ──────────────────────────────────────────────────────────────

@pytest.mark.django_db
def test_matches_requires_date(client):
    response = client.get(reverse("matches"))
    assert response.status_code == 400


@pytest.mark.django_db
def test_matches_invalid_date_format(client):
    response = client.get(reverse("matches") + "?date=2024-99-99")
    assert response.status_code == 400


@pytest.mark.django_db
def test_matches_200_valid_date(client):
    response = client.get(reverse("matches") + "?date=2024-03-01")
    assert response.status_code == 200


@pytest.mark.django_db
def test_matches_league_filter_invalid_id(client):
    response = client.get(reverse("matches") + "?date=2024-03-01&league=notanumber")
    assert response.status_code == 400


@pytest.mark.django_db
def test_matches_returns_fixtures_for_date(client, today_fixture):
    today_str = timezone.now().date().strftime("%Y-%m-%d")
    response = client.get(reverse("matches") + f"?date={today_str}")
    assert response.status_code == 200
    fixtures = response.context["fixtures"]
    assert any(f.id == today_fixture.id for f in fixtures)


@pytest.mark.django_db
def test_matches_league_filter(client, today_fixture):
    today_str = timezone.now().date().strftime("%Y-%m-%d")
    response = client.get(
        reverse("matches") + f"?date={today_str}&league={today_fixture.league.id}"
    )
    assert response.status_code == 200
    fixtures = response.context["fixtures"]
    assert all(f.league_id == today_fixture.league.id for f in fixtures)


# ── fixture_stats view ────────────────────────────────────────────────────────

@pytest.mark.django_db
def test_fixture_stats_404_unknown_id(client):
    response = client.get(reverse("fixture_stats", args=[99999, "some-slug"]))
    assert response.status_code == 404


@pytest.mark.django_db
def test_fixture_stats_200(client, today_fixture):
    url = reverse("fixture_stats", args=[today_fixture.id, today_fixture.slug])
    response = client.get(url)
    assert response.status_code == 200


@pytest.mark.django_db
def test_fixture_stats_context_keys(client, today_fixture):
    url = reverse("fixture_stats", args=[today_fixture.id, today_fixture.slug])
    response = client.get(url)
    for key in ("fixture", "h2h_matches", "h2h_summary", "advanced_stats",
                "home_standing", "away_standing", "full_table"):
        assert key in response.context, f"Missing context key: {key}"


@pytest.mark.django_db
def test_fixture_stats_no_advanced_stats(client, today_fixture):
    """When no FixtureAdvancedStats exists, advanced_stats is None — no crash."""
    url = reverse("fixture_stats", args=[today_fixture.id, today_fixture.slug])
    response = client.get(url)
    assert response.status_code == 200
    assert response.context["advanced_stats"] is None


@pytest.mark.django_db
def test_fixture_stats_with_standings(client, today_fixture, home_team, away_team, league):
    LeagueTableSnapshot.objects.create(
        league=league, season=2024, rank=1, team=home_team,
        points=45, goals_for=40, goals_against=20, goal_difference=20,
        matches_played=15, wins=14, draws=1, losses=0
    )
    LeagueTableSnapshot.objects.create(
        league=league, season=2024, rank=2, team=away_team,
        points=40, goals_for=35, goals_against=22, goal_difference=13,
        matches_played=15, wins=12, draws=4, losses=0
    )
    url = reverse("fixture_stats", args=[today_fixture.id, today_fixture.slug])
    response = client.get(url)
    assert response.status_code == 200
    home_s = response.context["home_standing"]
    away_s = response.context["away_standing"]
    assert home_s is not None
    assert away_s is not None


@pytest.mark.django_db
def test_fixture_stats_h2h_summary(client, today_fixture, home_team, away_team):
    # Create two H2H matches where home team won both
    HeadToHeadMatch.objects.create(
        fixture=today_fixture,
        past_fixture_id=500,
        league_name="Premier League",
        date=timezone.now() - timezone.timedelta(days=30),
        home_name=home_team.name,
        away_name=away_team.name,
        home_fulltime_goals=2,
        away_fulltime_goals=0,
    )
    HeadToHeadMatch.objects.create(
        fixture=today_fixture,
        past_fixture_id=501,
        league_name="Premier League",
        date=timezone.now() - timezone.timedelta(days=60),
        home_name=home_team.name,
        away_name=away_team.name,
        home_fulltime_goals=1,
        away_fulltime_goals=1,
    )
    url = reverse("fixture_stats", args=[today_fixture.id, today_fixture.slug])
    response = client.get(url)
    summary = response.context["h2h_summary"]
    assert summary["total"] == 2
    assert summary["home_wins"] == 1
    assert summary["draws"] == 1
    assert summary["away_wins"] == 0


@pytest.mark.django_db
def test_fixture_stats_cache_populated_on_first_hit(client, today_fixture):
    url = reverse("fixture_stats", args=[today_fixture.id, today_fixture.slug])
    client.get(url)
    cached = cache.get(f"fixture_stats_{today_fixture.id}")
    assert cached is not None
    assert "h2h_summary" in cached


@pytest.mark.django_db
def test_fixture_stats_served_from_cache_on_second_hit(client, today_fixture, django_assert_num_queries):
    url = reverse("fixture_stats", args=[today_fixture.id, today_fixture.slug])
    # Warm the cache
    client.get(url)
    # fixture is always fetched fresh (1 query), everything else from cache
    with django_assert_num_queries(1):
        r2 = client.get(url)
    assert r2.status_code == 200


# ── fixture_deep_stats view ───────────────────────────────────────────────────

@pytest.mark.django_db
def test_fixture_deep_stats_no_stats_object(client, today_fixture):
    """No FixtureStatistics → renders with has_stats=False, not a 404."""
    url = reverse("fixture_deep_stats", args=[9999])
    response = client.get(url)
    assert response.status_code == 200
    assert response.context["has_stats"] is False


@pytest.mark.django_db
def test_fixture_deep_stats_200_with_stats(client, today_fixture, home_team, away_team):
    stats = FixtureStatistics.objects.create(
        match_id=8001,
        fixture=today_fixture,
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        home_team_name=home_team.name,
        away_team_name=away_team.name,
    )
    url = reverse("fixture_deep_stats", args=[stats.match_id])
    response = client.get(url)
    assert response.status_code == 200
    assert response.context["has_stats"] is True


@pytest.mark.django_db
def test_fixture_deep_stats_context_keys(client, today_fixture, home_team, away_team):
    stats = FixtureStatistics.objects.create(
        match_id=8002,
        fixture=today_fixture,
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        home_team_name=home_team.name,
        away_team_name=away_team.name,
    )
    url = reverse("fixture_deep_stats", args=[stats.match_id])
    response = client.get(url)
    for key in ("fixture", "stats", "has_stats", "past_score", "insights"):
        assert key in response.context, f"Missing context key: {key}"


@pytest.mark.django_db
def test_fixture_deep_stats_cache_populated(client, today_fixture, home_team, away_team):
    stats = FixtureStatistics.objects.create(
        match_id=8003,
        fixture=today_fixture,
        home_team_id=home_team.id,
        away_team_id=away_team.id,
        home_team_name=home_team.name,
        away_team_name=away_team.name,
    )
    url = reverse("fixture_deep_stats", args=[stats.match_id])
    client.get(url)
    cached = cache.get(f"deep_stats_{stats.match_id}")
    assert cached is not None
    assert "past_score" in cached
