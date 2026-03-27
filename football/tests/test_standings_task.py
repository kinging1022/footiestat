"""
Tests for refresh_today_standings task (data_pipeline.py).

Strategy:
- Patch process_single_league_standings at football.tasks (where it is imported
  inside the task body via `from football.tasks import ...`).
- Patch cache.delete to verify cache busting happens before the helper is called.
"""
import pytest
from unittest.mock import patch, MagicMock
from django.utils import timezone
from django.core.cache import cache

from football.models import Country, League, Team, Fixture
from football.tasks.data_pipeline import refresh_today_standings

# Correct patch target: the helper is imported *inside* the task body from football.tasks
_HELPER_PATH = "football.tasks.process_single_league_standings"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clear_cache():
    cache.clear()
    yield
    cache.clear()


@pytest.fixture
def country(db):
    return Country.objects.create(name="England", country_code="ENG")


@pytest.fixture
def league(country):
    return League.objects.create(
        id=39, name="Premier League", country=country, season=2024, priority=1
    )


@pytest.fixture
def home_team(country):
    return Team.objects.create(id=33, name="Manchester United", country=country)


@pytest.fixture
def away_team(country):
    return Team.objects.create(id=40, name="Liverpool", country=country)


@pytest.fixture
def yesterday_fixture(league, home_team, away_team):
    yesterday = timezone.now() - timezone.timedelta(days=1)
    return Fixture.objects.create(
        id=2001,
        date=yesterday,
        league=league,
        home_team=home_team,
        away_team=away_team,
    )


# ── Helper: invoke task as if Celery ran it ───────────────────────────────────

def run_task():
    """Run the task synchronously using Celery's eager apply().

    .apply() bypasses the broker and runs the task in the current process.
    It also handles the bind=True self-injection correctly, so we don't need
    to mock the task instance at all.
    """
    return refresh_today_standings.apply().get()


# ── Tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.django_db
def test_no_fixtures_yesterday_returns_no_work():
    """When no fixtures exist for yesterday, task returns status=no_work."""
    result = run_task()
    assert result["status"] == "no_work"
    assert result["processed"] == 0


@pytest.mark.django_db
def test_returns_success_when_fixtures_exist(yesterday_fixture):
    with patch("football.tasks.process_single_league_standings") as mock_helper:
        mock_helper.return_value = {"status": "success"}
        result = run_task()

    assert result["status"] == "success"
    assert result["processed"] == 1
    assert result["total"] == 1


@pytest.mark.django_db
def test_cache_keys_deleted_before_helper_called(yesterday_fixture, league):
    """Cache bust must happen before process_single_league_standings is called."""
    call_order = []

    def record_delete(key):
        call_order.append(("delete", key))

    def record_helper(league_id, season):
        call_order.append(("helper", league_id, season))
        return {"status": "success"}

    with patch("football.tasks.data_pipeline.cache") as mock_cache:
        mock_cache.delete.side_effect = record_delete
        with patch("football.tasks.process_single_league_standings") as mock_helper:
            mock_helper.side_effect = record_helper
            run_task()

    # delete must appear before helper in the call sequence
    delete_idx  = next(i for i, c in enumerate(call_order) if c[0] == "delete")
    helper_idx  = next(i for i, c in enumerate(call_order) if c[0] == "helper")
    assert delete_idx < helper_idx


@pytest.mark.django_db
def test_correct_cache_key_deleted(yesterday_fixture, league):
    expected_key = f"standings_{league.id}_{league.season}"
    with patch("football.tasks.data_pipeline.cache") as mock_cache:
        mock_cache.delete.return_value = None
        with patch("football.tasks.process_single_league_standings") as mock_helper:
            mock_helper.return_value = {"status": "success"}
            run_task()

    mock_cache.delete.assert_called_with(expected_key)


@pytest.mark.django_db
def test_rate_limit_stops_processing(country, league, home_team, away_team):
    """If RateLimitExceeded is raised, remaining leagues are skipped."""
    from football.api_client import RateLimitExceeded

    # Create two leagues with fixtures yesterday
    league2 = League.objects.create(
        id=140, name="La Liga", country=country, season=2024, priority=2
    )
    yesterday = timezone.now() - timezone.timedelta(days=1)
    Fixture.objects.create(
        id=2002, date=yesterday, league=league,
        home_team=home_team, away_team=away_team
    )
    Fixture.objects.create(
        id=2003, date=yesterday, league=league2,
        home_team=home_team, away_team=away_team
    )

    call_count = {"n": 0}

    def helper_raises_on_second(league_id, season):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RateLimitExceeded(wait_time=60, usage_stats={})
        return {"status": "success"}

    with patch("football.tasks.data_pipeline.cache"):
        with patch("football.tasks.process_single_league_standings") as mock_helper:
            mock_helper.side_effect = helper_raises_on_second
            result = run_task()

    # First call raised rate limit → second call never made
    assert mock_helper.call_count == 1
    assert result["processed"] == 0


@pytest.mark.django_db
def test_individual_errors_do_not_abort_run(country, league, home_team, away_team):
    """A generic exception on one league should not stop the others."""
    league2 = League.objects.create(
        id=78, name="Bundesliga", country=country, season=2024, priority=3
    )
    yesterday = timezone.now() - timezone.timedelta(days=1)
    Fixture.objects.create(
        id=2004, date=yesterday, league=league,
        home_team=home_team, away_team=away_team
    )
    Fixture.objects.create(
        id=2005, date=yesterday, league=league2,
        home_team=home_team, away_team=away_team
    )

    call_count = {"n": 0}

    def helper_errors_on_first(league_id, season):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise Exception("API exploded")
        return {"status": "success"}

    with patch("football.tasks.data_pipeline.cache"):
        with patch("football.tasks.process_single_league_standings") as mock_helper:
            mock_helper.side_effect = helper_errors_on_first
            result = run_task()

    # Both leagues attempted; second succeeded
    assert mock_helper.call_count == 2
    assert result["processed"] == 1
    assert result["total"] == 2


@pytest.mark.django_db
def test_duplicate_leagues_processed_once(league, home_team, away_team):
    """Two fixtures in the same league → league processed only once."""
    yesterday = timezone.now() - timezone.timedelta(days=1)
    team2 = Team.objects.create(id=55, name="Chelsea", country=home_team.country)
    Fixture.objects.create(
        id=2006, date=yesterday, league=league,
        home_team=home_team, away_team=away_team
    )
    Fixture.objects.create(
        id=2007, date=yesterday, league=league,
        home_team=away_team, away_team=team2
    )

    with patch("football.tasks.data_pipeline.cache"):
        with patch("football.tasks.process_single_league_standings") as mock_helper:
            mock_helper.return_value = {"status": "success"}
            result = run_task()

    assert mock_helper.call_count == 1
    assert result["total"] == 1


@pytest.mark.django_db
def test_result_date_is_yesterday(yesterday_fixture):
    from datetime import timedelta
    expected_yesterday = (timezone.now() - timedelta(days=1)).date().isoformat()

    with patch("football.tasks.process_single_league_standings") as mock_helper:
        mock_helper.return_value = {"status": "success"}
        result = run_task()

    assert result["date"] == expected_yesterday
