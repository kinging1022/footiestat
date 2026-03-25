"""
Tests for model methods, properties, and constraints.
"""
import pytest
from django.db import IntegrityError
from django.utils import timezone
from football.models import (
    Country, League, Team, Fixture,
    FixtureIngestion, HeadToHeadMatch, LeagueTableSnapshot,
)


# ── Country ─────────────────────────────────────────────────────────────────

@pytest.mark.django_db
def test_country_str():
    country = Country.objects.create(name="England", country_code="ENG")
    assert str(country) == "England (ENG)"


@pytest.mark.django_db
def test_country_name_unique():
    Country.objects.create(name="England", country_code="ENG")
    with pytest.raises(IntegrityError):
        Country.objects.create(name="England", country_code="ENG")


# ── League ───────────────────────────────────────────────────────────────────

@pytest.mark.django_db
def test_league_str(league):
    assert "Premier League" in str(league)
    assert "England" in str(league)
    assert "2024" in str(league)


@pytest.mark.django_db
def test_league_unique_constraint(country):
    League.objects.create(id=39, name="Premier League", country=country, season=2024)
    with pytest.raises(IntegrityError):
        # Same name, season, country — violates unique_league_season
        League.objects.create(id=99, name="Premier League", country=country, season=2024)


@pytest.mark.django_db
@pytest.mark.parametrize("priority,expected", [
    (1, True),
    (10, True),
    (20, True),   # boundary — exactly 20 is priority
    (21, False),  # boundary — 21 is NOT priority
    (999, False),
])
def test_league_is_priority(country, priority, expected):
    league = League.objects.create(
        id=priority, name=f"League {priority}", country=country,
        season=2024, priority=priority
    )
    assert league.is_priority is expected


# ── Fixture ──────────────────────────────────────────────────────────────────

@pytest.mark.django_db
def test_fixture_slug(fixture):
    slug = fixture.slug
    assert "manchester-united" in slug
    assert "liverpool" in slug
    assert "vs" in slug
    # Format: home-vs-away-YYYYMMDD
    parts = slug.split("-")
    assert parts[-1].isdigit() and len(parts[-1]) == 8


@pytest.mark.django_db
def test_fixture_str(fixture):
    result = str(fixture)
    assert "Manchester United" in result
    assert "Liverpool" in result


@pytest.mark.django_db
def test_fixture_default_status(fixture):
    assert fixture.status == Fixture.STATUS_NS


# ── FixtureIngestion ──────────────────────────────────────────────────────────

@pytest.mark.django_db
def test_ingestion_starts_unprocessed(ingestion):
    assert ingestion.is_fully_processed is False
    assert ingestion.fully_processed_at is None


@pytest.mark.django_db
def test_check_and_mark_complete_when_all_done(ingestion):
    """All needs_* flags False → should mark fully processed."""
    ingestion.needs_h2h = False
    ingestion.needs_form = False
    ingestion.needs_standings = False
    ingestion.needs_advanced_stats = False
    ingestion.needs_detailed_stats = False
    ingestion.save()

    ingestion.check_and_mark_complete()
    ingestion.refresh_from_db()

    assert ingestion.is_fully_processed is True
    assert ingestion.fully_processed_at is not None


@pytest.mark.django_db
@pytest.mark.parametrize("pending_field", [
    "needs_h2h",
    "needs_form",
    "needs_standings",
    "needs_advanced_stats",
    "needs_detailed_stats",
])
def test_check_and_mark_complete_blocks_if_any_pending(ingestion, pending_field):
    """Even one pending flag should prevent marking as fully processed."""
    ingestion.needs_h2h = False
    ingestion.needs_form = False
    ingestion.needs_standings = False
    ingestion.needs_advanced_stats = False
    ingestion.needs_detailed_stats = False
    setattr(ingestion, pending_field, True)
    ingestion.save()

    ingestion.check_and_mark_complete()
    ingestion.refresh_from_db()

    assert ingestion.is_fully_processed is False


@pytest.mark.django_db
@pytest.mark.parametrize("done_count,expected_pct", [
    (0, 0.0),
    (1, 20.0),
    (2, 40.0),
    (3, 60.0),
    (4, 80.0),
    (5, 100.0),
])
def test_processing_percentage(ingestion, done_count, expected_pct):
    fields = [
        "needs_h2h", "needs_form", "needs_standings",
        "needs_advanced_stats", "needs_detailed_stats",
    ]
    # Mark `done_count` fields as done (False = done)
    for i, field in enumerate(fields):
        setattr(ingestion, field, i >= done_count)
    ingestion.save()

    assert ingestion.processing_percentage == expected_pct


# ── LeagueTableSnapshot ───────────────────────────────────────────────────────

@pytest.mark.django_db
def test_league_table_snapshot_unique_constraint(league, home_team, away_team):
    LeagueTableSnapshot.objects.create(
        league=league, season=2024, team=home_team,
        rank=1, points=30, goals_for=20, goals_against=5,
        goal_difference=15, matches_played=10, wins=9, draws=1, losses=0,
    )
    with pytest.raises(IntegrityError):
        LeagueTableSnapshot.objects.create(
            league=league, season=2024, team=home_team,
            rank=2, points=30, goals_for=20, goals_against=5,
            goal_difference=15, matches_played=10, wins=9, draws=1, losses=0,
        )


# ── HeadToHeadMatch ───────────────────────────────────────────────────────────

@pytest.mark.django_db
def test_h2h_match_str(fixture):
    h2h = HeadToHeadMatch.objects.create(
        fixture=fixture,
        past_fixture_id=500,
        league_name="Premier League",
        date=timezone.now(),
        home_name="Manchester United",
        away_name="Liverpool",
        home_fulltime_goals=2,
        away_fulltime_goals=1,
    )
    result = str(h2h)
    assert "Manchester United" in result
    assert "Liverpool" in result
    assert "2" in result
    assert "1" in result


@pytest.mark.django_db
def test_h2h_filters_out_current_fixture(fixture):
    """H2H should exclude matches where past_fixture_id == fixture.id."""
    # Create a past match with same ID as current fixture (should be excluded)
    HeadToHeadMatch.objects.create(
        fixture=fixture,
        past_fixture_id=fixture.id,  # Same as parent — should be filtered out
        league_name="Premier League",
        date=timezone.now(),
        home_name="Manchester United",
        away_name="Liverpool",
        home_fulltime_goals=1,
        away_fulltime_goals=0,
    )
    # Create a valid past match
    HeadToHeadMatch.objects.create(
        fixture=fixture,
        past_fixture_id=999,
        league_name="Premier League",
        date=timezone.now(),
        home_name="Manchester United",
        away_name="Liverpool",
        home_fulltime_goals=2,
        away_fulltime_goals=2,
    )

    valid_h2h = HeadToHeadMatch.objects.filter(
        fixture=fixture
    ).exclude(past_fixture_id=fixture.id)

    assert valid_h2h.count() == 1
    assert valid_h2h.first().past_fixture_id == 999
