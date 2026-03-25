import pytest
from django.utils import timezone
from football.models import (
    Country, League, Team, Fixture,
    FixtureIngestion, FixtureAdvancedStats, HeadToHeadMatch,
)


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
def fixture(league, home_team, away_team):
    return Fixture.objects.create(
        id=1001,
        date=timezone.now(),
        league=league,
        home_team=home_team,
        away_team=away_team,
    )


@pytest.fixture
def ingestion(fixture):
    return FixtureIngestion.objects.create(fixture=fixture)
