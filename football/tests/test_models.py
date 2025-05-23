import pytest
from django.db import IntegrityError
from football.models import Country, League

@pytest.mark.django_db
def test_country_str():
    country = Country.objects.create(name="England", country_code="ENG")
    assert str(country) == "England (ENG)"

@pytest.mark.django_db
def test_league_creation():
    country = Country.objects.create(name="Poland")
    league = League.objects.create(
        id=107,
        name="I Liga",
        country=country,
        season=2023
    )
    
    assert league.country.name == "Poland"
    assert country.leagues.count() == 1  # Testing reverse relation

@pytest.mark.django_db
def test_unique_constraints():
    country = Country.objects.create(name="World")
    
    # Create first instance
    League.objects.create(
        id=4,
        name="Euro Championship",
        country=country,
        season=2024
    )
    
    # Attempt duplicate creation
    with pytest.raises(Exception) as exc_info:
        League.objects.create(
            id=4,  # Duplicate primary key
            name="Euro Championship",
            country=country,
            season=2024
        )
    
    # Verify we caught an IntegrityError (subclass of Exception)
    assert isinstance(exc_info.value, IntegrityError)