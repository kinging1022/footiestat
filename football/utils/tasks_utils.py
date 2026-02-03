from typing import Dict, List, Any
from football.models import Fixture
import logging

logger = logging.getLogger(__name__)


def get_fixtures_for_processing(fixture_ids:List[int]) -> List[Dict[str,Any]]:

    fixtures = Fixture.objects.filter(id__in=fixture_ids).select_related('home_team', 'away_team', 'league')

    
    return [{
    'id': f.id,
    'home_team_id': f.home_team.id,
    'away_team_id': f.away_team.id,
    'league_id': f.league.id,
    'season': f.league.season,
    'fixture_date': f.date,
    'status': f.status
} for f in fixtures]




def format_date_time(date_str: str):
    """
    Format date and time string to a more readable format.

    :param date_str: Date and time string in ISO format.
    :return: Formatted date and time string.
    """
    from datetime import datetime
    try:
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'
        dt = datetime.fromisoformat(date_str)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        raise ValueError(f"Invalid date format: {date_str}") from e
    



