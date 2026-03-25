import requests
from django.conf import settings
from football.utils import get_football_api_limiter,  get_football_api_second_limiter

API_BASE_URL = settings.BASE_API_URL
API_KEY = settings.API_KEY
HEADERS = {
    'x-apisports-key': API_KEY
}

# Simple custom exception for rate limiting
class RateLimitExceeded(Exception):
    def __init__(self, wait_time, usage_stats):
        self.wait_time = wait_time
        self.usage_stats = usage_stats
        super().__init__(f"Rate limit exceeded. Wait {wait_time:.1f}s")



def _make_request(url, params=None):
    """
    Internal helper to make rate-limited requests.
    Checks per-second limit first, then per-minute limit.
    """

    minute_limiter = get_football_api_limiter()
    second_limiter = get_football_api_second_limiter()

    # Check per-second limit first — most likely to be hit
    if not second_limiter.can_make_request():
        wait_time = second_limiter.wait_time_until_next_slot()
        usage     = second_limiter.get_current_usage()
        raise RateLimitExceeded(wait_time, usage)

    # Check per-minute limit
    if not minute_limiter.can_make_request():
        wait_time = minute_limiter.wait_time_until_next_slot()
        usage     = minute_limiter.get_current_usage()
        raise RateLimitExceeded(wait_time, usage)

    response = requests.get(url, headers=HEADERS, params=params or {})
    response.raise_for_status()
    return response.json()


def get_fixtures(date=None, team_id=None, season=None, round=None, fixture_id=None, status=None, from_date=None, to_date=None, last=None, next=None, league=None):
    """
    Fetch fixtures from the football API.
    :param start_date: date to filter fixtures.
    :param club_id: ID of the club to filter fixtures.
    :param season: Season to filter fixtures.
    :param round: Round to filter fixtures.
    :return: JSON response containing fixtures.
    """
    params = {}
    if date:
        params['date'] = date
    
    if team_id:
        params['team'] = team_id

    if round:
        params['round'] = round

    if season:
        params['season'] = season

    if fixture_id:
        params['fixture'] = fixture_id

    if status:
        params['status'] = status

    if from_date:
        params["from"] = from_date

    if to_date:
        params['to'] = to_date

    if last:
        params['last'] = last

    if next:
        params['next'] = next

    if league:
        params['league'] = league

    url = API_BASE_URL + '/fixtures'
    return _make_request(url, params)  


def get_country_details(country_name):
    """
    Fetch country details from the football API.
    :param country_name: name of the country to fetch details for.
    :return: JSON response containing country details.
    """
    url = API_BASE_URL + '/countries/'
    params = {'name': country_name}
    return _make_request(url, params)  


def get_league_details(id=None, season=None):
    """
    Fetch country details from the football API.
    :param id: id of the league to fetch details for.
    :param season: season to filter the league details.
    :return: JSON response containing country details.
    """
    url = API_BASE_URL + '/leagues/'
    params = {'current': 'true'}
    if id:
        params['id'] = id
    if season:
        params['season'] = season
    return _make_request(url, params)  


def get_team_details(id=None, season=None, country=None):
    """
    Fetch team details from football api
    :param id: id of the team to fetch details for.
    :param season: season to filter the team details.
    :param country: country to filter the team details.
    :return: JSON response containing team details.
    """
    url = API_BASE_URL + '/teams'
    params = {}

    if id:
        params['id'] = id
    if season:  
        params['season'] = season
    if country:
        params['country'] = country
    
    return _make_request(url, params)  


def get_league_table(league_id, season=None):
    """
    Fetch league table from the football API.
    :param league_id: ID of the league to fetch table for.
    :param season: season to filter the league table.
    :return: JSON response containing league table.
    """
    url = API_BASE_URL + '/standings'
    params = {'league': league_id}
    if season:
        params['season'] = season
    return _make_request(url, params)  


def get_fixture_head_to_head(team_ids, last=None):
    """
    Fetch head-to-head data between two teams from the football API.
    :param teamids: list of team IDs to fetch head-to-head data for.
    :param last: number of last matches to consider for head-to-head data.
    :return: JSON response containing head-to-head data.
    """
    url = API_BASE_URL + '/fixtures/headtohead'
    params = {'h2h': team_ids}
    if last:
        params['last'] = last
    return _make_request(url, params)  


def get_fixture_stats(fixture_id):
    """
    Fetch fixture statistics from the football API.
    :param fixture_id: ID of the fixture to fetch statistics for.
    :return: JSON response containing fixture statistics.
    """
    url = API_BASE_URL + '/fixtures/statistics'
    params = {'fixture': fixture_id}
    return _make_request(url, params)  



def get_team_competitions(team_id, season=None):
    """
    Fetch competitions a team is participating in from the football API.
    :param team_id: ID of the team to fetch competitions for.
    :param season: season to filter the competitions.
    :return: JSON response containing team competitions.
    """
    url = API_BASE_URL + '/leagues'
    params = {'team': team_id}
    if season:
        params['season'] = season
    return _make_request(url, params)  



def get_match_stats(fixture_id):
    """
    Fetch detailed match statistics for a fixture from the football API.
    :param fixture_id: ID of the fixture to fetch match statistics for.
    :return: JSON response containing match statistics.
    """
    url = API_BASE_URL + '/fixtures/statistics'
    params = {'fixture': fixture_id}
    return _make_request(url, params)