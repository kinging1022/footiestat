import requests
from django.conf import settings

API_BASE_URL = settings.BASE_API_URL
API_KEY = settings.API_KEY
HEADERS = {
    'x-apisports-key': API_KEY
}


def get_fixtures(start_date=None, club_id=None, season=None, round=None , fixture_id=None, status=None):
    """
     Fetch fixtures from the football API.
    :param start_date: date to filter fixtures.
    :param club_id: ID of the club to filter fixtures.
    :param season: Season to filter fixtures.
    :param round: Round to filter fixtures.
    :return: JSON response containing fixtures.
   
    """
    params = {}
    if start_date:
        params['date'] = start_date
    if club_id:
        params['club'] = club_id

    if round:
        params['round'] = round

    if season:
        params['season'] = season

    if fixture_id:
        params['fixture'] = fixture_id

    if status:
        params['status'] = status
    
    url = API_BASE_URL + '/fixtures'
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()



def get_country_details(country_name):
    """
    Fetch country details from the football API.
    :param country_name: name of the country to fetch details for.
    :return: JSON response containing country details.
    """
    url = API_BASE_URL + '/countries/'
    params = {'name': country_name}
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


def get_league_details(id=None, season=None):
    """
    Fetch country details from the football API.
    :param id: id of the league to fetch details for.
    :param season: season to filter the league details.
    :return: JSON response containing country details.
    """
    
    url = API_BASE_URL + '/leagues/'
    params = {'current':'true'}
    if id:
        params['id'] = id
    if season:
        params['season'] = season
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


def get_team_details(id=None,season=None,country=None):

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
    
    
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()
    
        


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
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()


def get_fixture_head_to_head(team_ids,last=None):
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
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()




def get_fixture_stats(fixture_id):
    """
    Fetch fixture statistics from the football API.
    :param fixture_id: ID of the fixture to fetch statistics for.
    :return: JSON response containing fixture statistics.
    """
    url = API_BASE_URL + '/fixtures/statistics'
    params = {'fixture': fixture_id}
    response = requests.get(url, headers=HEADERS, params=params)
    response.raise_for_status()
    return response.json()