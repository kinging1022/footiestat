from football.api_client import get_fixtures
from .utils import handle_club_data , handle_league_date , handle_country_data, handle_fixtures_data
from datetime import date, timedelta



def load_fixtures_country_league_club():
    """
    Load fixtures, country, league, and club data from the football API and save it to the database.
    """
    
    
     
    all_fixtures = []

    for days_ahead in range(3):
       
        start_date = date.today() + timedelta(days=days_ahead)
       
        # Fetch fixtures for the next 3 days
        try:
            response = get_fixtures(start_date=start_date)
            if response and 'response' in response:
                # Append the fixtures to the all_fixtures list
                all_fixtures.extend(response['response'])
            else:

                print(f"No fixtures found for {start_date}")
        except Exception as e:
            print(f"Error fetching fixtures for {start_date}: {e}")
            continue
    
    
    for fixture in all_fixtures:
        fixture_data = fixture['fixture']
        league_data = fixture['league']
        teams_data = fixture['teams']


        # create or update country and league
        country = handle_country_data(league_data)



        #create or update league
        league = handle_league_date(league_data, country)



        #create or update clubs
        home_team = handle_club_data(teams_data['home'], country)
        away_team = handle_club_data(teams_data['away'], country)



        #handle fixture
        fixture_sync = handle_fixtures_data(fixture_data,league, home_team, away_team)
        


                







