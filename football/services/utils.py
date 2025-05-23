import os
import json
from django.conf import settings
import logging
logger = logging.getLogger(__name__)



def save_unique_league_country_data(league_data):
    file_path= os.path.join(settings.BASE_DIR, 'football', 'services', 'unique_league_country.json')

    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            existing_data = json.load(f)
    else:
        existing_data = []
    
    uinque_dict = {league['id']:league for league in existing_data}

    for league in league_data:
        league_id = league.get('league', {}).get('id')
        name = league.get('league', {}).get('name', '')
        league_type = league.get('league', {}).get('type', '')
        country = league.get('country',{}).get('name', '')
        season = league.get('seasons', {})[0].get('year', 0)

        if league_id and name and league_type and country and season >= 2024:
            uinque_dict[league_id] = {
                'id': league_id,
                'name': name,
                'type': league_type,
                'country': country
            }


    with open(file_path, 'w') as f:
        json.dump(list(uinque_dict.values()), f, indent=4)

