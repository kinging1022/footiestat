from django.shortcuts import render
from django.http import HttpResponseBadRequest
from datetime import datetime, timedelta
from football.models import Fixture



def home(request):
    today = datetime.today().date()
    selected_date = request.GET.get('date', today.strftime("%Y-%m-%d"))
    
    # Validate date
    try:
        datetime.strptime(selected_date, '%Y-%m-%d')
    except ValueError:
        selected_date = today.strftime("%Y-%m-%d")
    
    # Fetch all fixtures for the date
    fixtures = Fixture.objects.filter(
        date__date=selected_date
    ).select_related(
        'home_team', 
        'away_team', 
        'league',
        'league__country'
    ).order_by(
        'league__country__name',
        'league__name',
        'date'
    )
    
    # Group fixtures by country and league for sidebar
    grouped_fixtures = {}
    for fixture in fixtures:
        country_name = fixture.league.country.name
        league_name = fixture.league.name
        league_id = fixture.league.id
        
        if country_name not in grouped_fixtures:
            grouped_fixtures[country_name] = {}
        
        if league_id not in grouped_fixtures[country_name]:
            grouped_fixtures[country_name][league_id] = {
                'name': league_name,
                'logo': fixture.league.logo,
                'fixtures': []
            }
        
        grouped_fixtures[country_name][league_id]['fixtures'].append(fixture)
    
    # Build countries list for sidebar
    countries_data = []
    for country_name, leagues in grouped_fixtures.items():
        country_total = sum(len(league['fixtures']) for league in leagues.values())
        leagues_list = [
            {
                'id': league_id,
                'name': league_data['name'],
                'logo': league_data.get('logo'),
                'count': len(league_data['fixtures']),
                'fixtures': league_data['fixtures']
            }
            for league_id, league_data in leagues.items()
        ]
        
        countries_data.append({
            'name': country_name,
            'total_matches': country_total,
            'leagues': leagues_list
        })
    
    # Group fixtures by league for display (flat list for easy template iteration)
    leagues_with_fixtures = []
    for country in countries_data:
        for league in country['leagues']:
            leagues_with_fixtures.append({
                'country': country['name'],
                'league_name': league['name'],
                'league_logo': league['logo'],
                'league_id': league['id'],
                'fixtures': league['fixtures']
            })
    
    # Build dates for picker
    dates = []
    for i in range(5):
        d = today + timedelta(days=i)
        dates.append({
            "iso": d.strftime("%Y-%m-%d"),
            "day": d.strftime("%a").upper(), 
            "num": d.strftime("%d"),
            "mon": d.strftime("%b").upper(),
            "is_today": i == 0,
            "label": d.strftime("%A, %d %b %Y"),
        })
    
    context = {
        'dates': dates,
        'countries': countries_data,
        'selected_date': selected_date,
        'total_fixtures': fixtures.count(),
        'leagues_with_fixtures': leagues_with_fixtures,  # For display
    }
    
    # Check if HTMX request
    if request.headers.get('HX-Request'):
        return render(request, 'football/home_fragment.html', context)
    
    return render(request, 'football/home.html', context)

    
def matches(request):
    league_id = request.GET.get('league')
    date_str = request.GET.get('date')
    
    # Validate date parameter
    if not date_str:
        return HttpResponseBadRequest("Date parameter is required")
    
    try:
        # Validate date format
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return HttpResponseBadRequest("Invalid date format. Use YYYY-MM-DD")
    
    fixtures = Fixture.objects.filter(
        date__date=date_str
    ).select_related(
        'home_team',
        'away_team', 
        'league',
        'league__country'
    ).order_by('date')
    
    # Filter by league if specified
    if league_id:
        try:
            league_id = int(league_id)
            fixtures = fixtures.filter(league_id=league_id)
        except (ValueError, TypeError):
            return HttpResponseBadRequest("Invalid league ID")
    
    context = {
        'fixtures': fixtures,
        'selected_date': date_str,
        'selected_league': league_id
    }
    
    return render(request, 'football/matches.html', context)