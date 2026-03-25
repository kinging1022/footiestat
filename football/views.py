from django.shortcuts import render, get_object_or_404
from django.db.models import Count
from django.http import HttpResponseBadRequest
from datetime import datetime, timedelta
from football.models import Team, Fixture,League, HeadToHeadMatch, FixtureAdvancedStats, FixtureStatistics
from collections import defaultdict

def home(request):
    today = datetime.today().date()
    selected_date = request.GET.get('date', today.strftime("%Y-%m-%d"))
    selected_league_id = request.GET.get('league')  # ← add this

    days_ahead = 10 if request.user.is_staff else 5

    try:
        datetime.strptime(selected_date, '%Y-%m-%d')
    except ValueError:
        selected_date = today.strftime("%Y-%m-%d")

    # SIDEBAR — unchanged
    fixtures_for_sidebar = Fixture.objects.filter(
        date__date=selected_date
    ).select_related('league', 'league__country')

    countries_dict = defaultdict(lambda: {'leagues': {}, 'total_fixtures': 0})
    for fixture in fixtures_for_sidebar:
        country_name = fixture.league.country.name
        league_id = fixture.league.id
        if league_id not in countries_dict[country_name]['leagues']:
            countries_dict[country_name]['leagues'][league_id] = {
                'id': league_id,
                'name': fixture.league.name,
                'logo': fixture.league.logo,
                'is_priority': fixture.league.is_priority,
                'priority': fixture.league.priority,
                'count': 0
            }
        countries_dict[country_name]['leagues'][league_id]['count'] += 1
        countries_dict[country_name]['total_fixtures'] += 1

    countries_data = []
    for country_name in sorted(countries_dict.keys()):
        country_info = countries_dict[country_name]
        leagues_list = sorted(
            country_info['leagues'].values(),
            key=lambda x: (x['priority'] if x['priority'] else 999, x['name'])
        )
        countries_data.append({
            'name': country_name,
            'total_fixtures': country_info['total_fixtures'],
            'leagues': leagues_list
        })

    # MAIN CONTENT — now respects league filter
    fixtures_query = Fixture.objects.filter(
        date__date=selected_date
    ).select_related(
        'home_team', 'away_team', 'league', 'league__country'
    )

    if selected_league_id:
        # League selected from sidebar — show only that league
        try:
            selected_league_id = int(selected_league_id)
            fixtures = fixtures_query.filter(
                league_id=selected_league_id
            ).order_by('date')[:100]
            display_mode = 'league_filter'
        except (ValueError, TypeError):
            fixtures = fixtures_query.order_by('league__name', 'date')[:100]
            display_mode = 'alphabetical_fallback'
    else:
        # No league selected — use priority logic
        priority_league_ids = League.objects.filter(
            priority__lte=20
        ).values_list('id', flat=True)

        priority_fixtures = fixtures_query.filter(
            league_id__in=priority_league_ids
        ).order_by('league__priority', 'league__name', 'date')[:100]

        if priority_fixtures.exists():
            fixtures = priority_fixtures
            display_mode = 'priority_leagues'
        else:
            fixtures = fixtures_query.order_by('league__name', 'date')[:100]
            display_mode = 'alphabetical_fallback'

    # Group by league for display
    fixtures_by_league = defaultdict(list)
    for fixture in fixtures:
        fixtures_by_league[fixture.league].append(fixture)

    if display_mode == 'priority_leagues':
        sorted_leagues = sorted(
            fixtures_by_league.items(),
            key=lambda x: (x[0].priority, x[0].name)
        )
    else:
        sorted_leagues = sorted(
            fixtures_by_league.items(),
            key=lambda x: x[0].name
        )

    leagues_with_fixtures = []
    for league, league_fixtures in sorted_leagues:
        leagues_with_fixtures.append({
            'league_id': league.id,
            'league_name': league.name,
            'league_logo': league.logo,
            'country': league.country.name,
            'fixtures': league_fixtures,
            'is_priority': league.is_priority
        })

    # DATE PICKER
    dates = []
    for i in range(days_ahead):
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
        'selected_league_id': selected_league_id,
        'total_fixtures': fixtures_for_sidebar.count(),
        'leagues_with_fixtures': leagues_with_fixtures,
        'display_mode': display_mode,
    }

    if request.headers.get('HX-Request'):
        return render(request, 'football/partials/home_htmx.html', context)

    return render(request, 'football/home.html', context)

def matches(request):
    """
    Filter matches by league (for sidebar league filtering)
    """
    league_id = request.GET.get('league')
    date_str = request.GET.get('date')

    if not date_str:
        return HttpResponseBadRequest("Date parameter is required")

    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return HttpResponseBadRequest("Invalid date format. Use YYYY-MM-DD")

    fixtures = Fixture.objects.filter(
        date__date=date_str
    ).select_related(
        'home_team', 'away_team', 'league', 'league__country'
    ).order_by('date')

    league_info = None
    if league_id:
        try:
            league_id = int(league_id)
            fixtures = fixtures.filter(league_id=league_id)
            try:
                league = League.objects.select_related('country').get(id=league_id)
                league_info = {
                    'name': league.name,
                    'logo': league.logo,
                    'country': league.country.name
                }
            except League.DoesNotExist:
                pass
        except (ValueError, TypeError):
            return HttpResponseBadRequest("Invalid league ID")

    context = {
        'fixtures': fixtures,
        'selected_date': date_str,
        'selected_league': league_id,
        'league_info': league_info,
    }

    return render(request, 'football/matches.html', context)



def fixture_stats(request, fixture_id, slug=None):
    from football.models import LeagueTableSnapshot
    
    fixture = get_object_or_404(
        Fixture.objects.select_related(
            'home_team',
            'away_team',
            'league',
            'league__country'
        ),
        id=fixture_id
    )
    
    # ✅ NEW: Get team standings
    try:
        home_standing = LeagueTableSnapshot.objects.get(
            team=fixture.home_team,
            league=fixture.league,
            season=fixture.league.season
        )
    except LeagueTableSnapshot.DoesNotExist:
        home_standing = None
    
    try:
        away_standing = LeagueTableSnapshot.objects.get(
            team=fixture.away_team,
            league=fixture.league,
            season=fixture.league.season
        )
    except LeagueTableSnapshot.DoesNotExist:
        away_standing = None
    
    # ✅ NEW: Get full league table
    full_table = LeagueTableSnapshot.objects.filter(
        league=fixture.league,
        season=fixture.league.season
    ).select_related('team').order_by('rank')

    first_snapshot = full_table.first()
    standings_updated_at = first_snapshot.updated_at if first_snapshot else None
    
    # Get head to head matches
    h2h_matches = HeadToHeadMatch.objects.filter(
        fixture=fixture
    ).exclude(
        past_fixture_id=fixture.id
    ).order_by('-date')[:6]

    h2h_matches_display = [m for m in h2h_matches if m.home_fulltime_goals is not None and m.away_fulltime_goals is not None]
    
    # Get advanced stats
    try:
        advanced_stats = FixtureAdvancedStats.objects.get(fixture=fixture)
    except FixtureAdvancedStats.DoesNotExist:
        advanced_stats = None
    
    # Calculate H2H summary
    h2h_summary = {
        'total': h2h_matches.count(),
        'home_wins': 0,
        'away_wins': 0,
        'draws': 0,
        'home_goals': 0,
        'away_goals': 0
    }
    
    for match in h2h_matches:
        if match.home_fulltime_goals is None or match.away_fulltime_goals is None:
            continue

        if match.home_name == fixture.home_team.name:
            h2h_summary['home_goals'] += match.home_fulltime_goals
            h2h_summary['away_goals'] += match.away_fulltime_goals
        else:   
            h2h_summary['home_goals'] += match.away_fulltime_goals
            h2h_summary['away_goals'] += match.home_fulltime_goals
        
        if match.home_fulltime_goals > match.away_fulltime_goals:
            if match.home_name == fixture.home_team.name:
                h2h_summary['home_wins'] += 1
            else:
                h2h_summary['away_wins'] += 1
        elif match.home_fulltime_goals < match.away_fulltime_goals:
            if match.away_name == fixture.away_team.name:
                h2h_summary['away_wins'] += 1
            else:
                h2h_summary['home_wins'] += 1
        else:
            h2h_summary['draws'] += 1
    
    # Calculate avg goals per match
    completed_matches = sum(1 for m in h2h_matches if m.home_fulltime_goals is not None)
    if completed_matches > 0:
        h2h_summary['avg_goals'] = round((h2h_summary['home_goals'] + h2h_summary['away_goals']) / completed_matches, 1)
    else:
        h2h_summary['avg_goals'] = 0
    
    # Calculate weighted form rating
    def calculate_form_rating(wins, draws, losses):
        total_matches = wins + draws + losses
        if total_matches == 0:
            return 0
        points_earned = (wins * 3) + (draws * 1)
        max_possible = total_matches * 3
        return round((points_earned / max_possible) * 100, 1)
    
    def categorize_form(rating):
        if rating >= 60:
            return 'Strong'
        elif rating >= 40:
            return 'Fair'
        else:
            return 'Poor'
    
    # Calculate insights if advanced stats exist
    overall_insights = None
    home_away_insights = None
    similar_rank_stats = None
    
    # Cup detection
    is_cup = fixture.league.type.lower() == 'cup' if fixture.league.type else False
    home_cup_form = []
    away_cup_form = []

    if is_cup:
        from football.models import TeamFormSnapshot

        def _build_cup_form(team):
            snapshots = TeamFormSnapshot.objects.filter(
                team=team,
                league_id=fixture.league.id,
            ).select_related('opponent').order_by('-match_date')[:5]
            result = []
            for s in snapshots:
                gs = s.home_fulltime_goals if s.is_home else s.away_fulltime_goals
                gc = s.away_fulltime_goals if s.is_home else s.home_fulltime_goals
                result.append({
                    'result': s.result,
                    'is_home': s.is_home,
                    'opponent': s.opponent.name,
                    'goals_scored': gs,
                    'goals_conceded': gc,
                    'fixture_id': s.fixture_id,
                })
            return result

        home_cup_form = _build_cup_form(fixture.home_team)
        away_cup_form = _build_cup_form(fixture.away_team)

    if advanced_stats:
        # Overall form insights — use actual matches played, not hardcoded 5
        home_total_last_5 = (
            advanced_stats.home_wins_last_5 +
            advanced_stats.home_draws_last_5 +
            advanced_stats.home_losses_last_5
        )
        away_total_last_5 = (
            advanced_stats.away_wins_last_5 +
            advanced_stats.away_draws_last_5 +
            advanced_stats.away_losses_last_5
        )

        overall_insights = {
            'home_win_rate': round((advanced_stats.home_wins_last_5 / home_total_last_5) * 100, 1) if home_total_last_5 else 0,
            'home_avg_goals': round(advanced_stats.home_goals_scored_last_5 / home_total_last_5, 1) if home_total_last_5 else 0,
            'away_win_rate': round((advanced_stats.away_wins_last_5 / away_total_last_5) * 100, 1) if away_total_last_5 else 0,
            'away_avg_goals': round(advanced_stats.away_goals_scored_last_5 / away_total_last_5, 1) if away_total_last_5 else 0,
        }

        # Home/Away specific insights with weighted form
        home_home_total = (
            advanced_stats.home_home_wins_last_5 +
            advanced_stats.home_home_draws_last_5 +
            advanced_stats.home_home_losses_last_5
        )
        away_away_total = (
            advanced_stats.away_away_wins_last_5 +
            advanced_stats.away_away_draws_last_5 +
            advanced_stats.away_away_losses_last_5
        )

        home_home_rating = calculate_form_rating(
            advanced_stats.home_home_wins_last_5,
            advanced_stats.home_home_draws_last_5,
            advanced_stats.home_home_losses_last_5
        )

        away_away_rating = calculate_form_rating(
            advanced_stats.away_away_wins_last_5,
            advanced_stats.away_away_draws_last_5,
            advanced_stats.away_away_losses_last_5
        )

        home_away_insights = {
            'home_home_win_rate': round((advanced_stats.home_home_wins_last_5 / home_home_total) * 100, 1) if home_home_total else 0,
            'home_advantage': categorize_form(home_home_rating),
            'home_home_rating': home_home_rating,
            'away_away_win_rate': round((advanced_stats.away_away_wins_last_5 / away_away_total) * 100, 1) if away_away_total else 0,
            'away_strength': categorize_form(away_away_rating),
            'away_away_rating': away_away_rating,
        }

        # Similar rank stats
        home_similar = advanced_stats.home_last_5_vs_similar_rank or []
        away_similar = advanced_stats.away_last_5_vs_similar_rank or []

        def count_results(matches_list):
            wins = sum(1 for m in matches_list if isinstance(m, dict) and m.get('result') == 'W')
            draws = sum(1 for m in matches_list if isinstance(m, dict) and m.get('result') == 'D')
            losses = sum(1 for m in matches_list if isinstance(m, dict) and m.get('result') == 'L')
            return wins, draws, losses

        home_similar_wins, home_similar_draws, home_similar_losses = count_results(home_similar)
        away_similar_wins, away_similar_draws, away_similar_losses = count_results(away_similar)

        home_similar_rating = calculate_form_rating(
            home_similar_wins,
            home_similar_draws,
            home_similar_losses
        )

        away_similar_rating = calculate_form_rating(
            away_similar_wins,
            away_similar_draws,
            away_similar_losses
        )

        home_similar_total = len([m for m in home_similar if isinstance(m, dict) and m.get('opponent') != 'No data'])
        away_similar_total = len([m for m in away_similar if isinstance(m, dict) and m.get('opponent') != 'No data'])

        similar_rank_stats = {
            'home_wins': home_similar_wins,
            'home_draws': home_similar_draws,
            'home_losses': home_similar_losses,
            'home_total': home_similar_total,
            'home_record': categorize_form(home_similar_rating),
            'home_rating': home_similar_rating,

            'away_wins': away_similar_wins,
            'away_draws': away_similar_draws,
            'away_losses': away_similar_losses,
            'away_total': away_similar_total,
            'away_record': categorize_form(away_similar_rating),
            'away_rating': away_similar_rating,
            'has_data': home_similar_total > 0 or away_similar_total > 0,
        }
    
    context = {
        'fixture': fixture,
        'h2h_matches': h2h_matches_display,
        'h2h_summary': h2h_summary,
        'advanced_stats': advanced_stats,
        'overall_insights': overall_insights,
        'home_away_insights': home_away_insights,
        'similar_rank_stats': similar_rank_stats,
        'home_standing': home_standing,
        'away_standing': away_standing,
        'full_table': full_table,
        'standings_updated_at': standings_updated_at,
        'is_cup': is_cup,
        'home_cup_form': home_cup_form,
        'away_cup_form': away_cup_form,
    }
    
    return render(request, 'football/fixture_stats.html', context)




def fixture_deep_stats(request, match_id):
    """
    Display detailed match statistics for a specific past match.

    Flow:
        1. Query FixtureStatistics by match_id (PK)
        2. Pass real parent Fixture object to template for league/venue/referee
        3. Override team display with names actually stored in the stats row
           (past match teams may differ from parent fixture teams)
    """

    # ── Query stats by match_id (PK) ──────────────────────────────────────
    try:
        stats = FixtureStatistics.objects.select_related(
            'fixture__league__country',
            'fixture__home_team',
            'fixture__away_team',
        ).get(match_id=match_id)
        has_stats = True

    except FixtureStatistics.DoesNotExist:
        return render(request, 'football/fixture_deep_stats.html', {
            'has_stats': False,
            'fixture':   None,
            'stats':     None,
            'insights':  None,
        })

    # ── Real parent fixture — template uses .league, .venue, .referee, etc. ─
    parent_fixture = stats.fixture

    # ── Team logos via team IDs stored in the stats row ───────────────────
    try:
        home_team = Team.objects.get(id=stats.home_team_id)
    except Team.DoesNotExist:
        home_team = None

    try:
        away_team = Team.objects.get(id=stats.away_team_id)
    except Team.DoesNotExist:
        away_team = None

    # ── Score from the past match ──────────────────────────────────────────
    past_score = {
        'home_ft':  stats.home_fulltime_goals,
        'away_ft':  stats.away_fulltime_goals,
        'home_ht':  stats.home_half_time_goals,
        'away_ht':  stats.away_half_time_goals,
        'home_pen': stats.home_penalty_goals,
        'away_pen': stats.away_penalty_goals,
    }

    # ── Shooting ───────────────────────────────────────────────────────────
    home_total_shots     = stats.home_total_shots     or 0
    away_total_shots     = stats.away_total_shots     or 0
    home_shots_on_target = stats.home_shots_on_goal   or 0
    away_shots_on_target = stats.away_shots_on_goal   or 0
    home_inside_box      = stats.home_shots_insidebox or 0
    away_inside_box      = stats.away_shots_insidebox or 0

    home_shot_accuracy   = round((home_shots_on_target / home_total_shots) * 100, 1) if home_total_shots else 0
    away_shot_accuracy   = round((away_shots_on_target / away_total_shots) * 100, 1) if away_total_shots else 0
    home_inside_box_pct  = round((home_inside_box      / home_total_shots) * 100, 1) if home_total_shots else 0
    away_inside_box_pct  = round((away_inside_box      / away_total_shots) * 100, 1) if away_total_shots else 0

    # ── Possession & passing ───────────────────────────────────────────────
    home_possession  = stats.home_ball_possession    or 0
    away_possession  = stats.away_ball_possession    or 0
    home_pass_acc    = stats.home_passes_percentage  or 0
    away_pass_acc    = stats.away_passes_percentage  or 0

    possession_leader    = (
        "home"  if home_possession > away_possession else
        "away"  if away_possession > home_possession else
        "equal"
    )
    possession_advantage = abs(home_possession - away_possession)

    # ── Discipline ─────────────────────────────────────────────────────────
    home_yellows = stats.home_yellow_cards or 0
    away_yellows = stats.away_yellow_cards or 0
    home_reds    = stats.home_red_cards    or 0
    away_reds    = stats.away_red_cards    or 0
    home_fouls   = stats.home_fouls        or 0
    away_fouls   = stats.away_fouls        or 0

    home_discipline_score = home_yellows + (home_reds * 3)
    away_discipline_score = away_yellows + (away_reds * 3)

    # ── Attack ─────────────────────────────────────────────────────────────
    home_corners         = stats.home_corner_kicks or 0
    away_corners         = stats.away_corner_kicks or 0
    home_attacking_threat = home_corners + home_shots_on_target + home_inside_box
    away_attacking_threat = away_corners + away_shots_on_target + away_inside_box

    # ── Defence ────────────────────────────────────────────────────────────
    home_saves  = stats.home_goalkeeper_saves or 0
    away_saves  = stats.away_goalkeeper_saves or 0
    home_blocks = stats.home_blocked_shots    or 0
    away_blocks = stats.away_blocked_shots    or 0

    home_save_pct = round((home_saves / away_shots_on_target) * 100, 1) if away_shots_on_target else 0
    away_save_pct = round((away_saves / home_shots_on_target) * 100, 1) if home_shots_on_target else 0

    # ── Expected goals ─────────────────────────────────────────────────────
    home_xg = float(stats.home_expected_goals or 0)
    away_xg = float(stats.away_expected_goals or 0)

    # ── Territory ──────────────────────────────────────────────────────────
    home_offsides  = stats.home_offsides or 0
    away_offsides  = stats.away_offsides or 0
    home_territory = home_corners + home_offsides
    away_territory = away_corners + away_offsides

    # ── Insights dict ──────────────────────────────────────────────────────
    insights = {
        'shooting': {
            'home_total':         home_total_shots,
            'away_total':         away_total_shots,
            'home_accuracy':      home_shot_accuracy,
            'away_accuracy':      away_shot_accuracy,
            'home_inside_box_pct': home_inside_box_pct,
            'away_inside_box_pct': away_inside_box_pct,
            'shots_leader': (
                'home'  if home_total_shots > away_total_shots else
                'away'  if away_total_shots > home_total_shots else
                'equal'
            ),
        },
        'possession': {
            'home':             home_possession,
            'away':             away_possession,
            'leader':           possession_leader,
            'advantage':        possession_advantage,
            'home_pass_accuracy': home_pass_acc,
            'away_pass_accuracy': away_pass_acc,
        },
        'discipline': {
            'home_score': home_discipline_score,
            'away_score': away_discipline_score,
            'home_fouls': home_fouls,
            'away_fouls': away_fouls,
            'cleaner_team': (
                'home'  if home_discipline_score < away_discipline_score else
                'away'  if away_discipline_score < home_discipline_score else
                'equal'
            ),
        },
        'attack': {
            'home_threat':  home_attacking_threat,
            'away_threat':  away_attacking_threat,
            'home_corners': home_corners,
            'away_corners': away_corners,
            'attacking_leader': (
                'home'  if home_attacking_threat > away_attacking_threat else
                'away'  if away_attacking_threat > home_attacking_threat else
                'equal'
            ),
        },
        'defense': {
            'home_saves':    home_saves,
            'away_saves':    away_saves,
            'home_save_pct': home_save_pct,
            'away_save_pct': away_save_pct,
            'home_blocks':   home_blocks,
            'away_blocks':   away_blocks,
        },
        'xg': {
            'home':           home_xg,
            'away':           away_xg,
            'xg_difference':  abs(home_xg - away_xg),
            'xg_leader': (
                'home'  if home_xg > away_xg else
                'away'  if away_xg > home_xg else
                'equal'
            ),
        },
        'control': {
            'possession_leader': possession_leader,
            'territory_leader': (
                'home'  if home_territory > away_territory else
                'away'  if away_territory > home_territory else
                'equal'
            ),
            'dominant_team': possession_leader if possession_advantage > 15 else 'balanced',
        },
    }

    context = {
        # Real Fixture object — template uses fixture.league, fixture.venue,
        # fixture.referee, fixture.league.country.name, fixture.slug, etc.
        'fixture':   parent_fixture,

        # Past match team display — may differ from parent fixture teams
        'past_match_home_name': stats.home_team_name,
        'past_match_away_name': stats.away_team_name,
        'home_team_logo':       home_team.logo if home_team else None,
        'away_team_logo':       away_team.logo if away_team else None,

        # Score lines from the past match
        'past_score': past_score,

        # Raw stats row and computed insights
        'stats':     stats,
        'has_stats': has_stats,
        'insights':  insights,

        # Passed explicitly so template can reference without going through fixture
        'match_id':  match_id,
    }

    return render(request, 'football/fixture_deep_stats.html', context)