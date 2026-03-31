from django.shortcuts import render, get_object_or_404

from django.http import HttpResponseBadRequest
from django.core.cache import cache
from datetime import datetime, timedelta
from football.models import Team, Fixture, League, HeadToHeadMatch, FixtureAdvancedStats, FixtureStatistics
from collections import defaultdict

_24H  = 86400
_7DAY = 604800


def _serialize_standing(s):
    if s is None:
        return None
    return {
        'rank':            s.rank,
        'points':          s.points,
        'wins':            s.wins,
        'draws':           s.draws,
        'losses':          s.losses,
        'last_five':       s.last_five,
        'matches_played':  s.matches_played,
        'goals_for':       s.goals_for,
        'goals_against':   s.goals_against,
        'goal_difference': s.goal_difference,
        'group_name':      s.group_name or '',
        'group_label':     _group_label(s.group_name),
    }


def _serialize_table_row(row):
    return {
        'rank':            row.rank,
        'points':          row.points,
        'matches_played':  row.matches_played,
        'wins':            row.wins,
        'draws':           row.draws,
        'losses':          row.losses,
        'goals_for':       row.goals_for,
        'goals_against':   row.goals_against,
        'goal_difference': row.goal_difference,
        'group_name':      row.group_name or '',
        'team': {
            'id':   row.team.id,
            'name': row.team.name,
            'logo': row.team.logo,
        },
    }


_KNOCKOUT_KEYWORDS = (
    'final', 'semi-final', 'quarter-final', 'round of',
    'play-off', '3rd place', 'third place',
)

def _is_knockout_round(round_name):
    """Return True if the round name indicates a knockout (elimination) stage."""
    if not round_name:
        return False
    name_lower = round_name.lower()
    return any(kw in name_lower for kw in _KNOCKOUT_KEYWORDS)


def _group_label(full_group_name):
    """Shorten 'Apertura, Group A' → 'Group A' for display."""
    if not full_group_name:
        return ''
    parts = full_group_name.split(', ')
    return parts[-1] if len(parts) > 1 else full_group_name


def _serialize_h2h_match(m):
    return {
        'home_name':            m.home_name,
        'away_name':            m.away_name,
        'home_fulltime_goals':  m.home_fulltime_goals,
        'away_fulltime_goals':  m.away_fulltime_goals,
        'home_half_time_goals': m.home_half_time_goals,
        'away_half_time_goals': m.away_half_time_goals,
        'date':                 m.date.strftime("%-d %b %Y") if m.date else '',
        'league_name':          m.league_name or '',
        'past_fixture_id':      m.past_fixture_id,
    }


def _serialize_advanced_stats(a):
    if a is None:
        return None
    return {
        'home_last_5_form':             a.home_last_5_form,
        'home_last_5_home_form':        a.home_last_5_home_form,
        'home_last_5_vs_similar_rank':  a.home_last_5_vs_similar_rank,
        'home_wins_last_5':             a.home_wins_last_5,
        'home_draws_last_5':            a.home_draws_last_5,
        'home_losses_last_5':           a.home_losses_last_5,
        'home_goals_scored_last_5':     a.home_goals_scored_last_5,
        'home_goals_conceded_last_5':   a.home_goals_conceded_last_5,
        'home_home_wins_last_5':        a.home_home_wins_last_5,
        'home_home_draws_last_5':       a.home_home_draws_last_5,
        'home_home_losses_last_5':      a.home_home_losses_last_5,
        'away_last_5_form':             a.away_last_5_form,
        'away_last_5_away_form':        a.away_last_5_away_form,
        'away_last_5_vs_similar_rank':  a.away_last_5_vs_similar_rank,
        'away_wins_last_5':             a.away_wins_last_5,
        'away_draws_last_5':            a.away_draws_last_5,
        'away_losses_last_5':           a.away_losses_last_5,
        'away_goals_scored_last_5':     a.away_goals_scored_last_5,
        'away_goals_conceded_last_5':   a.away_goals_conceded_last_5,
        'away_away_wins_last_5':        a.away_away_wins_last_5,
        'away_away_draws_last_5':       a.away_away_draws_last_5,
        'away_away_losses_last_5':      a.away_away_losses_last_5,
    }

def _serialize_fixture_card(f):
    return {
        'id':             f.id,
        'slug':           f.slug,
        'date':           f.date,
        'home_team_name': f.home_team.name,
        'home_team_logo': f.home_team.logo,
        'home_team_id':   f.home_team.id,
        'away_team_name': f.away_team.name,
        'away_team_logo': f.away_team.logo,
    }


def home(request):
    today         = datetime.today().date()
    selected_date = request.GET.get('date', today.strftime("%Y-%m-%d"))
    days_ahead    = 7 if request.user.is_staff else 5

    try:
        datetime.strptime(selected_date, '%Y-%m-%d')
    except ValueError:
        selected_date = today.strftime("%Y-%m-%d")

    # Parse league filter once, up front
    try:
        selected_league_id = int(request.GET.get('league') or 0) or None
    except (ValueError, TypeError):
        selected_league_id = None

    dates = []
    for i in range(days_ahead):
        d = today + timedelta(days=i)
        dates.append({
            "iso":      d.strftime("%Y-%m-%d"),
            "day":      d.strftime("%a").upper(),
            "num":      d.strftime("%d"),
            "mon":      d.strftime("%b").upper(),
            "is_today": i == 0,
            "label":    d.strftime("%A, %d %b %Y"),
        })

    sidebar_cache_key = f'sidebar_{selected_date}'
    cache_key         = f'home_{selected_date}_{selected_league_id or "all"}'

    sidebar_cached = cache.get(sidebar_cache_key)
    main_cached    = cache.get(cache_key)

    # ── Both cached: fastest path, zero DB hits ───────────────────────────
    if sidebar_cached and main_cached:
        context = {
            'dates':                 dates,
            'selected_date':         selected_date,
            'selected_league_id':    selected_league_id,
            'countries':             sidebar_cached['countries'],
            'total_fixtures':        sidebar_cached['total_fixtures'],
            'leagues_with_fixtures': main_cached['leagues_with_fixtures'],
            'display_mode':          main_cached['display_mode'],
        }
        if request.headers.get('HX-Request'):
            return render(request, 'football/partials/home_htmx.html', context)
        return render(request, 'football/home.html', context)

    # ── Base queryset — only fields the templates actually use ─────────────
    base_qs = (
        Fixture.objects
        .filter(date__date=selected_date)
        .select_related('home_team', 'away_team', 'league', 'league__country')
        .order_by('league__priority', 'league__name', 'date')
    )

    # ── Fetch fixtures ─────────────────────────────────────────────────────
    if not sidebar_cached:
        # Need ALL fixtures to build the sidebar — one query covers both
        all_fixtures = list(base_qs)

        # Build sidebar from the already-loaded set
        countries_dict = defaultdict(lambda: {'leagues': {}, 'total_fixtures': 0})
        for fixture in all_fixtures:
            country_name = fixture.league.country.name
            lg_id        = fixture.league.id
            if lg_id not in countries_dict[country_name]['leagues']:
                countries_dict[country_name]['leagues'][lg_id] = {
                    'id':          lg_id,
                    'name':        fixture.league.name,
                    'logo':        fixture.league.logo,
                    'is_priority': fixture.league.priority <= 20,
                    'priority':    fixture.league.priority,
                    'count':       0,
                }
            countries_dict[country_name]['leagues'][lg_id]['count'] += 1
            countries_dict[country_name]['total_fixtures'] += 1

        countries_data = []
        for country_name in sorted(countries_dict.keys()):
            country_info = countries_dict[country_name]
            leagues_list = sorted(
                country_info['leagues'].values(),
                key=lambda x: (x['priority'] if x['priority'] else 999, x['name'])
            )
            countries_data.append({
                'name':           country_name,
                'total_fixtures': country_info['total_fixtures'],
                'leagues':        leagues_list,
            })

        total_fixtures = sum(c['total_fixtures'] for c in countries_data)
        cache.set(sidebar_cache_key, {
            'countries':      countries_data,
            'total_fixtures': total_fixtures,
        }, timeout=_24H)

        # Narrow to league filter in Python — no extra DB hit
        if selected_league_id:
            all_fixtures = [f for f in all_fixtures if f.league_id == selected_league_id]
    else:
        countries_data = sidebar_cached['countries']
        total_fixtures = sidebar_cached['total_fixtures']

        # Sidebar already cached — fetch only what main content needs
        if selected_league_id:
            all_fixtures = list(base_qs.filter(league_id=selected_league_id)[:100])
        else:
            all_fixtures = list(base_qs[:100])

    # ── Build main content from fixtures ──────────────────────────────────
    if selected_league_id:
        fixtures     = all_fixtures[:100]
        display_mode = 'league_filter'
        priority_league_id_set = set()
    else:
        priority_list = [f for f in all_fixtures if f.league.priority <= 20][:100]
        if priority_list:
            # Priority leagues are playing — show only them, skip non-priority noise
            fixtures               = priority_list
            display_mode           = 'priority_leagues'
            priority_league_id_set = {f.league_id for f in fixtures}
        else:
            # Quiet day — no priority fixtures, fall back to whatever is available
            fixtures               = all_fixtures[:100]
            display_mode           = 'alphabetical_fallback'
            priority_league_id_set = set()

    fixtures_by_league = defaultdict(list)
    for fixture in fixtures:
        fixtures_by_league[fixture.league].append(fixture)

    if display_mode == 'priority_leagues':
        priority_sorted = sorted(
            [(lg, f) for lg, f in fixtures_by_league.items() if lg.id in priority_league_id_set],
            key=lambda x: (x[0].priority or 999, x[0].name)
        )
        extra_sorted = sorted(
            [(lg, f) for lg, f in fixtures_by_league.items() if lg.id not in priority_league_id_set],
            key=lambda x: x[0].name
        )[:15]
        sorted_leagues = priority_sorted + extra_sorted
    else:
        sorted_leagues = sorted(fixtures_by_league.items(), key=lambda x: x[0].name)

    leagues_with_fixtures = []
    for league, league_fixtures in sorted_leagues:
        leagues_with_fixtures.append({
            'league_id':   league.id,
            'league_name': league.name,
            'league_logo': league.logo,
            'country':     league.country.name,
            'is_priority': league.priority <= 20,
            'fixtures':    [_serialize_fixture_card(f) for f in league_fixtures],
        })

    cache.set(cache_key, {
        'leagues_with_fixtures': leagues_with_fixtures,
        'display_mode':          display_mode,
    }, timeout=_24H)

    context = {
        'dates':                 dates,
        'selected_date':         selected_date,
        'selected_league_id':    selected_league_id,
        'countries':             countries_data,
        'total_fixtures':        total_fixtures,
        'leagues_with_fixtures': leagues_with_fixtures,
        'display_mode':          display_mode,
    }

    if request.headers.get('HX-Request'):
        return render(request, 'football/partials/home_htmx.html', context)

    return render(request, 'football/home.html', context)

def matches(request):
    """
    Filter matches by league (for sidebar league filtering)
    """
    league_id = request.GET.get('league')
    date_str  = request.GET.get('date')

    if not date_str:
        return HttpResponseBadRequest("Date parameter is required")

    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return HttpResponseBadRequest("Invalid date format. Use YYYY-MM-DD")

    cache_key = f'matches_{date_str}_{league_id or "all"}'
    cached    = cache.get(cache_key)

    if cached:
        return render(request, 'football/matches.html', {
            'fixtures':        cached['fixtures'],
            'selected_date':   date_str,
            'selected_league': league_id,
            'league_info':     cached['league_info'],
        })

    fixtures = Fixture.objects.filter(
        date__date=date_str
    ).select_related(
        'home_team', 'away_team', 'league', 'league__country'
    ).order_by('date')

    league_info = None
    if league_id:
        try:
            league_id = int(league_id)
            fixtures  = fixtures.filter(league_id=league_id)
            try:
                league      = League.objects.select_related('country').get(id=league_id)
                league_info = {
                    'name':    league.name,
                    'logo':    league.logo,
                    'country': league.country.name,
                }
            except League.DoesNotExist:
                pass
        except (ValueError, TypeError):
            return HttpResponseBadRequest("Invalid league ID")

    fixtures = list(fixtures)

    cache.set(cache_key, {
        'fixtures':    fixtures,
        'league_info': league_info,
    }, timeout=_24H)

    context = {
        'fixtures':        fixtures,
        'selected_date':   date_str,
        'selected_league': league_id,
        'league_info':     league_info,
    }

    return render(request, 'football/matches.html', context)



def fixture_stats(request, fixture_id, slug=None):
    from football.models import LeagueTableSnapshot

    # Always fetch fixture fresh — status/date can change
    fixture = get_object_or_404(
        Fixture.objects.select_related(
            'home_team', 'away_team', 'league', 'league__country'
        ),
        id=fixture_id
    )

    cache_key = f'fixture_stats_{fixture_id}'
    cached    = cache.get(cache_key)

    if cached:
        context = {
            'fixture':           fixture,
            'h2h_matches':       cached['h2h_matches'],
            'h2h_summary':       cached['h2h_summary'],
            'advanced_stats':    cached['advanced_stats'],
            'overall_insights':  cached['overall_insights'],
            'home_away_insights':cached['home_away_insights'],
            'similar_rank_stats':cached['similar_rank_stats'],
            'home_standing':     cached['home_standing'],
            'away_standing':     cached['away_standing'],
            'full_table':        cached['full_table'],
            'standings_updated_at': cached['standings_updated_at'],
            'is_cup':            cached['is_cup'],
            'is_friendly':       cached.get('is_friendly', False),
            'home_cup_form':     cached['home_cup_form'],
            'away_cup_form':     cached['away_cup_form'],
            'home_cup_stats':    cached.get('home_cup_stats'),
            'away_cup_stats':    cached.get('away_cup_stats'),
        }
        return render(request, 'football/fixture_stats.html', context)

    # ── Cache miss: run all queries and computation ───────────────────────

    # Determine league type early so we can skip standings queries that
    # don't apply. Standings are only meaningful for standard league
    # competitions. Cups use the cup-form section; friendlies have no
    # table at all.
    _league_type = (fixture.league.type or '').lower()
    is_cup       = _league_type == 'cup'
    is_friendly  = _league_type == 'friendly'

    # Standings are shown for:
    #   - all standard leagues (always have a table)
    #   - cup competitions in the group stage only (not once knockout rounds begin)
    shows_standings = (
        _league_type == 'league' or
        (is_cup and not _is_knockout_round(fixture.round))
    )

    home_standing        = None
    away_standing        = None
    full_table_grouped   = []
    standings_updated_at = None

    if shows_standings:
        try:
            home_standing = LeagueTableSnapshot.objects.get(
                team=fixture.home_team,
                league=fixture.league,
                season=fixture.league.season
            )
        except LeagueTableSnapshot.DoesNotExist:
            pass

        try:
            away_standing = LeagueTableSnapshot.objects.get(
                team=fixture.away_team,
                league=fixture.league,
                season=fixture.league.season
            )
        except LeagueTableSnapshot.DoesNotExist:
            pass

        full_table_qs = LeagueTableSnapshot.objects.filter(
            league=fixture.league,
            season=fixture.league.season
        ).select_related('team').order_by('group_name', 'rank')

        first_snapshot       = full_table_qs.first()
        standings_updated_at = first_snapshot.updated_at if first_snapshot else None

        # Group rows by group_name so the template can render section headers.
        # For single-group leagues every row has group_name='' so this produces
        # exactly one group and the template renders identically to before.
        _seen_groups = []
        _group_rows  = {}
        for row in full_table_qs:
            gn = row.group_name or ''
            if gn not in _group_rows:
                _seen_groups.append(gn)
                _group_rows[gn] = []
            _group_rows[gn].append(row)

        full_table_grouped = [
            {'group_name': gn, 'label': _group_label(gn), 'rows': _group_rows[gn]}
            for gn in _seen_groups
        ]

        # For standard leagues with multiple groups (e.g. Apertura/Clausura),
        # only the first 2 groups carry the current-season data. Cups (UCL,
        # World Cup) can have Group A–H so we keep all of them.
        if _league_type == 'league' and len(full_table_grouped) > 2:
            full_table_grouped = full_table_grouped[:2]

    h2h_matches = HeadToHeadMatch.objects.filter(
        fixture=fixture
    ).exclude(
        past_fixture_id=fixture.id
    ).order_by('-date')[:6]

    h2h_matches_display = [
        m for m in h2h_matches
        if m.home_fulltime_goals is not None and m.away_fulltime_goals is not None
    ]

    try:
        advanced_stats = FixtureAdvancedStats.objects.get(fixture=fixture)
    except FixtureAdvancedStats.DoesNotExist:
        advanced_stats = None

    # ── H2H summary ───────────────────────────────────────────────────────
    h2h_summary = {
        'total':      h2h_matches.count(),
        'home_wins':  0,
        'away_wins':  0,
        'draws':      0,
        'home_goals': 0,
        'away_goals': 0,
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

    completed_matches = sum(1 for m in h2h_matches if m.home_fulltime_goals is not None)
    h2h_summary['avg_goals'] = (
        round((h2h_summary['home_goals'] + h2h_summary['away_goals']) / completed_matches, 1)
        if completed_matches else 0
    )

    # ── Form helpers ──────────────────────────────────────────────────────
    def calculate_form_rating(wins, draws, losses):
        total = wins + draws + losses
        if not total:
            return 0
        return round(((wins * 3 + draws) / (total * 3)) * 100, 1)

    def categorize_form(rating):
        if rating >= 60:
            return 'Strong'
        elif rating >= 40:
            return 'Fair'
        return 'Poor'

    # ── Insights ──────────────────────────────────────────────────────────
    overall_insights   = None
    home_away_insights = None
    similar_rank_stats = None

    # ── Cup form ──────────────────────────────────────────────────────────
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
                    'result':         s.result,
                    'is_home':        s.is_home,
                    'opponent':       s.opponent.name,
                    'goals_scored':   gs,
                    'goals_conceded': gc,
                    'fixture_id':     s.fixture_id,
                })
            return result

        home_cup_form = _build_cup_form(fixture.home_team)
        away_cup_form = _build_cup_form(fixture.away_team)

    def _cup_stats(form_list):
        wins   = sum(1 for r in form_list if r.get('result') == 'W')
        draws  = sum(1 for r in form_list if r.get('result') == 'D')
        losses = sum(1 for r in form_list if r.get('result') == 'L')
        rating = calculate_form_rating(wins, draws, losses)
        return {
            'wins':   wins,
            'draws':  draws,
            'losses': losses,
            'rating': rating,
            'label':  categorize_form(rating),
            'total':  wins + draws + losses,
        }

    home_cup_stats = _cup_stats(home_cup_form) if is_cup else None
    away_cup_stats = _cup_stats(away_cup_form) if is_cup else None

    if advanced_stats:
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
            'home_win_rate':  round((advanced_stats.home_wins_last_5 / home_total_last_5) * 100, 1) if home_total_last_5 else 0,
            'home_avg_goals': round(advanced_stats.home_goals_scored_last_5 / home_total_last_5, 1) if home_total_last_5 else 0,
            'away_win_rate':  round((advanced_stats.away_wins_last_5 / away_total_last_5) * 100, 1) if away_total_last_5 else 0,
            'away_avg_goals': round(advanced_stats.away_goals_scored_last_5 / away_total_last_5, 1) if away_total_last_5 else 0,
        }

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
            'home_advantage':     categorize_form(home_home_rating) if home_home_total > 0 else None,
            'home_home_rating':   home_home_rating,
            'away_away_win_rate': round((advanced_stats.away_away_wins_last_5 / away_away_total) * 100, 1) if away_away_total else 0,
            'away_strength':      categorize_form(away_away_rating) if away_away_total > 0 else None,
            'away_away_rating':   away_away_rating,
        }

        home_similar = advanced_stats.home_last_5_vs_similar_rank or []
        away_similar = advanced_stats.away_last_5_vs_similar_rank or []

        def count_results(matches_list):
            real = [m for m in matches_list if isinstance(m, dict) and m.get('opponent') != 'No data']
            wins   = sum(1 for m in real if m.get('result') == 'W')
            draws  = sum(1 for m in real if m.get('result') == 'D')
            losses = sum(1 for m in real if m.get('result') == 'L')
            return wins, draws, losses

        home_sw, home_sd, home_sl = count_results(home_similar)
        away_sw, away_sd, away_sl = count_results(away_similar)
        home_similar_rating       = calculate_form_rating(home_sw, home_sd, home_sl)
        away_similar_rating       = calculate_form_rating(away_sw, away_sd, away_sl)
        home_similar_total        = home_sw + home_sd + home_sl
        away_similar_total        = away_sw + away_sd + away_sl

        similar_rank_stats = {
            'home_wins':   home_sw,
            'home_draws':  home_sd,
            'home_losses': home_sl,
            'home_total':  home_similar_total,
            'home_record': categorize_form(home_similar_rating) if home_similar_total > 0 else None,
            'home_rating': home_similar_rating,
            'away_wins':   away_sw,
            'away_draws':  away_sd,
            'away_losses': away_sl,
            'away_total':  away_similar_total,
            'away_record': categorize_form(away_similar_rating) if away_similar_total > 0 else None,
            'away_rating': away_similar_rating,
            'has_data':    home_similar_total > 0 or away_similar_total > 0,
        }

    # ── Serialize and cache ───────────────────────────────────────────────
    cache.set(cache_key, {
        'h2h_matches':        [_serialize_h2h_match(m) for m in h2h_matches_display],
        'h2h_summary':        h2h_summary,
        'advanced_stats':     _serialize_advanced_stats(advanced_stats),
        'overall_insights':   overall_insights,
        'home_away_insights': home_away_insights,
        'similar_rank_stats': similar_rank_stats,
        'home_standing':      _serialize_standing(home_standing),
        'away_standing':      _serialize_standing(away_standing),
        'full_table':         [
            {
                'group_name': g['group_name'],
                'label':      g['label'],
                'rows':       [_serialize_table_row(r) for r in g['rows']],
            }
            for g in full_table_grouped
        ],
        'standings_updated_at': standings_updated_at,
        'is_cup':             is_cup,
        'is_friendly':        is_friendly,
        'home_cup_form':      home_cup_form,
        'away_cup_form':      away_cup_form,
        'home_cup_stats':     home_cup_stats,
        'away_cup_stats':     away_cup_stats,
    }, timeout=_24H)

    context = {
        'fixture':            fixture,
        'h2h_matches':        [_serialize_h2h_match(m) for m in h2h_matches_display],
        'h2h_summary':        h2h_summary,
        'advanced_stats':     advanced_stats,
        'overall_insights':   overall_insights,
        'home_away_insights': home_away_insights,
        'similar_rank_stats': similar_rank_stats,
        'home_standing':      _serialize_standing(home_standing),
        'away_standing':      _serialize_standing(away_standing),
        'full_table':         full_table_grouped,
        'standings_updated_at': standings_updated_at,
        'is_cup':             is_cup,
        'is_friendly':        is_friendly,
        'home_cup_form':      home_cup_form,
        'away_cup_form':      away_cup_form,
        'home_cup_stats':     home_cup_stats,
        'away_cup_stats':     away_cup_stats,
    }

    return render(request, 'football/fixture_stats.html', context)




def fixture_deep_stats(request, match_id):
    """
    Display detailed match statistics for a specific past match.
    Historical data never changes — insights and team logos are cached for 7 days.
    On cache hit: 1 query (stats + fixture via select_related). Zero computation.
    """

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

    parent_fixture = stats.fixture

    cache_key = f'deep_stats_{match_id}'
    cached    = cache.get(cache_key)

    if cached:
        context = {
            'fixture':              parent_fixture,
            'stats':                stats,
            'has_stats':            has_stats,
            'match_id':             match_id,
            'past_match_home_name': stats.home_team_name,
            'past_match_away_name': stats.away_team_name,
            'home_team_logo':       cached['home_team_logo'],
            'away_team_logo':       cached['away_team_logo'],
            'past_score':           cached['past_score'],
            'insights':             cached['insights'],
        }
        return render(request, 'football/fixture_deep_stats.html', context)

    # ── Cache miss: fetch team logos and compute insights ─────────────────

    try:
        home_team = Team.objects.get(id=stats.home_team_id)
    except Team.DoesNotExist:
        home_team = None

    try:
        away_team = Team.objects.get(id=stats.away_team_id)
    except Team.DoesNotExist:
        away_team = None

    past_score = {
        'home_ft':  stats.home_fulltime_goals,
        'away_ft':  stats.away_fulltime_goals,
        'home_ht':  stats.home_half_time_goals,
        'away_ht':  stats.away_half_time_goals,
        'home_pen': stats.home_penalty_goals,
        'away_pen': stats.away_penalty_goals,
    }

    home_total_shots     = stats.home_total_shots     or 0
    away_total_shots     = stats.away_total_shots     or 0
    home_shots_on_target = stats.home_shots_on_goal   or 0
    away_shots_on_target = stats.away_shots_on_goal   or 0
    home_inside_box      = stats.home_shots_insidebox or 0
    away_inside_box      = stats.away_shots_insidebox or 0

    home_shot_accuracy  = round((home_shots_on_target / home_total_shots) * 100, 1) if home_total_shots else 0
    away_shot_accuracy  = round((away_shots_on_target / away_total_shots) * 100, 1) if away_total_shots else 0
    home_inside_box_pct = round((home_inside_box      / home_total_shots) * 100, 1) if home_total_shots else 0
    away_inside_box_pct = round((away_inside_box      / away_total_shots) * 100, 1) if away_total_shots else 0

    home_possession      = stats.home_ball_possession   or 0
    away_possession      = stats.away_ball_possession   or 0
    home_pass_acc        = stats.home_passes_percentage or 0
    away_pass_acc        = stats.away_passes_percentage or 0
    possession_leader    = (
        'home'  if home_possession > away_possession else
        'away'  if away_possession > home_possession else
        'equal'
    )
    possession_advantage = abs(home_possession - away_possession)

    home_yellows          = stats.home_yellow_cards or 0
    away_yellows          = stats.away_yellow_cards or 0
    home_reds             = stats.home_red_cards    or 0
    away_reds             = stats.away_red_cards    or 0
    home_fouls            = stats.home_fouls        or 0
    away_fouls            = stats.away_fouls        or 0
    home_discipline_score = home_yellows + (home_reds * 3)
    away_discipline_score = away_yellows + (away_reds * 3)

    home_corners          = stats.home_corner_kicks or 0
    away_corners          = stats.away_corner_kicks or 0
    home_attacking_threat = home_corners + home_shots_on_target + home_inside_box
    away_attacking_threat = away_corners + away_shots_on_target + away_inside_box

    home_saves    = stats.home_goalkeeper_saves or 0
    away_saves    = stats.away_goalkeeper_saves or 0
    home_blocks   = stats.home_blocked_shots    or 0
    away_blocks   = stats.away_blocked_shots    or 0
    home_save_pct = round((home_saves / away_shots_on_target) * 100, 1) if away_shots_on_target else 0
    away_save_pct = round((away_saves / home_shots_on_target) * 100, 1) if home_shots_on_target else 0

    home_xg        = float(stats.home_expected_goals or 0)
    away_xg        = float(stats.away_expected_goals or 0)
    home_offsides  = stats.home_offsides or 0
    away_offsides  = stats.away_offsides or 0
    home_territory = home_corners + home_offsides
    away_territory = away_corners + away_offsides

    insights = {
        'shooting': {
            'home_total':          home_total_shots,
            'away_total':          away_total_shots,
            'home_accuracy':       home_shot_accuracy,
            'away_accuracy':       away_shot_accuracy,
            'home_inside_box_pct': home_inside_box_pct,
            'away_inside_box_pct': away_inside_box_pct,
            'shots_leader': (
                'home'  if home_total_shots > away_total_shots else
                'away'  if away_total_shots > home_total_shots else
                'equal'
            ),
        },
        'possession': {
            'home':               home_possession,
            'away':               away_possession,
            'leader':             possession_leader,
            'advantage':          possession_advantage,
            'home_pass_accuracy': home_pass_acc,
            'away_pass_accuracy': away_pass_acc,
        },
        'discipline': {
            'home_score':   home_discipline_score,
            'away_score':   away_discipline_score,
            'home_fouls':   home_fouls,
            'away_fouls':   away_fouls,
            'cleaner_team': (
                'home'  if home_discipline_score < away_discipline_score else
                'away'  if away_discipline_score < home_discipline_score else
                'equal'
            ),
        },
        'attack': {
            'home_threat':      home_attacking_threat,
            'away_threat':      away_attacking_threat,
            'home_corners':     home_corners,
            'away_corners':     away_corners,
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
            'home':          home_xg,
            'away':          away_xg,
            'xg_difference': abs(home_xg - away_xg),
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

    home_logo = home_team.logo if home_team else None
    away_logo = away_team.logo if away_team else None

    cache.set(cache_key, {
        'past_score':     past_score,
        'insights':       insights,
        'home_team_logo': home_logo,
        'away_team_logo': away_logo,
    }, timeout=_7DAY)

    context = {
        'fixture':              parent_fixture,
        'stats':                stats,
        'has_stats':            has_stats,
        'match_id':             match_id,
        'past_match_home_name': stats.home_team_name,
        'past_match_away_name': stats.away_team_name,
        'home_team_logo':       home_logo,
        'away_team_logo':       away_logo,
        'past_score':           past_score,
        'insights':             insights,
    }

    return render(request, 'football/fixture_deep_stats.html', context)