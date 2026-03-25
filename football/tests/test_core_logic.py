"""
Pure-Python unit tests for business logic embedded in views.py.

These tests do NOT hit the database. They verify the calculation
algorithms directly — form ratings, H2H attribution, deep-stats
insights — so regressions in the math are caught immediately.
"""
import pytest
from types import SimpleNamespace


# ── Helpers: replicate view-level functions ───────────────────────────────────

def calculate_form_rating(wins, draws, losses):
    """Replicated from fixture_stats view."""
    total_matches = wins + draws + losses
    if total_matches == 0:
        return 0
    points_earned = (wins * 3) + (draws * 1)
    max_possible = total_matches * 3
    return round((points_earned / max_possible) * 100, 1)


def categorize_form(rating):
    """Replicated from fixture_stats view."""
    if rating >= 60:
        return 'Strong'
    elif rating >= 40:
        return 'Fair'
    else:
        return 'Poor'


def count_results(matches_list):
    """Replicated from fixture_stats view (similar-rank results)."""
    wins  = sum(1 for m in matches_list if isinstance(m, dict) and m.get('result') == 'W')
    draws = sum(1 for m in matches_list if isinstance(m, dict) and m.get('result') == 'D')
    losses = sum(1 for m in matches_list if isinstance(m, dict) and m.get('result') == 'L')
    return wins, draws, losses


def compute_h2h_summary(fixture_home_name, fixture_away_name, h2h_matches):
    """
    Replicated from fixture_stats view.

    Critical invariant: goals/wins are attributed to the perspective of
    the *upcoming* fixture's home/away assignment regardless of which
    side each team played in the historical match.
    """
    summary = {
        'total': len(h2h_matches),
        'home_wins': 0,
        'away_wins': 0,
        'draws': 0,
        'home_goals': 0,
        'away_goals': 0,
    }

    for match in h2h_matches:
        if match.home_fulltime_goals is None or match.away_fulltime_goals is None:
            continue

        if match.home_name == fixture_home_name:
            summary['home_goals'] += match.home_fulltime_goals
            summary['away_goals'] += match.away_fulltime_goals
        else:
            # Teams were reversed in the historical record
            summary['home_goals'] += match.away_fulltime_goals
            summary['away_goals'] += match.home_fulltime_goals

        if match.home_fulltime_goals > match.away_fulltime_goals:
            if match.home_name == fixture_home_name:
                summary['home_wins'] += 1
            else:
                summary['away_wins'] += 1
        elif match.home_fulltime_goals < match.away_fulltime_goals:
            if match.away_name == fixture_away_name:
                summary['away_wins'] += 1
            else:
                summary['home_wins'] += 1
        else:
            summary['draws'] += 1

    completed = sum(1 for m in h2h_matches if m.home_fulltime_goals is not None)
    summary['avg_goals'] = (
        round((summary['home_goals'] + summary['away_goals']) / completed, 1)
        if completed > 0 else 0
    )
    return summary


def h2h_match(home_name, away_name, home_goals, away_goals):
    return SimpleNamespace(
        home_name=home_name,
        away_name=away_name,
        home_fulltime_goals=home_goals,
        away_fulltime_goals=away_goals,
    )


# ── Form Rating ───────────────────────────────────────────────────────────────

class TestCalculateFormRating:
    def test_perfect_record(self):
        assert calculate_form_rating(5, 0, 0) == 100.0

    def test_all_losses(self):
        assert calculate_form_rating(0, 0, 5) == 0.0

    def test_all_draws(self):
        # 5 draws → 5 pts out of 15 max
        assert calculate_form_rating(0, 5, 0) == round((5 / 15) * 100, 1)

    def test_mixed_record(self):
        # 3W 1D 1L → (9+1)/15 = 66.7%
        assert calculate_form_rating(3, 1, 1) == round((10 / 15) * 100, 1)

    def test_zero_matches(self):
        assert calculate_form_rating(0, 0, 0) == 0

    def test_single_win(self):
        assert calculate_form_rating(1, 0, 0) == 100.0

    def test_single_loss(self):
        assert calculate_form_rating(0, 0, 1) == 0.0

    def test_single_draw(self):
        assert calculate_form_rating(0, 1, 0) == round((1 / 3) * 100, 1)


# ── Categorize Form ───────────────────────────────────────────────────────────

class TestCategorizeForm:
    @pytest.mark.parametrize("rating,expected", [
        (100.0, 'Strong'),
        (60.0, 'Strong'),   # exact boundary — 60 is Strong
        (59.9, 'Fair'),
        (40.0, 'Fair'),     # exact boundary — 40 is Fair
        (39.9, 'Poor'),
        (0.0, 'Poor'),
    ])
    def test_categories(self, rating, expected):
        assert categorize_form(rating) == expected


# ── Count Results (Similar Rank) ──────────────────────────────────────────────

class TestCountResults:
    def test_all_wins(self):
        matches = [{'result': 'W'}, {'result': 'W'}, {'result': 'W'}]
        assert count_results(matches) == (3, 0, 0)

    def test_mixed(self):
        matches = [{'result': 'W'}, {'result': 'D'}, {'result': 'L'}, {'result': 'W'}]
        assert count_results(matches) == (2, 1, 1)

    def test_empty(self):
        assert count_results([]) == (0, 0, 0)

    def test_skips_non_dicts(self):
        matches = [{'result': 'W'}, "not a dict", None, {'result': 'L'}]
        assert count_results(matches) == (1, 0, 1)

    def test_skips_no_data_entries(self):
        matches = [{'result': 'W'}, {'opponent': 'No data'}]
        # Second entry has no 'result' key → counted as no match for W/D/L
        assert count_results(matches) == (1, 0, 0)


# ── H2H Summary ───────────────────────────────────────────────────────────────

class TestH2HSummary:
    HOME = "Manchester United"
    AWAY = "Liverpool"

    def test_home_win_when_home_team_played_at_home(self):
        matches = [h2h_match(self.HOME, self.AWAY, 3, 1)]
        s = compute_h2h_summary(self.HOME, self.AWAY, matches)
        assert s['home_wins'] == 1
        assert s['away_wins'] == 0
        assert s['home_goals'] == 3
        assert s['away_goals'] == 1

    def test_away_win_when_away_team_played_at_home(self):
        # Liverpool hosted Man Utd and won 2-0 in the historical record
        matches = [h2h_match(self.AWAY, self.HOME, 2, 0)]
        s = compute_h2h_summary(self.HOME, self.AWAY, matches)
        assert s['away_wins'] == 1
        assert s['home_wins'] == 0
        # Goals flipped: Man Utd got 0, Liverpool got 2
        assert s['home_goals'] == 0
        assert s['away_goals'] == 2

    def test_home_win_when_teams_reversed_in_h2h(self):
        # Liverpool (home) 0-1 Man Utd (away) — Man Utd won
        matches = [h2h_match(self.AWAY, self.HOME, 0, 1)]
        s = compute_h2h_summary(self.HOME, self.AWAY, matches)
        assert s['home_wins'] == 1
        assert s['away_wins'] == 0

    def test_draw(self):
        matches = [h2h_match(self.HOME, self.AWAY, 1, 1)]
        s = compute_h2h_summary(self.HOME, self.AWAY, matches)
        assert s['draws'] == 1
        assert s['home_wins'] == 0
        assert s['away_wins'] == 0

    def test_skips_incomplete_scores(self):
        null_match = SimpleNamespace(
            home_name=self.HOME, away_name=self.AWAY,
            home_fulltime_goals=None, away_fulltime_goals=None,
        )
        s = compute_h2h_summary(self.HOME, self.AWAY, [null_match])
        assert s['home_wins'] == 0
        assert s['home_goals'] == 0
        assert s['avg_goals'] == 0

    def test_avg_goals_calculation(self):
        matches = [
            h2h_match(self.HOME, self.AWAY, 2, 1),  # 3 goals
            h2h_match(self.HOME, self.AWAY, 1, 1),  # 2 goals
        ]
        s = compute_h2h_summary(self.HOME, self.AWAY, matches)
        assert s['avg_goals'] == 2.5  # 5 goals / 2 matches

    def test_multiple_mixed_matches(self):
        matches = [
            h2h_match(self.HOME, self.AWAY, 2, 0),  # home win
            h2h_match(self.AWAY, self.HOME, 1, 0),  # away win (reversed)
            h2h_match(self.HOME, self.AWAY, 1, 1),  # draw
        ]
        s = compute_h2h_summary(self.HOME, self.AWAY, matches)
        assert s['home_wins'] == 1
        assert s['away_wins'] == 1
        assert s['draws'] == 1


# ── Deep Stats Insights ───────────────────────────────────────────────────────
# Replicate the key calculations from fixture_deep_stats view.

def shot_accuracy(on_target, total):
    return round((on_target / total) * 100, 1) if total else 0


def save_pct(saves, opp_shots_on_target):
    return round((saves / opp_shots_on_target) * 100, 1) if opp_shots_on_target else 0


def discipline_score(yellows, reds):
    return yellows + (reds * 3)


def possession_leader(home_poss, away_poss):
    if home_poss > away_poss:
        return 'home'
    elif away_poss > home_poss:
        return 'away'
    return 'equal'


def attacking_threat(corners, shots_on_target, inside_box):
    return corners + shots_on_target + inside_box


def dominant_team(leader, advantage):
    return leader if advantage > 15 else 'balanced'


class TestShotAccuracy:
    def test_normal(self):
        assert shot_accuracy(5, 10) == 50.0

    def test_perfect(self):
        assert shot_accuracy(6, 6) == 100.0

    def test_zero_shots(self):
        assert shot_accuracy(0, 0) == 0

    def test_rounds_correctly(self):
        assert shot_accuracy(1, 3) == round((1 / 3) * 100, 1)


class TestSavePct:
    def test_normal(self):
        assert save_pct(4, 5) == 80.0

    def test_no_shots_on_target(self):
        assert save_pct(0, 0) == 0

    def test_all_saved(self):
        assert save_pct(5, 5) == 100.0


class TestDisciplineScore:
    def test_only_yellows(self):
        assert discipline_score(3, 0) == 3

    def test_only_reds(self):
        assert discipline_score(0, 2) == 6  # 2 * 3

    def test_mixed(self):
        assert discipline_score(2, 1) == 5  # 2 + 3

    def test_clean(self):
        assert discipline_score(0, 0) == 0


class TestPossessionLeader:
    def test_home_leads(self):
        assert possession_leader(60, 40) == 'home'

    def test_away_leads(self):
        assert possession_leader(40, 60) == 'away'

    def test_equal(self):
        assert possession_leader(50, 50) == 'equal'


class TestAttackingThreat:
    def test_combined(self):
        assert attacking_threat(5, 4, 6) == 15

    def test_zeros(self):
        assert attacking_threat(0, 0, 0) == 0


class TestDominantTeam:
    def test_dominant_home(self):
        assert dominant_team('home', 20) == 'home'

    def test_dominant_away(self):
        assert dominant_team('away', 30) == 'away'

    def test_balanced_low_advantage(self):
        assert dominant_team('home', 10) == 'balanced'

    def test_boundary_exactly_15(self):
        # advantage must be > 15 to be dominant
        assert dominant_team('home', 15) == 'balanced'

    def test_boundary_16(self):
        assert dominant_team('home', 16) == 'home'


# ── Overall Insights ──────────────────────────────────────────────────────────
# Replicate overall_insights calculation from fixture_stats.

def compute_overall_insights(home_wins, home_draws, home_losses,
                              home_goals, away_wins, away_draws,
                              away_losses, away_goals):
    home_total = home_wins + home_draws + home_losses
    away_total = away_wins + away_draws + away_losses
    return {
        'home_win_rate': round((home_wins / home_total) * 100, 1) if home_total else 0,
        'home_avg_goals': round(home_goals / home_total, 1) if home_total else 0,
        'away_win_rate': round((away_wins / away_total) * 100, 1) if away_total else 0,
        'away_avg_goals': round(away_goals / away_total, 1) if away_total else 0,
    }


class TestOverallInsights:
    def test_perfect_home(self):
        result = compute_overall_insights(5, 0, 0, 10, 2, 1, 2, 4)
        assert result['home_win_rate'] == 100.0
        assert result['home_avg_goals'] == 2.0

    def test_no_matches(self):
        result = compute_overall_insights(0, 0, 0, 0, 0, 0, 0, 0)
        assert result['home_win_rate'] == 0
        assert result['away_win_rate'] == 0
        assert result['home_avg_goals'] == 0
        assert result['away_avg_goals'] == 0

    def test_typical_record(self):
        # 3W 1D 1L from 5 matches, 8 goals
        result = compute_overall_insights(3, 1, 1, 8, 2, 2, 1, 5)
        assert result['home_win_rate'] == 60.0
        assert result['home_avg_goals'] == 1.6
