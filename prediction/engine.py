"""
engine.py — Pure scoring and accumulator-building logic.

No API calls, no DB calls, no Redis. All inputs come from the caller.
All methods wrapped in try/except. Type hints and docstrings throughout.
"""

import logging

from django.conf import settings

logger = logging.getLogger(__name__)


class PredictionEngine:
    """Scores fixtures and assembles accumulator bets."""

    # ------------------------------------------------------------------
    # Filtering
    # ------------------------------------------------------------------

    def filter_eligible_fixtures(
        self,
        fixtures: list[dict],
        standings: dict,
        predictions: dict,
        odds: dict,
        mode: str = "small",
    ) -> list[dict]:
        """
        Apply gate conditions and return only fixtures that pass all checks.

        Logs which gate failed at DEBUG level for each rejected fixture.
        """
        eligible: list[dict] = []

        for fixture in fixtures:
            fid = fixture.get("fixture_id")
            try:
                # --- All-modes gates ---
                if not fixture.get("advanced_stats"):
                    logger.debug(f"Gate: no advanced_stats — fixture {fid}")
                    continue
                if not predictions.get(fid):
                    logger.debug(f"Gate: no predictions — fixture {fid}")
                    continue
                if not odds.get(fid):
                    logger.debug(f"Gate: no odds — fixture {fid}")
                    continue

                standing = standings.get(fid, {})
                home_st = standing.get("home", {})
                away_st = standing.get("away", {})
                total_teams = standing.get("total_teams", 0)

                if mode == "small":
                    if home_st.get("matches_played", 0) < 6:
                        logger.debug(f"Gate: home matches_played < 6 — fixture {fid}")
                        continue
                    if away_st.get("matches_played", 0) < 6:
                        logger.debug(f"Gate: away matches_played < 6 — fixture {fid}")
                        continue
                    if total_teams < 6:
                        logger.debug(f"Gate: total_teams < 6 — fixture {fid}")
                        continue

                elif mode == "monster":
                    if home_st.get("matches_played", 0) < 5:
                        logger.debug(f"Gate: home matches_played < 5 (monster) — fixture {fid}")
                        continue
                    if away_st.get("matches_played", 0) < 5:
                        logger.debug(f"Gate: away matches_played < 5 (monster) — fixture {fid}")
                        continue
                    if total_teams < 6:
                        logger.debug(f"Gate: total_teams < 6 (monster) — fixture {fid}")
                        continue
                    if not fixture.get("is_priority"):
                        logger.debug(f"Gate: not priority league (monster) — fixture {fid}")
                        continue

                eligible.append(fixture)

            except Exception:
                logger.exception(f"filter_eligible_fixtures: error on fixture {fid}")
                continue

        logger.info(f"filter_eligible_fixtures(mode={mode}): {len(eligible)}/{len(fixtures)} passed")
        return eligible

    # ------------------------------------------------------------------
    # Helper calculators
    # ------------------------------------------------------------------

    def _calc_form_points(self, wins: int, draws: int) -> int:
        """Return wins×3 + draws×1."""
        return wins * 3 + draws * 1

    def _calc_h2h_win_rate(self, h2h: list[dict], team_name: str) -> float:
        """
        Calculate historical H2H win rate for team_name.

        Returns 0.5 if h2h is empty.
        """
        if not h2h:
            return 0.5
        wins = 0
        for match in h2h:
            home_g = match.get("home_goals")
            away_g = match.get("away_goals")
            if home_g is None or away_g is None:
                continue
            if match.get("home_name") == team_name and home_g > away_g:
                wins += 1
            elif match.get("away_name") == team_name and away_g > home_g:
                wins += 1
        return wins / len(h2h)

    def _calc_home_away_rate(self, standings: dict, side: str) -> float:
        """
        Calculate home win rate (side='home') or away win rate (side='away').

        Uses home_stat or away_stat sub-dict from the standings row.
        Returns 0.0 if no data.
        """
        try:
            stat_key = "home_stat" if side == "home" else "away_stat"
            stat = standings.get(side, {}).get(stat_key, {})
            if not stat:
                return 0.0
            played = stat.get("played", 0) or stat.get("matches_played", 0)
            wins = stat.get("win", 0) or stat.get("wins", 0)
            if played > 0:
                return wins / played
        except Exception:
            logger.debug(f"_calc_home_away_rate failed (side={side})")
        return 0.0

    # ------------------------------------------------------------------
    # BTTS guard
    # ------------------------------------------------------------------

    def _btts_guard(
        self,
        fixture: dict,
        adv: dict,
        h2h: list,
        btts_odds: float,
        sub_score: int,
    ) -> tuple[bool, str]:
        """
        Five-layer BTTS Yes selection guard.

        Returns (allowed, rejection_code).  rejection_code is one of:
          REJECTED_BTTS:H2H_FILTER
          REJECTED_BTTS:AWAY_DROUGHT
          REJECTED_BTTS:DOMINANCE
          REJECTED_BTTS:ODDS_SANITY
        All checks pass → returns (True, "").
        """
        home_name = fixture.get("home_team_name", "")
        away_name = fixture.get("away_team_name", "")

        # Pre-filter H2H to rows with valid scorelines (reused across checks)
        valid_h2h = [
            m for m in (h2h or [])
            if m.get("home_goals") is not None and m.get("away_goals") is not None
        ]
        n_h2h = len(valid_h2h)

        # ── Check 1: H2H Scoring Filter ────────────────────────────────
        # Away team must have scored in >= 40 % of recent H2H meetings.
        # Threshold rises to 50 % when odds > 2.30 (market already sceptical).
        if n_h2h >= 4:
            away_scored_in_h2h = 0
            for m in valid_h2h:
                if m.get("home_name") == away_name:
                    if (m.get("home_goals") or 0) >= 1:
                        away_scored_in_h2h += 1
                elif m.get("away_name") == away_name:
                    if (m.get("away_goals") or 0) >= 1:
                        away_scored_in_h2h += 1
            h2h_score_rate = away_scored_in_h2h / n_h2h
            threshold = 0.50 if btts_odds > 2.30 else 0.40
            if h2h_score_rate < threshold:
                logger.debug(
                    "REJECTED_BTTS:H2H_FILTER — %s scored in %.0f%% of H2H "
                    "(need %.0f%%, odds=%.2f) fixture=%s",
                    away_name, h2h_score_rate * 100, threshold * 100,
                    btts_odds, fixture.get("fixture_id"),
                )
                return False, "REJECTED_BTTS:H2H_FILTER"

        # ── Check 2: Away Goal Drought ──────────────────────────────────
        # Away team failed to score in 3+ of last 5 away games → reject.
        away_away_form = [
            m for m in adv.get("away_last_5_away_form", [])
            if isinstance(m, dict) and m.get("opponent") != "No data"
        ]
        if len(away_away_form) >= 4:
            blanks = sum(
                1 for m in away_away_form if (m.get("goals_scored") or 0) == 0
            )
            if blanks >= 3:
                # Equivalent to a -20 confidence penalty; reject if sub_score
                # can't survive it (sub 55 + ~10 s6 ≈ 65 total threshold).
                if (sub_score - 20) < 55:
                    logger.debug(
                        "REJECTED_BTTS:AWAY_DROUGHT — %s blanked in %d/%d away "
                        "games, penalized sub_score=%d fixture=%s",
                        away_name, blanks, len(away_away_form),
                        sub_score - 20, fixture.get("fixture_id"),
                    )
                    return False, "REJECTED_BTTS:AWAY_DROUGHT"

        # ── Check 3: Dominance Asymmetry ────────────────────────────────
        # Home team wins > 60 % AND away team wins < 15 % of H2H → dominant
        # fixture.  Then require BOTH: (a) away team scored in last home match,
        # (b) home team conceded in >= 2 of last 5 home games.
        if n_h2h >= 4:
            home_h2h_wins = 0
            away_h2h_wins = 0
            for m in valid_h2h:
                hg = m.get("home_goals") or 0
                ag = m.get("away_goals") or 0
                if m.get("home_name") == home_name and hg > ag:
                    home_h2h_wins += 1
                elif m.get("away_name") == home_name and ag > hg:
                    home_h2h_wins += 1
                if m.get("home_name") == away_name and hg > ag:
                    away_h2h_wins += 1
                elif m.get("away_name") == away_name and ag > hg:
                    away_h2h_wins += 1

            if (home_h2h_wins / n_h2h) > 0.60 and (away_h2h_wins / n_h2h) < 0.15:
                # a) Away team scored in their most recent home match
                away_form = adv.get("away_last_5_form", [])
                away_last_home = next(
                    (m for m in away_form
                     if isinstance(m, dict) and m.get("is_home") is True),
                    None,
                )
                away_can_score = (
                    away_last_home is not None
                    and (away_last_home.get("goals_scored") or 0) >= 1
                )
                # b) Home team conceded in >= 2 of last 5 home games
                home_home_form = adv.get("home_last_5_home_form", [])
                home_porous = sum(
                    1 for m in home_home_form
                    if isinstance(m, dict) and (m.get("goals_conceded") or 0) >= 1
                ) >= 2

                if not away_can_score or not home_porous:
                    logger.debug(
                        "REJECTED_BTTS:DOMINANCE — home H2H %.0f%%, away H2H %.0f%%, "
                        "away_can_score=%s, home_porous=%s fixture=%s",
                        (home_h2h_wins / n_h2h) * 100,
                        (away_h2h_wins / n_h2h) * 100,
                        away_can_score, home_porous,
                        fixture.get("fixture_id"),
                    )
                    return False, "REJECTED_BTTS:DOMINANCE"

        # ── Check 4: Odds Sanity ─────────────────────────────────────────
        # Odds > 2.80 means the market considers BTTS unlikely; only allow if
        # both teams show strong recent form (home wins >= 3, away wins >= 2
        # in last 5 overall).
        if btts_odds > 2.80:
            if not (
                adv.get("home_wins_last_5", 0) >= 3
                and adv.get("away_wins_last_5", 0) >= 2
            ):
                logger.debug(
                    "REJECTED_BTTS:ODDS_SANITY — odds=%.2f, home_wins=%d, "
                    "away_wins=%d fixture=%s",
                    btts_odds,
                    adv.get("home_wins_last_5", 0),
                    adv.get("away_wins_last_5", 0),
                    fixture.get("fixture_id"),
                )
                return False, "REJECTED_BTTS:ODDS_SANITY"

        return True, ""

    def _win_guard(
        self,
        fixture: dict,
        adv: dict,
        h2h: list,
        standings: dict,
        pick_side: str,    # "home" or "away"
        pick_odds: float,
        sub_score: int,
    ) -> tuple[bool, str]:
        """
        WIN (1X2) selection guard.

        Returns (allowed, rejection_code).  rejection_code is one of:
          REJECTED_WIN:FORM
          REJECTED_WIN:OPPOSITION
          REJECTED_WIN:H2H_BIAS
          REJECTED_WIN:ODDS_FLOOR
          REJECTED_WIN:TIER_PENALTY
        All checks pass → returns (True, "").
        """
        home_name   = fixture.get("home_team_name", "")
        away_name   = fixture.get("away_team_name", "")
        winner_name = home_name if pick_side == "home" else away_name
        fid         = fixture.get("fixture_id")

        # Venue-context form for the predicted winner and opponent.
        # winner_form = predicted winner's last 5 matches IN the relevant venue.
        # opp_form    = opponent's last 5 matches IN their travelling context.
        if pick_side == "home":
            winner_form = [
                m for m in adv.get("home_last_5_home_form", [])
                if isinstance(m, dict) and m.get("opponent") != "No data"
            ]
            opp_form = [
                m for m in adv.get("away_last_5_away_form", [])
                if isinstance(m, dict) and m.get("opponent") != "No data"
            ]
        else:
            winner_form = [
                m for m in adv.get("away_last_5_away_form", [])
                if isinstance(m, dict) and m.get("opponent") != "No data"
            ]
            opp_form = [
                m for m in adv.get("home_last_5_home_form", [])
                if isinstance(m, dict) and m.get("opponent") != "No data"
            ]

        # H2H pre-computation — shared by checks 3 and 4.
        valid_h2h = [
            m for m in (h2h or [])
            if m.get("home_goals") is not None and m.get("away_goals") is not None
        ]
        n_h2h           = len(valid_h2h)
        winner_h2h_wins = 0
        h2h_draw_count  = 0
        if n_h2h >= 4:
            for m in valid_h2h:
                hg = m.get("home_goals") or 0
                ag = m.get("away_goals") or 0
                if hg == ag:
                    h2h_draw_count += 1
                elif m.get("home_name") == winner_name and hg > ag:
                    winner_h2h_wins += 1
                elif m.get("away_name") == winner_name and ag > hg:
                    winner_h2h_wins += 1

        # ── Check 1: Form Consistency ──────────────────────────────────
        # Predicted winner must win >= 40% of recent venue-context matches.
        # Two consecutive losses in that context = immediate reject.
        if len(winner_form) >= 4:
            ctx_wins = sum(1 for m in winner_form if m.get("result") == "W")
            ctx_rate = ctx_wins / len(winner_form)
            if ctx_rate < 0.40 and (sub_score - 15) < 55:
                logger.debug(
                    "REJECTED_WIN:FORM — %s %s-context win rate %.0f%%, "
                    "penalized sub=%d fixture=%s",
                    winner_name, pick_side, ctx_rate * 100, sub_score - 15, fid,
                )
                return False, "REJECTED_WIN:FORM"
            if (
                len(winner_form) >= 2
                and [m.get("result") for m in winner_form[:2]] == ["L", "L"]
            ):
                logger.debug(
                    "REJECTED_WIN:FORM — %s lost last 2 consecutive context "
                    "matches fixture=%s", winner_name, fid,
                )
                return False, "REJECTED_WIN:FORM"

        # ── Check 2: Opposition Strength ──────────────────────────────
        # If opponent has kept 3+ clean sheets in their last 5 travelling
        # matches, they are defensively solid — require winner ctx rate >= 60%.
        if len(opp_form) >= 4:
            opp_clean_sheets = sum(
                1 for m in opp_form if (m.get("goals_conceded") or 0) == 0
            )
            if opp_clean_sheets >= 3 and len(winner_form) >= 4:
                ctx_wins = sum(1 for m in winner_form if m.get("result") == "W")
                if ctx_wins / len(winner_form) < 0.60:
                    logger.debug(
                        "REJECTED_WIN:OPPOSITION — opp %d/%d clean sheets, "
                        "winner ctx rate below 60%% fixture=%s",
                        opp_clean_sheets, len(opp_form), fid,
                    )
                    return False, "REJECTED_WIN:OPPOSITION"

        # ── Check 3: H2H Win Bias ─────────────────────────────────────
        if n_h2h >= 4:
            h2h_win_rate  = winner_h2h_wins / n_h2h
            h2h_draw_rate = h2h_draw_count  / n_h2h

            # Never back a team that lost their last 3 H2H meetings
            lost_last_3 = len(valid_h2h) >= 3 and all(
                (
                    m.get("home_name") == winner_name
                    and (m.get("home_goals") or 0) < (m.get("away_goals") or 0)
                ) or (
                    m.get("away_name") == winner_name
                    and (m.get("away_goals") or 0) < (m.get("home_goals") or 0)
                )
                for m in valid_h2h[:3]
            )
            if lost_last_3:
                logger.debug(
                    "REJECTED_WIN:H2H_BIAS — %s lost last 3 H2H fixture=%s",
                    winner_name, fid,
                )
                return False, "REJECTED_WIN:H2H_BIAS"

            # Draw-heavy fixtures → Win pick unreliable
            if h2h_draw_rate > 0.40:
                logger.debug(
                    "REJECTED_WIN:H2H_BIAS — H2H draw rate %.0f%% > 40%% "
                    "fixture=%s", h2h_draw_rate * 100, fid,
                )
                return False, "REJECTED_WIN:H2H_BIAS"

            # Low H2H win rate → confidence penalty
            if h2h_win_rate < 0.35 and (sub_score - 20) < 55:
                logger.debug(
                    "REJECTED_WIN:H2H_BIAS — %s H2H win rate %.0f%% < 35%%, "
                    "penalized sub=%d fixture=%s",
                    winner_name, h2h_win_rate * 100, sub_score - 20, fid,
                )
                return False, "REJECTED_WIN:H2H_BIAS"

        # ── Check 4: Odds Floor — heavy favourite strict conditions ────
        # Our gate already excludes odds < 1.25; tighten the 1.25–1.35 band.
        if 1.25 <= pick_odds <= 1.35:
            # (a) Context win rate >= 67%
            ctx_ok = True
            if len(winner_form) >= 4:
                ctx_wins = sum(1 for m in winner_form if m.get("result") == "W")
                ctx_ok   = (ctx_wins / len(winner_form)) >= 0.67
            # (b) H2H win rate >= 50% (skipped when data is sparse)
            h2h_ok = True
            if n_h2h >= 4:
                h2h_ok = (winner_h2h_wins / n_h2h) >= 0.50
            # (c) Opponent in bottom half of table
            opp_key      = "away" if pick_side == "home" else "home"
            opp_rank     = standings.get(opp_key, {}).get("rank", 0)
            total_teams  = standings.get("total_teams", 0)
            opp_bottom   = total_teams > 0 and opp_rank > 0 and opp_rank > (total_teams / 2)
            if not (ctx_ok and h2h_ok and opp_bottom):
                logger.debug(
                    "REJECTED_WIN:ODDS_FLOOR — odds=%.2f strict gate failed "
                    "(ctx_ok=%s h2h_ok=%s opp_bottom=%s) fixture=%s",
                    pick_odds, ctx_ok, h2h_ok, opp_bottom, fid,
                )
                return False, "REJECTED_WIN:ODDS_FLOOR"

        # ── Check 5: League Tier Penalty ──────────────────────────────
        # Leagues outside the top-20 priority carry noisier form data.
        if fixture.get("league_priority", 999) > 20 and (sub_score - 10) < 60:
            logger.debug(
                "REJECTED_WIN:TIER_PENALTY — priority=%d penalized sub=%d "
                "fixture=%s",
                fixture.get("league_priority", 999), sub_score - 10, fid,
            )
            return False, "REJECTED_WIN:TIER_PENALTY"

        return True, ""

    def _over25_guard(
        self,
        fixture: dict,
        adv: dict,
        h2h: list,
        odds_data: dict,
        over_odds: float,
        sub_score: int,
    ) -> tuple[bool, str]:
        """
        OVER 2.5 selection guard.

        Returns (allowed, rejection_code).  rejection_code is one of:
          REJECTED_OVER:SCORING_RATE
          REJECTED_OVER:DEFENCE
          REJECTED_OVER:H2H_GOALS
          REJECTED_OVER:MATCH_CONTEXT
          REJECTED_OVER:ODDS_RANGE
        All checks pass → returns (True, "").
        """
        fid = fixture.get("fixture_id")

        # Form lists — venue-specific where meaningful, overall for defence check
        home_home_form = [
            m for m in adv.get("home_last_5_home_form", [])
            if isinstance(m, dict) and m.get("opponent") != "No data"
        ]
        away_away_form = [
            m for m in adv.get("away_last_5_away_form", [])
            if isinstance(m, dict) and m.get("opponent") != "No data"
        ]
        home_overall = [
            m for m in adv.get("home_last_5_form", [])
            if isinstance(m, dict) and m.get("opponent") != "No data"
        ]
        away_overall = [
            m for m in adv.get("away_last_5_form", [])
            if isinstance(m, dict) and m.get("opponent") != "No data"
        ]

        # H2H pre-computation
        valid_h2h = [
            m for m in (h2h or [])
            if m.get("home_goals") is not None and m.get("away_goals") is not None
        ]
        n_h2h   = len(valid_h2h)
        h2h_avg = (
            sum(
                (m.get("home_goals") or 0) + (m.get("away_goals") or 0)
                for m in valid_h2h
            ) / n_h2h
            if n_h2h >= 4 else None
        )

        # ── Check 1: Combined Venue-Context Scoring Rate ───────────────
        # Count matches in each team's venue context that had 3+ total goals.
        # Reject if combined rate < 45%, penalize if either side < 33%.
        if len(home_home_form) >= 4 and len(away_away_form) >= 4:
            home_over = sum(
                1 for m in home_home_form
                if (m.get("goals_scored") or 0) + (m.get("goals_conceded") or 0) >= 3
            )
            away_over = sum(
                1 for m in away_away_form
                if (m.get("goals_scored") or 0) + (m.get("goals_conceded") or 0) >= 3
            )
            n_total       = len(home_home_form) + len(away_away_form)
            combined_rate = (home_over + away_over) / n_total
            if combined_rate < 0.45:
                logger.debug(
                    "REJECTED_OVER:SCORING_RATE — combined Over2.5 rate %.0f%% "
                    "< 45%% fixture=%s", combined_rate * 100, fid,
                )
                return False, "REJECTED_OVER:SCORING_RATE"
            if (
                home_over / len(home_home_form) < 0.33
                or away_over / len(away_away_form) < 0.33
            ) and (sub_score - 15) < 50:
                logger.debug(
                    "REJECTED_OVER:SCORING_RATE — per-team rate too low "
                    "(home %.0f%% away %.0f%%), penalized sub=%d fixture=%s",
                    (home_over / len(home_home_form)) * 100,
                    (away_over / len(away_away_form)) * 100,
                    sub_score - 15, fid,
                )
                return False, "REJECTED_OVER:SCORING_RATE"

        # ── Check 2: Defensive Leakiness ──────────────────────────────
        # At least one team must have conceded 2+ goals in 2+ of last 5 overall.
        # If both teams are tight (avg < 1.0/game), reject.
        if len(home_overall) >= 4 and len(away_overall) >= 4:
            home_leaky = (
                sum(1 for m in home_overall if (m.get("goals_conceded") or 0) >= 2) >= 2
            )
            away_leaky = (
                sum(1 for m in away_overall if (m.get("goals_conceded") or 0) >= 2) >= 2
            )
            if not home_leaky and not away_leaky:
                home_avg_c = adv.get("home_goals_conceded_last_5", 0) / 5
                away_avg_c = adv.get("away_goals_conceded_last_5", 0) / 5
                if home_avg_c < 1.0 and away_avg_c < 1.0:
                    logger.debug(
                        "REJECTED_OVER:DEFENCE — both teams tight "
                        "(home avg=%.1f away avg=%.1f conceded/game) fixture=%s",
                        home_avg_c, away_avg_c, fid,
                    )
                    return False, "REJECTED_OVER:DEFENCE"

        # ── Check 3: H2H Goals Average ─────────────────────────────────
        # H2H goal patterns are sticky — they override single-season form.
        if h2h_avg is not None:
            if h2h_avg < 1.8:
                logger.debug(
                    "REJECTED_OVER:H2H_GOALS — H2H avg %.2f < 1.8 fixture=%s",
                    h2h_avg, fid,
                )
                return False, "REJECTED_OVER:H2H_GOALS"
            if h2h_avg < 2.2 and (sub_score - 15) < 50:
                logger.debug(
                    "REJECTED_OVER:H2H_GOALS — H2H avg %.2f < 2.2, penalized "
                    "sub=%d fixture=%s", h2h_avg, sub_score - 15, fid,
                )
                return False, "REJECTED_OVER:H2H_GOALS"

        # ── Check 4: Match Context ─────────────────────────────────────
        # A heavy home favourite (odds < 1.40) may sit back once ahead,
        # suppressing total goals.  Apply a -10 penalty.
        home_win_odds = odds_data.get("match_winner", {}).get("home", 0)
        if home_win_odds and 1.0 < home_win_odds < 1.40 and (sub_score - 10) < 50:
            logger.debug(
                "REJECTED_OVER:MATCH_CONTEXT — heavy home favourite odds=%.2f, "
                "penalized sub=%d fixture=%s",
                home_win_odds, sub_score - 10, fid,
            )
            return False, "REJECTED_OVER:MATCH_CONTEXT"

        # ── Check 5: Odds Range ────────────────────────────────────────
        # At odds > 1.90 the market is sceptical; require H2H avg > 2.80
        # to override it.
        if over_odds > 1.90 and h2h_avg is not None and h2h_avg <= 2.80:
            logger.debug(
                "REJECTED_OVER:ODDS_RANGE — odds=%.2f > 1.90 but H2H avg=%.2f "
                "<= 2.80 fixture=%s", over_odds, h2h_avg, fid,
            )
            return False, "REJECTED_OVER:ODDS_RANGE"

        return True, ""

    # ------------------------------------------------------------------
    # Market selection
    # ------------------------------------------------------------------

    def get_qualifying_markets(
        self,
        fixture: dict,
        standings: dict,
        odds: dict,
        sub_score: int,
        avg_goals: float,
        predictions: dict | None = None,
        h2h: list | None = None,
    ) -> list[dict]:
        """
        Return ALL qualifying betting markets for a fixture.

        Claude will choose the best one from this list during validation.
        Each entry has: market, pick, odds, no_double_chance.
        """
        fid = fixture.get("fixture_id")
        odds_data = odds.get(fid, {}) if isinstance(odds, dict) else odds
        if not odds_data:
            return []

        pred = predictions.get(fid, {}) if predictions else {}
        adv = fixture.get("advanced_stats", {})
        markets: list[dict] = []

        try:
            home_pct = pred.get("home_win_pct", 0) or 0
            away_pct = pred.get("away_win_pct", 0) or 0

            # 1X2
            if sub_score >= 65:
                home_odds = odds_data.get("match_winner", {}).get("home", 0)
                away_odds = odds_data.get("match_winner", {}).get("away", 0)
                if home_pct >= away_pct and 1.25 <= home_odds <= 2.50:
                    win_ok, _ = self._win_guard(
                        fixture, adv, h2h or [], standings, "home", home_odds, sub_score
                    )
                    if win_ok:
                        markets.append({
                            "market": "1X2", "pick": "Home Win",
                            "odds": home_odds, "no_double_chance": False,
                        })
                elif away_pct > home_pct and 1.25 <= away_odds <= 2.50:
                    win_ok, _ = self._win_guard(
                        fixture, adv, h2h or [], standings, "away", away_odds, sub_score
                    )
                    if win_ok:
                        markets.append({
                            "market": "1X2", "pick": "Away Win",
                            "odds": away_odds, "no_double_chance": False,
                        })

            # BTTS Yes — quality-adjusted check using vs-similar-rank stats.
            # If both teams have >= 3 similar-rank matches we use those goals
            # figures (which filter out goals scored against much weaker/stronger
            # opposition).  Otherwise we fall back to overall last-5 stats.
            # Additionally skip if one team is a clear favourite (>55 % win
            # probability) — a dominant side is likely to keep a clean sheet.
            def _similar_goals(matches: list) -> tuple[int, int, int]:
                """Return (goals_scored, goals_conceded, n_real_matches)."""
                real = [
                    m for m in (matches or [])
                    if isinstance(m, dict) and m.get("opponent") != "No data"
                ]
                return (
                    sum(m.get("goals_scored", 0) for m in real),
                    sum(m.get("goals_conceded", 0) for m in real),
                    len(real),
                )

            h_sim_scored, h_sim_conceded, h_sim_n = _similar_goals(
                adv.get("home_last_5_vs_similar_rank", [])
            )
            a_sim_scored, a_sim_conceded, a_sim_n = _similar_goals(
                adv.get("away_last_5_vs_similar_rank", [])
            )
            use_similar = h_sim_n >= 3 and a_sim_n >= 3

            if use_similar:
                btts_home_attack  = h_sim_scored   >= 3
                btts_away_attack  = a_sim_scored   >= 2
                btts_home_defense = h_sim_conceded >= 2
                btts_away_defense = a_sim_conceded >= 2
            else:
                btts_home_attack  = adv.get("home_goals_scored_last_5",    0) >= 4
                btts_away_attack  = adv.get("away_goals_scored_last_5",    0) >= 3
                btts_home_defense = adv.get("home_goals_conceded_last_5",  0) >= 2
                btts_away_defense = adv.get("away_goals_conceded_last_5",  0) >= 2

            max_win_pct = max(home_pct, away_pct)

            # Rank gap guard — block BTTS when teams are too far apart in the
            # table.  A 1st vs 17th matchup is a class difference, not a
            # goal-fest: the stronger side almost always keeps a clean sheet.
            home_rank = standings.get("home", {}).get("rank", 0)
            away_rank = standings.get("away", {}).get("rank", 0)
            total_teams_in_table = standings.get("total_teams", 0)
            rank_gap_blocks_btts = False
            if home_rank > 0 and away_rank > 0:
                rank_gap = abs(home_rank - away_rank)
                # Threshold scales slightly with league size:
                #   >= 16 teams → gap of 8+ is a clear mismatch
                #   < 16 teams  → gap of 5+ is already significant
                max_gap = 8 if total_teams_in_table >= 16 else 5
                rank_gap_blocks_btts = rank_gap >= max_gap

            if (
                not rank_gap_blocks_btts
                and max_win_pct <= 52      # tightened from 55 — clear favourites rarely concede
                and btts_home_attack
                and btts_away_attack
                and btts_home_defense
                and btts_away_defense
            ):
                btts_odds = odds_data.get("btts", {}).get("yes", 0)
                if btts_odds and 1.30 <= btts_odds <= 3.50:
                    btts_ok, btts_rejection = self._btts_guard(
                        fixture, adv, h2h or [], btts_odds, sub_score
                    )
                    if btts_ok:
                        markets.append({
                            "market": "BTTS Yes", "pick": "Both Teams Score",
                            "odds": btts_odds, "no_double_chance": False,
                        })

            # Over 2.5
            if avg_goals >= 2.5:
                over_odds = odds_data.get("over_under", {}).get("over", 0)
                if over_odds and 1.30 <= over_odds <= 3.50:
                    over_ok, _ = self._over25_guard(
                        fixture, adv, h2h or [], odds_data, over_odds, sub_score
                    )
                    if over_ok:
                        markets.append({
                            "market": "Over 2.5", "pick": "Over 2.5 Goals",
                            "odds": over_odds, "no_double_chance": False,
                        })

            # Double Chance
            if sub_score >= 58:
                dc_odds = odds_data.get("double_chance", {})
                if home_pct >= away_pct:
                    dc = dc_odds.get("1X", 0)
                    dc_pick = "1X"
                else:
                    dc = dc_odds.get("X2", 0)
                    dc_pick = "X2"
                if dc and 1.20 <= dc <= 3.00:
                    markets.append({
                        "market": "Double Chance", "pick": dc_pick,
                        "odds": dc, "no_double_chance": True,
                    })

            # Asian Handicap fallback
            home_rate = self._calc_home_away_rate(standings, "home")
            if home_rate >= 0.70:
                home_odds = odds_data.get("match_winner", {}).get("home", 0)
                if home_odds and 1.30 <= home_odds <= 3.50:
                    markets.append({
                        "market": "Asian Handicap", "pick": "Home -0.5",
                        "odds": home_odds, "no_double_chance": False,
                    })

        except Exception:
            logger.exception(f"get_qualifying_markets failed for fixture {fid}")

        return markets

    # ------------------------------------------------------------------
    # Single-fixture scoring
    # ------------------------------------------------------------------

    def score_fixture(
        self,
        fixture: dict,
        standings: dict,
        h2h: list[dict],
        predictions: dict,
        odds: dict,
        mode: str = "small",
    ) -> dict | None:
        """
        Score a single fixture and select the best market.

        Returns the fixture dict enriched with confidence, selected market,
        signal breakdown, etc. — or None if the fixture doesn't qualify.
        """
        fid = fixture.get("fixture_id")
        try:
            pred = predictions.get(fid, {})
            adv = fixture.get("advanced_stats", {})
            standing = standings.get(fid, {})

            home_pct = pred.get("home_win_pct", 0) or 0
            away_pct = pred.get("away_win_pct", 0) or 0
            favored = "home" if home_pct >= away_pct else "away"
            low_data = False

            # Signal 1 — Prediction % (25 pts max)
            # Football win probabilities rarely exceed 65%; calibrate accordingly.
            if not pred:
                s1 = 6
                low_data = True
            else:
                best_pct = max(home_pct, away_pct)
                if best_pct >= 55:
                    s1 = 25
                elif best_pct >= 45:
                    s1 = 18
                elif best_pct >= 35:
                    s1 = 12
                else:
                    s1 = 6

            # Signal 2 — Form points (20 pts max)
            if favored == "home":
                form_pts = self._calc_form_points(
                    adv.get("home_wins_last_5", 0),
                    adv.get("home_draws_last_5", 0),
                )
            else:
                form_pts = self._calc_form_points(
                    adv.get("away_wins_last_5", 0),
                    adv.get("away_draws_last_5", 0),
                )
            if form_pts >= 13:
                s2 = 20
            elif form_pts >= 10:
                s2 = 14
            elif form_pts >= 7:
                s2 = 9
            else:
                s2 = 4

            # Signal 3 — H2H (15 pts max)
            if h2h:
                team_name = (
                    fixture["home_team_name"]
                    if favored == "home"
                    else fixture["away_team_name"]
                )
                rate = self._calc_h2h_win_rate(h2h, team_name)
                if rate >= 0.67:
                    s3 = 15
                elif rate >= 0.50:
                    s3 = 10
                elif rate >= 0.33:
                    s3 = 6
                else:
                    s3 = 3
            else:
                s3 = 6

            # Signal 4 — Home/Away record (15 pts max)
            ha_rate = self._calc_home_away_rate(standing, favored)
            if ha_rate >= 0.65:
                s4 = 15
            elif ha_rate >= 0.50:
                s4 = 10
            elif ha_rate >= 0.35:
                s4 = 6
            else:
                s4 = 3

            # Signal 5 — Goals trend (15 pts max)
            avg_goals = (
                adv.get("home_goals_scored_last_5", 0)
                + adv.get("away_goals_scored_last_5", 0)
            ) / 5
            if avg_goals >= 3.0:
                s5 = 15
            elif avg_goals >= 2.5:
                s5 = 11
            elif avg_goals >= 2.0:
                s5 = 7
            else:
                s5 = 4

            sub_score = s1 + s2 + s3 + s4 + s5

            all_markets = self.get_qualifying_markets(
                fixture, standing, odds, sub_score, avg_goals, predictions, h2h
            )
            if not all_markets:
                logger.debug(
                    "score_fixture: no markets qualified for fixture %s", fid
                )
                return None
            # Use the first market for scoring; Claude will pick the best one
            market_result = all_markets[0]

            # Signal 6 — Odds value (10 pts max)
            selected_odds = market_result["odds"]
            implied_prob = (1 / selected_odds) * 100 if selected_odds else 0
            gap = sub_score - implied_prob
            if gap >= 15:
                s6 = 10
            elif gap >= 5:
                s6 = 7
            elif gap >= 0:
                s6 = 4
            else:
                s6 = 1

            total = sub_score + s6

            # Mode thresholds
            thresholds = {"small": 60, "monster": 58}
            threshold = thresholds.get(mode, 65)
            if total < threshold:
                logger.debug(
                    "Fixture %s scored %d below threshold %d",
                    fid, total, threshold,
                )
                return None

            return {
                **fixture,
                "confidence": total,
                "selected_market": market_result["market"],
                "selected_pick": market_result["pick"],
                "selected_odds": selected_odds,
                "no_double_chance": market_result.get("no_double_chance", False),
                "market_options": all_markets,
                "is_whitelisted": fixture.get("is_priority", False),
                "favored_team": favored,
                "low_data": low_data,
                # h2h merged so validator can include it in the Claude prompt
                "h2h": h2h,
                # prediction percentages merged so validator can read them
                "home_win_pct": home_pct,
                "draw_pct": pred.get("draw_pct", 0) or 0,
                "away_win_pct": away_pct,
                "advice": pred.get("advice", ""),
                # table positions for the validator rank-gap check
                "home_rank": standing.get("home", {}).get("rank", 0),
                "away_rank": standing.get("away", {}).get("rank", 0),
                "total_teams": standing.get("total_teams", 0),
                "signal_breakdown": {
                    "s1": s1,
                    "s2": s2,
                    "s3": s3,
                    "s4": s4,
                    "s5": s5,
                    "s6": s6,
                    "sub_score": sub_score,
                    "total": total,
                    "avg_goals": round(avg_goals, 2),
                },
            }

        except Exception:
            logger.exception(f"score_fixture failed for fixture {fid}")
            return None

    # ------------------------------------------------------------------
    # Batch scoring
    # ------------------------------------------------------------------

    def score_all(
        self,
        fixtures: list[dict],
        standings: dict,
        h2h_data: dict,
        predictions: dict,
        odds: dict,
        mode: str = "small",
    ) -> list[dict]:
        """
        Score every fixture in the list and return sorted results.

        Returns fixtures with confidence scores, sorted descending.
        """
        scored: list[dict] = []
        for fixture in fixtures:
            fid = fixture.get("fixture_id")
            h2h = h2h_data.get(fid, [])
            result = self.score_fixture(
                fixture, standings, h2h, predictions, odds, mode
            )
            if result:
                scored.append(result)

        scored.sort(key=lambda f: f["confidence"], reverse=True)
        logger.info(f"score_all(mode={mode}): {len(scored)} scored")
        return scored

    # ------------------------------------------------------------------
    # Accumulator building
    # ------------------------------------------------------------------

    def build_accas(self, scored: list[dict], mode: str = "small") -> dict:
        """
        Build accumulator bets from scored fixtures.

        mode='small':         up to 10 daily 3-5 leg accas targeting 4.50–5.50 odds.
        mode='daily_monster': daily 100x / 500x / 1000x high-odds accas.
        mode='monster':       long-term 10k and 100k accas.
        """
        if mode == "small":
            return self._build_small_accas(scored)
        if mode == "daily_monster":
            return self._build_daily_monster_accas(scored)
        return self._build_monster_accas(scored)

    # --- Small accas ---

    def _build_small_accas(self, scored: list[dict]) -> dict:
        """Build daily small accumulator bets."""
        try:
            if len(scored) < settings.PREDICTION_MIN_FIXTURES:
                logger.info(
                    "build_accas(small): only %d fixtures, need %d",
                    len(scored), settings.PREDICTION_MIN_FIXTURES,
                )
                return {
                    "daily_accas": [],
                    "best_acca": None,
                    "insufficient_fixtures": True,
                }

            # Pre-filter: small accas require confidence >= 60 and no low_data
            eligible = [
                f for f in scored
                if f.get("confidence", 0) >= 60
                and not f.get("low_data", False)
            ]

            daily_accas: list[dict] = []
            used_ids: set[int] = set()
            discard_counts: dict[str, int] = {
                "too_few_legs": 0,
                "odds_out_of_range": 0,
                "avg_conf_too_low": 0,
            }

            for _ in range(10):
                candidates = [
                    f for f in eligible
                    if f["fixture_id"] not in used_ids
                ]
                if not candidates:
                    break

                candidates_sorted = sorted(
                    candidates, key=lambda f: f["confidence"], reverse=True
                )
                legs: list[dict] = []
                used_leagues: set[int] = set()
                total_odds = 1.0

                for candidate in candidates_sorted:
                    # Hard confidence gate — no exceptions
                    if candidate["confidence"] < 60:
                        continue
                    if candidate["league_id"] in used_leagues:
                        continue
                    if candidate["selected_odds"] > 3.50:
                        continue
                    if candidate["selected_odds"] < 1.30:
                        continue
                    projected = total_odds * candidate["selected_odds"]
                    if projected > 5.60:
                        continue  # do NOT add if it overshoots
                    legs.append(candidate)
                    used_leagues.add(candidate["league_id"])
                    total_odds *= candidate["selected_odds"]
                    if len(legs) >= 5:
                        break

                if len(legs) < 3:
                    discard_counts["too_few_legs"] += 1
                    continue

                # If below minimum, try adding one more leg to push into range
                if total_odds < 4.50:
                    leg_ids = {l["fixture_id"] for l in legs}
                    leg_leagues = {l["league_id"] for l in legs}
                    remaining = [
                        c for c in candidates_sorted
                        if c["fixture_id"] not in leg_ids
                        and c["league_id"] not in leg_leagues
                        and c["confidence"] >= 60
                        and 1.30 <= c["selected_odds"] <= 3.50
                    ]
                    added = False
                    for extra in remaining:
                        projected = total_odds * extra["selected_odds"]
                        if 4.50 <= projected <= 5.50:
                            legs.append(extra)
                            used_leagues.add(extra["league_id"])
                            total_odds = projected
                            added = True
                            break
                    if not added:
                        continue

                # Hard odds gate — no exceptions
                if not 4.50 <= round(total_odds, 2) <= 5.50:
                    discard_counts["odds_out_of_range"] += 1
                    logger.debug(
                        "Acca discarded — odds %.2f out of range", total_odds
                    )
                    continue

                avg_conf = sum(l["confidence"] for l in legs) / len(legs)
                if avg_conf < 63:
                    discard_counts["avg_conf_too_low"] += 1
                    logger.debug(
                        "build_accas(small): discarding acca — avg_conf %.1f < 63",
                        avg_conf,
                    )
                    continue

                daily_accas.append({
                    "legs": legs,
                    "total_odds": round(total_odds, 2),
                    "avg_confidence": round(avg_conf, 1),
                    "n_legs": len(legs),
                })
                for leg in legs:
                    used_ids.add(leg["fixture_id"])

            if any(discard_counts.values()):
                logger.info(
                    "build_accas(small) discards — %s",
                    " | ".join(f"{k}={v}" for k, v in discard_counts.items() if v),
                )

            # Best acca is always different from ACCA #1
            best_acca = None
            if daily_accas:
                if len(daily_accas) > 1:
                    best_acca = max(
                        daily_accas[1:],
                        key=lambda a: (
                            a["avg_confidence"],
                            -abs(a["total_odds"] - 5.00),
                        ),
                    )
                else:
                    best_acca = daily_accas[0]

            if not daily_accas:
                pass  # fall through to return empty result below

            return {
                "daily_accas": daily_accas,
                "best_acca": best_acca,
                "insufficient_fixtures": False,
            }

        except Exception:
            logger.exception("_build_small_accas failed")
            return {
                "daily_accas": [],
                "best_acca": None,
                "insufficient_fixtures": True,
            }

    # --- Daily monster accas ---

    def _build_daily_monster_accas(self, scored: list[dict]) -> dict:
        """
        Build daily high-odds accumulators targeting 100x, 500x and 1000x.

        Uses today's scored fixtures (same pool as small mode).  Each target
        is built independently; whichever ones can be assembled from the
        available fixtures are returned — the others come back as None.
        """
        try:
            def build_target(
                candidates: list[dict],
                target_min: float,
                target_max: float,
                max_legs: int,
                min_leagues: int,
                min_confidence: int,
            ) -> dict | None:
                """Build one daily-monster acca to the given target-odds range."""
                try:
                    eligible = [
                        f for f in candidates
                        if f.get("confidence", 0) >= min_confidence
                        and f.get("selected_market") != "Double Chance"
                        and 1.50 <= f.get("selected_odds", 0) <= 4.00
                    ]
                    # Prefer Over 2.5 / BTTS first, then confidence DESC
                    eligible.sort(
                        key=lambda f: (
                            0 if f.get("selected_market") in ("Over 2.5", "BTTS Yes")
                            else 1,
                            -f["confidence"],
                        )
                    )

                    legs: list[dict] = []
                    total_odds = 1.0
                    league_counts: dict[int, int] = {}

                    for candidate in eligible:
                        if total_odds >= target_max:
                            break
                        if len(legs) >= max_legs:
                            break
                        lid = candidate["league_id"]
                        # Max 2 fixtures from the same league (daily pool is smaller)
                        if league_counts.get(lid, 0) >= 2:
                            continue
                        projected = total_odds * candidate["selected_odds"]
                        # Allow 10 % overshoot buffer on target_max
                        if projected > target_max * 1.10:
                            continue
                        legs.append(candidate)
                        total_odds *= candidate["selected_odds"]
                        league_counts[lid] = league_counts.get(lid, 0) + 1

                    if not legs:
                        return None
                    if total_odds < target_min:
                        return None
                    n_leagues = len({l["league_id"] for l in legs})
                    if n_leagues < min_leagues:
                        return None

                    return {
                        "legs": legs,
                        "total_odds": round(total_odds, 2),
                        "avg_confidence": round(
                            sum(l["confidence"] for l in legs) / len(legs), 1
                        ),
                        "n_legs": len(legs),
                        "n_leagues": n_leagues,
                    }
                except Exception:
                    logger.exception("build_target (daily_monster) inner failed")
                    return None

            # Shared pool — all three targets draw from the same daily fixtures;
            # their very different target-odds ranges produce distinct selections.
            pool = sorted(
                [
                    f for f in scored
                    if f.get("selected_market") != "Double Chance"
                ],
                key=lambda f: (
                    0 if f.get("selected_market") in ("Over 2.5", "BTTS Yes") else 1,
                    -f["confidence"],
                ),
            )

            # Thresholds match the engine scoring gates:
            #   100x  → confidence >= 65  (small-mode gate)
            #   500x  → confidence >= 62  (10k monster gate)
            #   1000x → confidence >= 60  (slightly relaxed for more legs)
            acca_100 = build_target(pool,   80,   120, 12, 3, 60)
            acca_500 = build_target(pool,  400,   600, 16, 4, 58)
            acca_1k  = build_target(pool,  800,  1200, 18, 5, 56)

            available = [a for a in [acca_100, acca_500, acca_1k] if a]
            logger.info(
                "build_accas(daily_monster): built %d/3 targets"
                " (100x=%s 500x=%s 1k=%s) from %d fixtures",
                len(available),
                "yes" if acca_100 else "no",
                "yes" if acca_500 else "no",
                "yes" if acca_1k  else "no",
                len(scored),
            )

            return {
                "acca_100": acca_100,
                "acca_500": acca_500,
                "acca_1k": acca_1k,
                "insufficient_daily_monster_fixtures": not available,
            }

        except Exception:
            logger.exception("_build_daily_monster_accas failed")
            return {
                "acca_100": None,
                "acca_500": None,
                "acca_1k": None,
                "insufficient_daily_monster_fixtures": True,
            }

    # --- Monster accas ---

    def _build_monster_accas(self, scored: list[dict]) -> dict:
        """Build long-range 10k and 100k monster accumulator bets."""
        try:
            if len(scored) < settings.PREDICTION_MIN_MONSTER_FIXTURES:
                logger.info(
                    "build_accas(monster): only %d fixtures, need %d",
                    len(scored), settings.PREDICTION_MIN_MONSTER_FIXTURES,
                )
                return {
                    "acca_10k": None,
                    "acca_100k": None,
                    "insufficient_monster_fixtures": True,
                }

            def build_monster(
                candidates: list[dict],
                target_min: float,
                target_max: float,
                max_legs: int,
                min_leagues: int,
                min_confidence: int,
                label: str = "",
            ) -> dict | None:
                """Inner helper that builds a single monster acca from a given pool."""
                tag = f"build_monster({label})"
                try:
                    # Odds floor is 1.30 (not 1.50): Claude regularly picks strong
                    # favourites at 1.30–1.49 which are still valid compounding legs.
                    eligible = [
                        f for f in candidates
                        if f.get("confidence", 0) >= min_confidence
                        and f.get("selected_market") != "Double Chance"
                        and 1.30 <= f.get("selected_odds", 0) <= 4.00
                    ]
                    # Prefer Over 2.5 and BTTS first, then confidence DESC
                    eligible.sort(
                        key=lambda f: (
                            0 if f.get("selected_market") in ("Over 2.5", "BTTS Yes")
                            else 1,
                            -f["confidence"],
                        )
                    )

                    logger.info(
                        "%s: pool=%d → eligible(conf>=%d, odds 1.30-4.00, no DC)=%d",
                        tag, len(candidates), min_confidence, len(eligible),
                    )

                    legs: list[dict] = []
                    total_odds = 1.0
                    league_counts: dict[int, int] = {}

                    for candidate in eligible:
                        if total_odds >= target_max:
                            break
                        if len(legs) >= max_legs:
                            break
                        lid = candidate["league_id"]
                        if league_counts.get(lid, 0) >= 3:
                            continue
                        projected = total_odds * candidate["selected_odds"]
                        # Allow 10% buffer overshoot on target_max
                        if projected > target_max * 1.10:
                            continue
                        legs.append(candidate)
                        total_odds *= candidate["selected_odds"]
                        league_counts[lid] = league_counts.get(lid, 0) + 1

                    if not legs:
                        logger.info("%s: 0 legs added — all candidates excluded by filters", tag)
                        return None

                    n_leagues = len({l["league_id"] for l in legs})

                    if total_odds < target_min:
                        avg_per_leg = total_odds ** (1 / len(legs))
                        logger.info(
                            "%s: FAIL total_odds=%.0f < target_min=%.0f "
                            "(legs=%d, leagues=%d, avg_odds_per_leg=%.2f)",
                            tag, total_odds, target_min, len(legs), n_leagues, avg_per_leg,
                        )
                        return None
                    if n_leagues < min_leagues:
                        logger.info(
                            "%s: FAIL n_leagues=%d < min_leagues=%d "
                            "(total_odds=%.0f, legs=%d)",
                            tag, n_leagues, min_leagues, total_odds, len(legs),
                        )
                        return None

                    # Defensive date handling — date may be a datetime or a string
                    raw_dates = [leg.get("date") for leg in legs if leg.get("date")]
                    try:
                        sorted_dates = sorted(raw_dates)
                        d0, d1 = sorted_dates[0], sorted_dates[-1]
                        start_date = d0.strftime("%a %d %b") if hasattr(d0, "strftime") else str(d0)[:10]
                        end_date   = d1.strftime("%a %d %b") if hasattr(d1, "strftime") else str(d1)[:10]
                    except Exception:
                        start_date = end_date = ""

                    logger.info(
                        "%s: OK — legs=%d, n_leagues=%d, total_odds=%.0f",
                        tag, len(legs), n_leagues, total_odds,
                    )
                    return {
                        "legs": legs,
                        "total_odds": round(total_odds, 2),
                        "avg_confidence": round(
                            sum(l["confidence"] for l in legs) / len(legs), 1
                        ),
                        "n_legs": len(legs),
                        "n_leagues": n_leagues,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                except Exception:
                    logger.exception("%s inner failed", tag)
                    return None

            # Both accas draw from the same scored pool — they are separate bets
            # with very different target-odds ranges so their leg selections will
            # naturally diverge.  Enforcing strict pool independence was the main
            # reason 100k could never be built (10k consumed too many fixtures).
            # Sort key: Over2.5/BTTS first (carry higher odds ~1.70-1.90),
            # then odds DESC within the same tier to maximise the compounding
            # product, then confidence DESC as tiebreaker.
            def _monster_sort_key(f):
                market_tier = 0 if f.get("selected_market") in ("Over 2.5", "BTTS Yes") else 1
                return (market_tier, -f.get("selected_odds", 0), -f["confidence"])

            # 10k: original conf≥62 pool, extended to 30 legs max.
            # Legs sorted by odds DESC within each market tier so higher-value
            # picks compound first and maximise the total product.
            pool_10k = sorted(
                [
                    f for f in scored
                    if f.get("confidence", 0) >= 62
                    and f.get("selected_market") != "Double Chance"
                ],
                key=_monster_sort_key,
            )

            acca_10k = build_monster(pool_10k, 8000, 12000, 30, 5, 62, label="10k")

            # 100k: original conf≥58 pool, extended to 40 legs max.
            pool_100k = sorted(
                [
                    f for f in scored
                    if f.get("confidence", 0) >= 58
                    and f.get("selected_market") != "Double Chance"
                ],
                key=_monster_sort_key,
            )

            acca_100k = build_monster(pool_100k, 80000, 120000, 40, 8, 58, label="100k")

            return {
                "acca_10k": acca_10k,
                "acca_100k": acca_100k,
                "insufficient_monster_fixtures": False,
            }

        except Exception:
            logger.exception("_build_monster_accas failed")
            return {
                "acca_10k": None,
                "acca_100k": None,
                "insufficient_monster_fixtures": True,
            }
