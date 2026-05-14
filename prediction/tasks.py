"""
tasks.py — Celery tasks for the prediction pipeline.

Uses shared_task only. No new Celery app instance.
All pipeline state is cached in Redis. Telegram messages are split at 4000 chars.
"""

import difflib
import re
import json
import logging
import time

import redis
import requests
from celery import shared_task
from django.conf import settings

from prediction.api_caller import APICaller
from prediction.db_reader import DBReader
from prediction.engine import PredictionEngine
from prediction.formatter import Formatter
from prediction.result_tracker import ResultTracker
from prediction.validator import ClaudeValidator
from prediction.win_engine import WinEngine

logger = logging.getLogger(__name__)

REDIS_CLIENT = redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)

CACHE_KEYS = {
    "small":          "prediction:cache:small:accas",
    "daily_monster":  "prediction:cache:daily:monster",
    "10k":            "prediction:cache:monster:10k",
    "100k":           "prediction:cache:monster:100k",
    "wins":           "prediction:cache:wins",
}


def send_telegram(message: str) -> None:
    """Send a plain-text message to Telegram. Splits if over 4000 chars."""
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        logger.warning("send_telegram: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    formatter = Formatter()
    chunks = formatter.split_message(message)

    for i, chunk in enumerate(chunks):
        try:
            requests.post(
                url,
                json={"chat_id": chat_id, "text": chunk},
                timeout=10,
            )
            if len(chunks) > 1 and i < len(chunks) - 1:
                time.sleep(0.5)
        except Exception as exc:
            logger.error(f"Telegram send failed: {exc}")


@shared_task(bind=True, max_retries=2)
def run_predict_pipeline(self, output_type: str = "all") -> None:
    """
    Main prediction pipeline task.

    output_type: 'all' | 'best' | 'big'
    Checks Redis cache before running each sub-pipeline.
    """
    db = DBReader()
    api = APICaller()
    engine = PredictionEngine()
    validator = ClaudeValidator()
    formatter = Formatter()
    tracker = ResultTracker()

    try:
        # ----------------------------------------------------------------
        # Small / daily accas
        # ----------------------------------------------------------------
        if output_type in ("all", "best"):

            cached_small = REDIS_CLIENT.get(CACHE_KEYS["small"])
            cached_dm    = REDIS_CLIENT.get(CACHE_KEYS["daily_monster"])

            if cached_small and cached_dm:
                logger.info("Serving small + daily monster accas from cache")
                small_result         = json.loads(cached_small)
                daily_monster_result = json.loads(cached_dm)
            else:
                logger.info("Running small + daily monster pipeline")
                start = time.time()

                fixtures = db.get_todays_fixtures("small")
                logger.info(f"Fixtures fetched: {len(fixtures)}")

                standings = db.get_batch_standings(fixtures)
                h2h_data = db.get_batch_h2h(fixtures)

                fixture_ids = [f["fixture_id"] for f in fixtures]
                predictions = api.get_batch_predictions(fixture_ids)
                odds = api.get_batch_odds(fixture_ids)

                eligible = engine.filter_eligible_fixtures(
                    fixtures, standings, predictions, odds, "small"
                )
                logger.info(f"Eligible fixtures: {len(eligible)}")

                scored = engine.score_all(
                    eligible, standings, h2h_data, predictions, odds, "small"
                )
                logger.info(f"Scored: {len(scored)}")

                validated = validator.validate_batch(scored)
                logger.info(f"Validated: {len(validated)}")

                small_result         = engine.build_accas(validated, "small")
                daily_monster_result = engine.build_accas(validated, "daily_monster")

                REDIS_CLIENT.setex(
                    CACHE_KEYS["small"],
                    settings.PREDICTION_CACHE_TTL_SMALL,
                    json.dumps(small_result, default=str),
                )
                REDIS_CLIENT.setex(
                    CACHE_KEYS["daily_monster"],
                    settings.PREDICTION_CACHE_TTL_SMALL,
                    json.dumps(daily_monster_result, default=str),
                )

                tracker.save_accas(small_result)
                tracker.save_accas(daily_monster_result)
                logger.info(
                    "Small + daily monster pipeline done in %.1fs", time.time() - start
                )

            # Send to Telegram
            send_telegram(formatter.format_header())

            if small_result.get("insufficient_fixtures"):
                send_telegram(formatter.format_insufficient("daily accas"))
            else:
                if output_type == "all":
                    for i, acca in enumerate(
                        small_result.get("daily_accas", []), 1
                    ):
                        send_telegram(formatter.format_small_acca(acca, i))

                if small_result.get("best_acca"):
                    send_telegram(
                        formatter.format_best_acca(small_result["best_acca"])
                    )

            # Daily monster — send whichever targets were built
            if daily_monster_result.get("insufficient_daily_monster_fixtures"):
                send_telegram(formatter.format_insufficient("daily monster accas"))
            else:
                for product, key in [("100", "acca_100"), ("500", "acca_500"), ("1k", "acca_1k")]:
                    acca = daily_monster_result.get(key)
                    if acca:
                        send_telegram(formatter.format_daily_monster_acca(acca, product))

        # ----------------------------------------------------------------
        # Monster accas
        # ----------------------------------------------------------------
        if output_type in ("big",):

            cached_10k = REDIS_CLIENT.get(CACHE_KEYS["10k"])
            cached_100k = REDIS_CLIENT.get(CACHE_KEYS["100k"])

            if cached_10k and cached_100k:
                logger.info("Serving monster accas from cache")
                acca_10k = json.loads(cached_10k)
                acca_100k = json.loads(cached_100k)
                monster_result = {
                    "acca_10k": acca_10k,
                    "acca_100k": acca_100k,
                    "insufficient_monster_fixtures": False,
                }
            else:
                logger.info("Running monster pipeline")
                start = time.time()

                m_fixtures = db.get_todays_fixtures("monster")
                logger.info(f"Monster fixtures: {len(m_fixtures)}")

                m_standings = db.get_batch_standings(m_fixtures)
                m_h2h = db.get_batch_h2h(m_fixtures)

                m_ids = [f["fixture_id"] for f in m_fixtures]
                m_preds = api.get_batch_predictions(m_ids)
                m_odds = api.get_batch_odds(m_ids)

                m_eligible = engine.filter_eligible_fixtures(
                    m_fixtures, m_standings, m_preds, m_odds, "monster"
                )

                m_scored = engine.score_all(
                    m_eligible, m_standings, m_h2h, m_preds, m_odds, "monster"
                )

                m_validated = validator.validate_batch(m_scored)

                monster_result = engine.build_accas(m_validated, "monster")

                if monster_result.get("acca_10k"):
                    REDIS_CLIENT.setex(
                        CACHE_KEYS["10k"],
                        settings.PREDICTION_CACHE_TTL_MONSTER,
                        json.dumps(monster_result["acca_10k"], default=str),
                    )
                if monster_result.get("acca_100k"):
                    REDIS_CLIENT.setex(
                        CACHE_KEYS["100k"],
                        settings.PREDICTION_CACHE_TTL_MONSTER,
                        json.dumps(monster_result["acca_100k"], default=str),
                    )

                tracker.save_accas(monster_result)
                logger.info(
                    "Monster pipeline done in %.1fs", time.time() - start
                )

            # Send header only if we didn't already send it above
            if output_type == "big":
                send_telegram(formatter.format_header())

            if monster_result.get("insufficient_monster_fixtures"):
                send_telegram(formatter.format_insufficient("monster accas"))
            else:
                acca_10k  = monster_result.get("acca_10k")
                acca_100k = monster_result.get("acca_100k")

                if not acca_10k and not acca_100k:
                    # Fixtures were available but combined odds fell below target.
                    # This happens on low-action matchdays dominated by heavy favourites.
                    send_telegram(
                        "⚠️ Monster accas could not be built today.\n"
                        "Odds were too short to reach target thresholds.\n"
                        "Try again tomorrow when more value is available."
                    )
                else:
                    if acca_10k:
                        send_telegram(
                            formatter.format_monster_acca(acca_10k, "10k")
                        )
                    if acca_100k:
                        send_telegram(
                            formatter.format_monster_acca(acca_100k, "100k")
                        )

    except Exception as exc:
        logger.critical(f"run_predict_pipeline unhandled error: {exc}", exc_info=True)
        send_telegram("❌ Pipeline error. Check logs.")
        raise self.retry(exc=exc, countdown=60)



@shared_task
def check_and_update_results() -> None:
    """Check fixture results and settle any pending accas.

    Uses APICaller.get_fixture_result() — local DB does not store final scores.
    """
    api = APICaller()
    tracker = ResultTracker()
    formatter = Formatter()

    try:
        settled = tracker.check_results(api)
        if settled:
            msg = formatter.format_result_update(settled)
            send_telegram(msg)
        else:
            logger.info("No settled accas yet")
    except Exception:
        logger.exception("check_and_update_results failed")


@shared_task(bind=True, max_retries=2)
def run_win_pipeline(self) -> None:
    """
    Heavy-favourite win pipeline.

    Sends 3 messages to Telegram:
      1. Header + up to 50 individual win picks
      2. 100x and 1K accumulators
      3. 100K accumulator (when buildable)
    """
    db = DBReader()
    api = APICaller()
    formatter = Formatter()

    try:
        win_engine = WinEngine()

        cached = REDIS_CLIENT.get(CACHE_KEYS["wins"])
        if cached:
            logger.info("win pipeline: serving from cache")
            result = json.loads(cached)
        else:
            start = time.time()
            fixtures = db.get_todays_fixtures("win")
            logger.info("win pipeline: %d fixtures", len(fixtures))

            standings   = db.get_batch_standings(fixtures)
            h2h_data    = db.get_batch_h2h(fixtures)
            fixture_ids = [f["fixture_id"] for f in fixtures]
            predictions = api.get_batch_predictions(fixture_ids)
            odds        = api.get_batch_odds(fixture_ids)

            scored = win_engine.score_all_wins(
                fixtures, standings, h2h_data, predictions, odds
            )
            accas = win_engine.build_win_accas(scored)
            result = {"picks": scored, "accas": accas}

            REDIS_CLIENT.setex(
                CACHE_KEYS["wins"],
                settings.PREDICTION_CACHE_TTL_MONSTER,
                json.dumps(result, default=str),
            )
            logger.info("win pipeline: done in %.1fs", time.time() - start)

        send_telegram(formatter.format_header())
        send_telegram(formatter.format_win_picks(result["picks"]))
        send_telegram(formatter.format_win_accas(result["accas"]))

    except Exception as exc:
        logger.critical("run_win_pipeline unhandled error: %s", exc, exc_info=True)
        send_telegram("❌ Win pipeline error. Check logs.")
        raise self.retry(exc=exc, countdown=60)


_REJECTION_REASONS = {
    'REJECTED_WIN:FORM':         'Poor venue form — win rate below threshold in last 5',
    'REJECTED_WIN:OPPOSITION':   'Opposition has kept too many clean sheets away',
    'REJECTED_WIN:H2H_BIAS':     'H2H record does not support this pick',
    'REJECTED_WIN:ODDS_FLOOR':   'Short-odds pick fails strict form + H2H + rank conditions',
    'REJECTED_WIN:TIER_PENALTY': 'Low-priority league — form data too noisy',
    'REJECTED_BTTS:H2H_FILTER':  'Away team rarely scores in H2H meetings',
    'REJECTED_BTTS:AWAY_DROUGHT':'Away team blanked in 3+ of last 5 away games',
    'REJECTED_BTTS:DOMINANCE':   'Dominance asymmetry — home side likely to keep a clean sheet',
    'REJECTED_BTTS:ODDS_SANITY': "Odds too high given both teams' scoring stats",
    'REJECTED_OVER:SCORING_RATE':'Combined over-2.5 rate too low in venue context',
    'REJECTED_OVER:DEFENCE':     'Both teams too tight defensively',
    'REJECTED_OVER:H2H_GOALS':   'H2H average goals too low for over 2.5',
    'REJECTED_OVER:MATCH_CONTEXT':'Heavy favourite likely to control and close game down',
    'REJECTED_OVER:ODDS_RANGE':  'H2H average does not justify these odds',
}

_ACCEPT_THRESHOLD    = 65
_DOWNGRADE_THRESHOLD = 55


def _reply(chat_id: int, text: str) -> None:
    token = settings.TELEGRAM_BOT_TOKEN
    if not token:
        return
    try:
        requests.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'},
            timeout=10,
        )
    except Exception as exc:
        logger.error("_reply: failed for chat %s — %s", chat_id, exc)


_BET_FORMAT_HELP = (
    "📋 Send your bets as text, one per line:\n\n"
    "<code>Arsenal vs Chelsea home 1.85</code>\n"
    "<code>Napoli vs Bologna away 2.10</code>\n"
    "<code>Benfica vs Braga draw 3.20</code>\n"
    "<code>Man City vs Liverpool over 2.5 1.95</code>\n"
    "<code>PSG vs Lyon btts 1.70</code>\n\n"
    "Format: <b>Home vs Away selection [line] odds</b>\n"
    "Markets: home | draw | away | over | under | btts"
)

_KEYWORDS = ['home', 'draw', 'away', 'over', 'under', 'btts']
_KW_PATTERN = re.compile(r'\b(' + '|'.join(_KEYWORDS) + r')\b', re.IGNORECASE)


def _parse_text_bets(text: str) -> list[dict]:
    """
    Parse plain-text bet lines into structured dicts.

    Supported formats (one bet per line):
      Rayo Vallecano vs Girona home 1.69
      Napoli vs Bologna away 2.10
      Benfica vs Braga draw 3.20
      CD Tondela vs Moreirense over 1.5 1.34
      Gil Vicente vs FC Arouca over 2 1.40
      PSG vs Lyon btts 1.70
      PSG vs Lyon btts no 2.10
    """
    bets = []
    for raw_line in text.strip().splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # Split on " vs " to separate home team from the rest
        parts = re.split(r'\s+vs\s+', line, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) != 2:
            continue

        home_team = parts[0].strip()
        rest = parts[1].strip()

        # Find the first market keyword in the rest of the line
        kw_match = _KW_PATTERN.search(rest)
        if not kw_match:
            continue

        away_team = rest[:kw_match.start()].strip()
        keyword   = kw_match.group(1).lower()
        after_kw  = rest[kw_match.end():].strip()
        tokens    = after_kw.split()

        try:
            if keyword in ('home', 'draw', 'away'):
                odds = float(tokens[0])
                bets.append({'home_team': home_team, 'away_team': away_team,
                             'market': '1x2', 'selection': keyword,
                             'odds': odds, 'line': None})

            elif keyword in ('over', 'under'):
                if len(tokens) >= 2:
                    goal_line = float(tokens[0])
                    odds      = float(tokens[1])
                else:
                    goal_line = 2.5
                    odds      = float(tokens[0])
                bets.append({'home_team': home_team, 'away_team': away_team,
                             'market': 'over_under', 'selection': keyword,
                             'odds': odds, 'line': goal_line})

            elif keyword == 'btts':
                if tokens and tokens[0].lower() in ('yes', 'no'):
                    selection = tokens[0].lower()
                    odds      = float(tokens[1])
                else:
                    selection = 'yes'
                    odds      = float(tokens[0])
                bets.append({'home_team': home_team, 'away_team': away_team,
                             'market': 'btts', 'selection': selection,
                             'odds': odds, 'line': None})

        except (ValueError, IndexError):
            continue  # skip malformed lines

    return bets


def _find_team(raw_name: str):
    from football.models import Team  # noqa: PLC0415

    team = Team.objects.filter(name__iexact=raw_name).first()
    if team:
        return team
    team = Team.objects.filter(short_name__iexact=raw_name).first()
    if team:
        return team
    team = Team.objects.filter(name__icontains=raw_name).first()
    if team:
        return team

    candidates = list(Team.objects.values_list('id', 'name', 'short_name'))
    name_to_id: dict[str, int] = {}
    for tid, name, short in candidates:
        name_to_id[name.lower()] = tid
        if short:
            name_to_id[short.lower()] = tid

    matches = difflib.get_close_matches(raw_name.lower(), name_to_id.keys(), n=1, cutoff=0.6)
    if matches:
        return Team.objects.get(pk=name_to_id[matches[0]])
    return None


def _find_fixture(home_name: str, away_name: str):
    from django.utils import timezone   # noqa: PLC0415
    from football.models import Fixture  # noqa: PLC0415

    home_team = _find_team(home_name)
    away_team = _find_team(away_name)
    if not home_team or not away_team:
        return None, home_team, away_team

    fixture = (
        Fixture.objects
        .filter(home_team=home_team, away_team=away_team,
                status__in=['NS', 'TBD'], date__gte=timezone.now())
        .order_by('date')
        .first()
    )
    return fixture, home_team, away_team


def _betslip_key_stats(fixture_dict: dict, adv: dict, standing: dict,
                       pred: dict, favored: str) -> list[str]:
    home = fixture_dict['home_team_name']
    away = fixture_dict['away_team_name']
    home_rank = standing.get('home', {}).get('rank', '?')
    away_rank = standing.get('away', {}).get('rank', '?')
    fav   = favored
    wins  = adv.get(f'{fav}_wins_last_5', 0)
    draws = adv.get(f'{fav}_draws_last_5', 0)
    losses = adv.get(f'{fav}_losses_last_5', 0)
    return [
        f"Table: {home} #{home_rank} / {away} #{away_rank}",
        f"{'Home' if fav == 'home' else 'Away'} last 5: {wins}W {draws}D {losses}L",
        f"API prediction: Home {pred.get('home_win_pct', '?')}% / Away {pred.get('away_win_pct', '?')}%",
    ]


def _run_betslip_pipeline(bet: dict, fixture) -> dict:
    """
    Validate one bet using the prediction engine.

    Same signals and guards as the prediction bot.
    Odds-range limits skipped — bet is already placed.
    Returns {verdict, reason, confidence, key_stats}.
    """
    db     = DBReader()
    engine = PredictionEngine()

    try:
        adv_obj = fixture.advanced_stats
    except Exception:
        return {'verdict': 'remove', 'reason': 'No advanced stats for this fixture yet.',
                'confidence': 0, 'key_stats': []}

    fixture_dict = db._serialize_fixture(fixture, adv_obj)
    adv          = fixture_dict['advanced_stats']
    standing     = db.get_standings(fixture.league.id, fixture.home_team.id,
                                    fixture.away_team.id, fixture.league.season)
    h2h          = db.get_h2h(fixture.id)
    pred         = APICaller().get_predictions(fixture.id)

    home_pct = pred.get('home_win_pct', 0) or 0
    away_pct = pred.get('away_win_pct', 0) or 0
    favored  = 'home' if home_pct >= away_pct else 'away'

    # Signals 1-5
    best_pct = max(home_pct, away_pct)
    if not pred:
        s1 = 6
    elif best_pct >= 55:
        s1 = 25
    elif best_pct >= 45:
        s1 = 18
    elif best_pct >= 35:
        s1 = 12
    else:
        s1 = 6

    form_pts = engine._calc_form_points(adv.get(f'{favored}_wins_last_5', 0),
                                        adv.get(f'{favored}_draws_last_5', 0))
    if form_pts >= 13:
        s2 = 20
    elif form_pts >= 10:
        s2 = 14
    elif form_pts >= 7:
        s2 = 9
    else:
        s2 = 4

    if h2h and len(h2h) >= 3:
        team_name = (fixture_dict['home_team_name'] if favored == 'home'
                     else fixture_dict['away_team_name'])
        rate = engine._calc_h2h_win_rate(h2h, team_name)
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

    ha_rate = engine._calc_home_away_rate(standing, favored)
    if ha_rate >= 0.65:
        s4 = 15
    elif ha_rate >= 0.50:
        s4 = 10
    elif ha_rate >= 0.35:
        s4 = 6
    else:
        s4 = 3

    avg_goals = (adv.get('home_goals_scored_last_5', 0) +
                 adv.get('away_goals_scored_last_5', 0)) / 5
    if avg_goals >= 3.0:
        s5 = 15
    elif avg_goals >= 2.5:
        s5 = 11
    elif avg_goals >= 2.0:
        s5 = 7
    else:
        s5 = 4

    sub_score = s1 + s2 + s3 + s4 + s5

    # Market guard — no odds-range filter
    market       = bet.get('market', '')
    selection    = bet.get('selection', '')
    offered_odds = bet.get('odds', 2.0)
    guard_ok     = True
    rejection_code = ''

    if market == '1x2' and selection in ('home', 'away'):
        guard_ok, rejection_code = engine._win_guard(
            fixture_dict, adv, h2h, standing, selection, offered_odds, sub_score)
    elif market == 'btts' and selection == 'yes':
        guard_ok, rejection_code = engine._btts_guard(
            fixture_dict, adv, h2h, offered_odds, sub_score)
    elif market == 'over_under' and selection == 'over':
        guard_ok, rejection_code = engine._over25_guard(
            fixture_dict, adv, h2h, {'match_winner': {}}, offered_odds, sub_score)

    key_stats = _betslip_key_stats(fixture_dict, adv, standing, pred, favored)

    if not guard_ok:
        return {'verdict': 'remove',
                'reason': _REJECTION_REASONS.get(rejection_code, rejection_code),
                'confidence': sub_score, 'key_stats': key_stats}

    # Signal 6
    implied_prob = (1 / offered_odds) * 100 if offered_odds else 0
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

    if total >= _ACCEPT_THRESHOLD:
        verdict = 'accept'
        reason  = f'Engine score {total}/100 — strong statistical backing.'
    elif total >= _DOWNGRADE_THRESHOLD:
        verdict = 'downgrade'
        reason  = f'Engine score {total}/100 — some support, consider a smaller stake.'
    else:
        verdict = 'remove'
        reason  = f'Engine score {total}/100 — not enough statistical confidence.'

    return {'verdict': verdict, 'reason': reason, 'confidence': total, 'key_stats': key_stats}


def _format_betslip_reply(results: list[dict]) -> str:
    _EMOJI = {'accept': '✅', 'downgrade': '⚠️', 'remove': '❌'}
    counts: dict[str, int] = {'accept': 0, 'downgrade': 0, 'remove': 0}
    lines:  list[str]      = []

    for r in results:
        verdict = r.get('verdict', 'remove')
        counts[verdict] = counts.get(verdict, 0) + 1
        emoji = _EMOJI.get(verdict, '❌')
        match = f"{r['home_team']} vs {r['away_team']}"

        if 'error' in r:
            lines.append(f"{emoji} <b>{match}</b>\n   {r['error']}")
            continue

        stats = '\n   📊 ' + '\n   📊 '.join(r.get('key_stats', []))
        lines.append(
            f"{emoji} <b>{verdict.upper()}</b> — {match}\n"
            f"   {r['selection_label']} @ {r['odds']}  [score: {r.get('confidence', 0)}/100]\n"
            f"   {r['reason']}"
            + stats
        )

    header = (
        f"<b>🎯 Bet Slip Analysis</b>\n"
        f"✅ {counts['accept']} accept  "
        f"⚠️ {counts['downgrade']} downgrade  "
        f"❌ {counts['remove']} remove\n"
        f"{'─' * 28}\n\n"
    )
    return header + '\n\n'.join(lines)


@shared_task(name='prediction.validate_bet_slip_text', bind=True, max_retries=0)
def validate_bet_slip_text(self, text: str, chat_id: int) -> None:
    """
    Bet slip validation — text path (fallback).
    User types bets manually, one per line.
    """
    bets = _parse_text_bets(text)

    if not bets:
        _reply(chat_id, f'❌ Could not parse any bets.\n\n{_BET_FORMAT_HELP}')
        return

    _run_betslip_validation(bets, chat_id)


def _run_betslip_validation(bets: list[dict], chat_id: int) -> None:
    """Shared validation loop used by both photo and text tasks."""

    results: list[dict] = []

    for bet in bets:
        home_name = bet.get('home_team', '')
        away_name = bet.get('away_team', '')
        sel       = bet.get('selection', '')
        sel_label = {
            'home':  f'{home_name} Win', 'draw': 'Draw', 'away': f'{away_name} Win',
            'yes':   'BTTS Yes',         'no':   'BTTS No',
            'over':  f"Over {bet.get('line', 2.5)}", 'under': f"Under {bet.get('line', 2.5)}",
        }.get(sel, sel)

        base = {'home_team': home_name, 'away_team': away_name, 'selection': sel,
                'selection_label': sel_label, 'market': bet.get('market'), 'odds': bet.get('odds')}

        fixture, home_obj, away_obj = _find_fixture(home_name, away_name)

        if not home_obj or not away_obj:
            unresolved = [n for n, t in [(home_name, home_obj), (away_name, away_obj)] if not t]
            results.append({**base, 'verdict': 'remove',
                            'error': f"Team not found: {', '.join(unresolved)}"})
            continue

        if not fixture:
            results.append({**base, 'verdict': 'remove',
                            'error': f"No upcoming fixture for {home_obj.name} vs {away_obj.name}"})
            continue

        try:
            outcome = _run_betslip_pipeline(bet, fixture)
        except Exception as exc:
            logger.error("betslip pipeline failed fixture %s — %s", fixture.id, exc)
            results.append({**base, 'verdict': 'remove', 'error': 'Pipeline error.'})
            continue

        results.append({**base, **outcome})

    _reply(chat_id, _format_betslip_reply(results))
