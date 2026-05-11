"""
betslip/tasks.py — Bet slip validation pipeline.

No DB writes. No Claude text validation. Pure data + engine:

  Step 1: Download photo bytes from Telegram CDN
  Step 2: Claude Vision — extract structured bets from the image
  Step 3: Resolve each team name → Team → Fixture
  Step 4: Run the prediction pipeline (DBReader + APICaller + PredictionEngine)
          - Same signals 1-5 as the prediction bot
          - Same per-market guards (win / BTTS / over-2.5)
          - Odds-range limits REMOVED (bet is already placed)
  Step 5: Verdict from score + guard result, reason from the data itself
"""
import base64
import difflib
import json
import logging

import requests
from celery import shared_task
from django.conf import settings

from betslip.bot import send_message

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Guard rejection code → plain-English reason
# ---------------------------------------------------------------------------

_REJECTION_REASONS = {
    'REJECTED_WIN:FORM':        'Poor venue form — win rate below threshold in last 5',
    'REJECTED_WIN:OPPOSITION':  'Opposition has kept too many clean sheets away',
    'REJECTED_WIN:H2H_BIAS':    'H2H record does not support this pick',
    'REJECTED_WIN:ODDS_FLOOR':  'Short-odds pick fails strict form + H2H + rank conditions',
    'REJECTED_WIN:TIER_PENALTY':'Low-priority league — form data too noisy',
    'REJECTED_BTTS:H2H_FILTER': 'Away team rarely scores in H2H meetings',
    'REJECTED_BTTS:AWAY_DROUGHT':'Away team blanked in 3+ of last 5 away games',
    'REJECTED_BTTS:DOMINANCE':  'Dominance asymmetry — home side likely to keep a clean sheet',
    'REJECTED_BTTS:ODDS_SANITY':'Odds too high given both teams\' scoring stats',
    'REJECTED_OVER:SCORING_RATE':'Combined over-2.5 rate too low in venue context',
    'REJECTED_OVER:DEFENCE':    'Both teams too tight defensively',
    'REJECTED_OVER:H2H_GOALS':  'H2H average goals too low for over 2.5',
    'REJECTED_OVER:MATCH_CONTEXT':'Heavy favourite likely to control and close game down',
    'REJECTED_OVER:ODDS_RANGE': 'H2H average does not justify these odds',
}

# Verdict thresholds (same scoring scale as the prediction engine)
_ACCEPT_THRESHOLD    = 65
_DOWNGRADE_THRESHOLD = 55


# ---------------------------------------------------------------------------
# Step 1: Download photo from Telegram CDN
# ---------------------------------------------------------------------------

def _download_photo(file_id: str) -> bytes:
    token = settings.TELEGRAM_BOT_TOKEN
    base  = f'https://api.telegram.org/bot{token}'

    resp = requests.get(f'{base}/getFile', params={'file_id': file_id}, timeout=10)
    resp.raise_for_status()
    file_path = resp.json()['result']['file_path']

    dl = requests.get(f'https://api.telegram.org/file/bot{token}/{file_path}', timeout=30)
    dl.raise_for_status()
    return dl.content


# ---------------------------------------------------------------------------
# Step 2: Claude Vision OCR
# ---------------------------------------------------------------------------

def _extract_bets(image_bytes: bytes) -> list[dict]:
    """
    Send image to Claude Vision. Returns structured bet list:
      [{home_team, away_team, market, selection, odds, line?}]
    """
    import anthropic  # noqa: PLC0415

    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
    b64    = base64.b64encode(image_bytes).decode()

    prompt = """Extract every individual bet from this bet slip image.

Return a JSON object with key "bets" — an array where each element is:
{
  "home_team": "<team name exactly as shown on slip>",
  "away_team": "<team name exactly as shown on slip>",
  "market": "1x2 | btts | over_under | dnb | double_chance",
  "selection": "home | draw | away | yes | no | over | under",
  "odds": <decimal number>,
  "line": <number or null>
}

Rules:
- 1X2: selection is "home", "draw", or "away"
- BTTS: selection is "yes" or "no"
- over/under: selection is "over" or "under", line is the goal threshold (e.g. 2.5)
- If a field is unclear, make your best guess
- Return only valid JSON, no other text"""

    response = client.messages.create(
        model='claude-sonnet-4-6',
        max_tokens=1000,
        messages=[{
            'role': 'user',
            'content': [
                {'type': 'image', 'source': {'type': 'base64', 'media_type': 'image/jpeg', 'data': b64}},
                {'type': 'text',  'text': prompt},
            ],
        }],
    )

    raw = response.content[0].text.strip()
    if raw.startswith('```'):
        raw = raw.split('```', 2)[1]
        if raw.startswith('json'):
            raw = raw[4:]
        raw = raw.rsplit('```', 1)[0].strip()

    return json.loads(raw).get('bets', [])


# ---------------------------------------------------------------------------
# Step 3: Team name → Fixture resolution
# ---------------------------------------------------------------------------

def _find_team(raw_name: str):
    """Fuzzy-match a raw bookmaker team name to a Team DB row."""
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
    """
    Resolve team names and find the upcoming Fixture.
    Returns (fixture_or_None, home_team_or_None, away_team_or_None).
    """
    from django.utils import timezone  # noqa: PLC0415
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


# ---------------------------------------------------------------------------
# Step 4 + 5: Prediction pipeline validation
# ---------------------------------------------------------------------------

def _run_prediction_pipeline(bet: dict, fixture) -> dict:
    """
    Validate a single bet using the prediction engine and our DB data.

    Same pipeline as the prediction bot:
      - DBReader  → fixture dict + standings + H2H
      - APICaller → win probabilities (Redis-cached)
      - PredictionEngine → signals 1-5 + market guard

    Odds-range limits are SKIPPED — the user already placed the bet.
    Only the data-quality guards run (form, H2H, opposition, defence, etc.).

    Returns {verdict, reason, confidence, key_stats}.
    verdict is one of: 'accept' | 'downgrade' | 'remove'
    """
    from prediction.api_caller import APICaller  # noqa: PLC0415
    from prediction.db_reader import DBReader    # noqa: PLC0415
    from prediction.engine import PredictionEngine  # noqa: PLC0415

    db     = DBReader()
    engine = PredictionEngine()

    # Advanced stats are required for the engine to run
    try:
        adv_obj = fixture.advanced_stats
    except Exception:
        return {
            'verdict': 'remove',
            'reason': 'No advanced stats computed for this fixture yet.',
            'confidence': 0,
            'key_stats': [],
        }

    fixture_dict = db._serialize_fixture(fixture, adv_obj)
    adv          = fixture_dict['advanced_stats']

    standing = db.get_standings(
        fixture.league.id,
        fixture.home_team.id,
        fixture.away_team.id,
        fixture.league.season,
    )
    h2h  = db.get_h2h(fixture.id)
    pred = APICaller().get_predictions(fixture.id)

    home_pct = pred.get('home_win_pct', 0) or 0
    away_pct = pred.get('away_win_pct', 0) or 0
    favored  = 'home' if home_pct >= away_pct else 'away'

    # ── Signals 1-5 (identical to PredictionEngine.score_fixture) ───────────

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

    form_pts = engine._calc_form_points(
        adv.get(f'{favored}_wins_last_5', 0),
        adv.get(f'{favored}_draws_last_5', 0),
    )
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

    avg_goals = (
        adv.get('home_goals_scored_last_5', 0) +
        adv.get('away_goals_scored_last_5', 0)
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

    # ── Market guard — no odds range check ──────────────────────────────────
    market       = bet.get('market', '')
    selection    = bet.get('selection', '')
    offered_odds = bet.get('odds', 2.0)

    guard_ok       = True
    rejection_code = ''

    if market == '1x2' and selection in ('home', 'away'):
        guard_ok, rejection_code = engine._win_guard(
            fixture_dict, adv, h2h, standing, selection, offered_odds, sub_score,
        )
    elif market == 'btts' and selection == 'yes':
        guard_ok, rejection_code = engine._btts_guard(
            fixture_dict, adv, h2h, offered_odds, sub_score,
        )
    elif market == 'over_under' and selection == 'over':
        guard_ok, rejection_code = engine._over25_guard(
            fixture_dict, adv, h2h, {'match_winner': {}}, offered_odds, sub_score,
        )
    # draw / under / double_chance → no guard, go straight to scoring

    if not guard_ok:
        return {
            'verdict': 'remove',
            'reason': _REJECTION_REASONS.get(rejection_code, rejection_code),
            'confidence': sub_score,
            'key_stats': _key_stats(fixture_dict, adv, standing, pred, favored),
        }

    # ── Signal 6: odds value ─────────────────────────────────────────────────
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

    # ── Verdict from score ───────────────────────────────────────────────────
    if total >= _ACCEPT_THRESHOLD:
        verdict = 'accept'
        reason  = f'Engine score {total}/100 — strong statistical backing.'
    elif total >= _DOWNGRADE_THRESHOLD:
        verdict = 'downgrade'
        reason  = f'Engine score {total}/100 — some support, consider a smaller stake.'
    else:
        verdict = 'remove'
        reason  = f'Engine score {total}/100 — not enough statistical confidence.'

    return {
        'verdict':    verdict,
        'reason':     reason,
        'confidence': total,
        'key_stats':  _key_stats(fixture_dict, adv, standing, pred, favored),
    }


def _key_stats(fixture_dict: dict, adv: dict, standing: dict, pred: dict, favored: str) -> list[str]:
    """Build 3 concise stat lines from the data for display in Telegram."""
    home = fixture_dict['home_team_name']
    away = fixture_dict['away_team_name']

    home_rank = standing.get('home', {}).get('rank', '?')
    away_rank = standing.get('away', {}).get('rank', '?')
    rank_line = f"Table: {home} #{home_rank} / {away} #{away_rank}"

    fav = 'home' if favored == 'home' else 'away'
    wins   = adv.get(f'{fav}_wins_last_5', 0)
    draws  = adv.get(f'{fav}_draws_last_5', 0)
    losses = adv.get(f'{fav}_losses_last_5', 0)
    form_line = f"{'Home' if fav == 'home' else 'Away'} last 5: {wins}W {draws}D {losses}L"

    home_pct = pred.get('home_win_pct', '?')
    away_pct = pred.get('away_win_pct', '?')
    pred_line = f"API prediction: Home {home_pct}% / Away {away_pct}%"

    return [rank_line, form_line, pred_line]


# ---------------------------------------------------------------------------
# Format Telegram reply
# ---------------------------------------------------------------------------

_EMOJI = {'accept': '✅', 'downgrade': '⚠️', 'remove': '❌'}


def _format_reply(results: list[dict]) -> str:
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

        conf       = r.get('confidence', 0)
        stats      = '\n   📊 ' + '\n   📊 '.join(r.get('key_stats', []))
        lines.append(
            f"{emoji} <b>{verdict.upper()}</b> — {match}\n"
            f"   {r['selection_label']} @ {r['odds']}  [score: {conf}/100]\n"
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


# ---------------------------------------------------------------------------
# Main Celery task
# ---------------------------------------------------------------------------

@shared_task(name='betslip.validate_bet_slip', bind=True, max_retries=0)
def validate_bet_slip(self, file_id: str, chat_id: int) -> None:
    """
    Full pipeline: download → OCR → resolve → prediction engine → reply.
    No DB writes. Always sends something back to chat_id.
    """
    try:
        image_bytes = _download_photo(file_id)
    except Exception as exc:
        send_message(chat_id, f'❌ Could not download image: {exc}')
        return

    try:
        bets = _extract_bets(image_bytes)
    except Exception as exc:
        send_message(chat_id, f'❌ Could not read the slip: {exc}\nTry a clearer photo.')
        return

    if not bets:
        send_message(chat_id, '❌ No bets found. Send a clearer photo of the bet slip.')
        return

    results: list[dict] = []

    for bet in bets:
        home_name = bet.get('home_team', '')
        away_name = bet.get('away_team', '')
        sel       = bet.get('selection', '')
        sel_label = {
            'home':  f'{home_name} Win',
            'draw':  'Draw',
            'away':  f'{away_name} Win',
            'yes':   'BTTS Yes',
            'no':    'BTTS No',
            'over':  f"Over {bet.get('line', 2.5)}",
            'under': f"Under {bet.get('line', 2.5)}",
        }.get(sel, sel)

        base = {
            'home_team':       home_name,
            'away_team':       away_name,
            'selection':       sel,
            'selection_label': sel_label,
            'market':          bet.get('market'),
            'odds':            bet.get('odds'),
        }

        fixture, home_obj, away_obj = _find_fixture(home_name, away_name)

        if not home_obj or not away_obj:
            unresolved = [n for n, t in [(home_name, home_obj), (away_name, away_obj)] if not t]
            results.append({**base, 'verdict': 'remove',
                            'error': f"Team not found in database: {', '.join(unresolved)}"})
            continue

        if not fixture:
            results.append({**base, 'verdict': 'remove',
                            'error': f"No upcoming fixture found for {home_obj.name} vs {away_obj.name}"})
            continue

        try:
            outcome = _run_prediction_pipeline(bet, fixture)
        except Exception as exc:
            logger.error("validate_bet_slip: pipeline failed fixture %s — %s", fixture.id, exc)
            results.append({**base, 'verdict': 'remove',
                            'error': 'Pipeline error — could not validate this bet.'})
            continue

        results.append({**base, **outcome})

    send_message(chat_id, _format_reply(results))
