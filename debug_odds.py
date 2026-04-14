import django, os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "fbackend.settings")
django.setup()

from prediction.db_reader import DBReader
from prediction.api_caller import APICaller
from prediction.engine import PredictionEngine

db = DBReader()
api = APICaller()
engine = PredictionEngine()

fixtures = db.get_todays_fixtures("monster")
preds, odds, standings = {}, {}, {}

for f in fixtures[:30]:
    fid = f["fixture_id"]
    preds[fid] = api.get_predictions(fid) or {}
    odds[fid] = api.get_odds(fid) or {}

eligible = engine.filter_eligible_fixtures(fixtures[:30], standings, preds, odds, "monster")

for f in eligible:
    fid = f["fixture_id"]
    adv = f.get("advanced_stats", {})
    o = odds.get(fid, {})
    p = preds.get(fid, {})
    home_odds = o.get("match_winner", {}).get("home", 0)
    away_odds = o.get("match_winner", {}).get("away", 0)
    btts = o.get("btts", {}).get("yes", 0)
    over = o.get("over_under", {}).get("over", 0)
    dc = o.get("double_chance", {})
    avg_goals = (adv.get("home_goals_scored_last_5", 0) + adv.get("away_goals_scored_last_5", 0)) / 5
    home_pct = p.get("home_win_pct", 0) or 0
    away_pct = p.get("away_win_pct", 0) or 0
    print(f"{fid}: home_odds={home_odds} away_odds={away_odds} btts={btts} over={over} dc={dc} avg_goals={avg_goals:.1f} home%={home_pct} away%={away_pct}")
