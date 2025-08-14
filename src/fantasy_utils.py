import datetime
import json
import os
import pandas as pd
from typing import Any, Dict, List
import re
from .utils import put_json_file, get_dataframe, put_dataframe, camel_to_snake
from espn_api.football import League, BoxPlayer


def flatten_player_payload(player, payload: Dict[str, Any], season: int, week: int) -> Dict[str, Any]:
    """Flattens a player's payload into a row for the dataframe"""
    row: Dict[str, Any] = {
        "season": season,
        "week": week,
        "player_id": payload.get("playerId"),
        "name": payload.get("name"),
        "position": payload.get("position"),
        "team": payload.get("proTeam"),
        "percent_owned": payload.get("percent_owned"),
        "percent_started": payload.get("percent_started"),
        "total_points": payload.get("total_points"),
        "projected_total_points": payload.get("projected_total_points"),
        "avg_points": payload.get("avg_points"),
        "projected_avg_points": payload.get("projected_avg_points"),
        "last_updated": datetime.datetime.now().isoformat()
    }

    stats = payload.get("stats", {}).get(week, {})

    # Actual stats
    if "points" in stats:
        row["points"] = stats["points"]
    if "avg_points" in stats:
        row["avg_points_week"] = stats["avg_points"]

    breakdown = stats.get("breakdown", {})
    for raw_k, v in breakdown.items():
        if raw_k.isdigit():
            continue
        k = camel_to_snake(raw_k)
        row[f"actual_{k}"] = v
        if k == 'receiving_receptions':
            row["points"] = stats["points"] + v*0.5 # Shift from 1/2 PPR to Full to match other stats

    # Projected stats
    if "projected_points" in stats:
        row["projected_points"] = stats["projected_points"]
    proj_break = stats.get("projected_breakdown", {})
    for raw_k, v in proj_break.items():
        if raw_k.isdigit():
            continue
        k = camel_to_snake(raw_k)
        row[f"projected_{k}"] = v
        if k == 'receiving_receptions':
            row["projected_points"] = stats["projected_points"] + v*0.5 # Shift from 1/2 PPR to Full to match other stats

    ### inject additional player stats missed from box
    try:
        ppr_rank_data = player['player']['draftRanksByRankType']['PPR']
        standard_rank_data = player['player']['draftRanksByRankType']['STANDARD']
        row['PPR_draft_rank'] = ppr_rank_data.get('rank', 3000)
        row['STANDARD_draft_rank'] = standard_rank_data.get('rank', 3000)
        row['draft_auction_value'] = standard_rank_data.get('auctionValue', -1)
    except Exception as e:
        row['PPR_draft_rank'] = 3000
        row['STANDARD_draft_rank'] = 3000
        row['draft_auction_value'] = -1

    try:
        ppr_rank_data = player['player']['ownership']
        row['community_ADP'] = ppr_rank_data.get('averageDraftPosition', 3000)
    except Exception as e:
        row['community_ADP'] = 3000

    return row

def process_week_data(league_id: int, season: int, week: int, swid=None, espn_s2=None,chunk: int = 250):
    """
       Pull ALL players for a given season/week from ESPN's kona_player_info, paginating until exhausted.
       Returns a list of flattened dict records (includes season, week, player_id).
       """
    print(f"[ESPN] Fetching season={season} week={week}")

    league = League(league_id=league_id, year=season, swid=swid, espn_s2=espn_s2)

    # Needed to construct BoxPlayer objects for this week
    pro_schedule = league._get_pro_schedule(week)
    positional_rankings = league._get_positional_ratings(week)

    params = {
        "view": "kona_player_info",
        "scoringPeriodId": week,
    }

    all_records: List[Dict[str, Any]] = []
    seen_ids = set()  # guard against any dupes that sometimes appear in paging

    offset = 0
    while True:
        filters = {
            "players": {
                "filterSlotIds": {"value": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,23,24]},
                "filterRanksForScoringPeriodIds":{"value":[week]},
                "limit": chunk,
                "offset": offset,
                "sortPercOwned": {"sortAsc": False, "sortPriority": 1},
                "sortDraftRanks":{"sortPriority":100,"sortAsc":True,"value":"STANDARD"},
                "filterRanksForRankTypes": {"value": ["PPR"]},
                "filterRanksForSlotIds":{"value":[0,2,4,6,17,16,8,9,10,12,13,24,11,14,15]},
            }
        }
        headers = {"x-fantasy-filter": json.dumps(filters)}

        data = league.espn_request.league_get(params=params, headers=headers)
        batch = data.get("players", []) or []

        print(f"[ESPN] page offset={offset} fetched={len(batch)}")
        if not batch:
            break

        # Build BoxPlayers and flatten
        for p in batch:
            bp = BoxPlayer(p, pro_schedule, positional_rankings, week, season)
            rec = flatten_player_payload(p, bp.__dict__, season, week)

            # Only append if we have a player_id; skip any malformed rows
            pid = rec.get("player_id")
            if pid is None or pid in seen_ids:
                continue
            seen_ids.add(pid)
            all_records.append(rec)

        # next page
        offset += chunk

    print(f"[ESPN] Done season={season} week={week} total_players={len(all_records)}")
    return all_records


