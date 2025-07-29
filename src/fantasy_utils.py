import datetime
import os
import pandas as pd
from typing import Any, Dict
import re
from .utils import put_json_file, get_dataframe, put_dataframe, camel_to_snake
from espn_api.football import League


def flatten_player_payload(payload: Dict[str, Any], season: int, week: int) -> Dict[str, Any]:
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

    # Projected stats
    if "projected_points" in stats:
        row["projected_points"] = stats["projected_points"]
    proj_break = stats.get("projected_breakdown", {})
    for raw_k, v in proj_break.items():
        if raw_k.isdigit():
            continue
        k = camel_to_snake(raw_k)
        row[f"projected_{k}"] = v

    return row


def get_weekly_free_agent_data(league: League, season: int, week: int) -> list:
    """Gets free agent data for a given week"""
    players = []
    
    # Free agents by position with size limits
    position_limits = {
        'RB': 100,
        'WR': 100,
        'QB': 40,
        'TE': 40,
        'K': 40,
        'D/ST': 32
    }

    for position, size in position_limits.items():
        position_players = league.free_agents(week, size=size, position=position)
        players.extend(position_players)

    # Flatten player data
    return [flatten_player_payload(player.__dict__, season, week) for player in players]


def process_week_data(league_id: int, season: int, week: int, swid=None, espn_s2=None):
    """Process data for a single week"""
    print(f"Processing season {season}, week {week}")
    
    league = League(league_id=league_id, swid=swid, espn_s2=espn_s2, year=season)
    players = []

    # Get rostered players
    league.load_roster_week(week)
    for team in league.teams:
        roster = [flatten_player_payload(player.__dict__, season, week) for player in team.roster]
        players.extend(roster)

    # Get free agents
    free_agents = get_weekly_free_agent_data(league, season, week)
    players.extend(free_agents)

    return players
