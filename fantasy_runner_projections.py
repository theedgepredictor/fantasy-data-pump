import os
import pandas as pd
import requests  # not used here, but harmless if you keep it installed
from espn_api_orm.consts import ESPNSportLeagueTypes
from espn_api_orm.league.api import ESPNLeagueAPI

from src.fantasy_utils import process_week_data
from src.utils import (
    get_seasons_to_update,
    find_year_for_season,
    get_current_week,
    get_dataframe,
    put_json_file,
    put_dataframe,
)

LEAGUE_ID = 2127
SWID = None
espn_s2 = None


def ensure_dir(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)


if __name__ == "__main__":
    root_path = "./raw"
    sport_league_pairs = [
        ESPNSportLeagueTypes.FOOTBALL_NFL,
        # add others as you enable them
    ]

    for sport_league in sport_league_pairs:
        sport_str, league_str = sport_league.value.split("/")
        raw_proj_path = f"{root_path}/{sport_str}/{league_str}/projections/"
        processed_proj_path = f"./processed/{sport_str}/{league_str}/projections/"
        ensure_dir(raw_proj_path)
        ensure_dir(processed_proj_path)

        league_api = ESPNLeagueAPI(sport_str, league_str)
        if not league_api.is_active():
            print("Running in OffSeason")

        update_seasons = get_seasons_to_update(root_path, sport_league)
        print(f"Running Projections Pump for: {sport_league.value} from {min(update_seasons)}-{max(update_seasons)}")

        for update_season in update_seasons:
            season_raw_proj_path = f"{raw_proj_path}{update_season}/"
            ensure_dir(season_raw_proj_path)

            processed_df = get_dataframe(f"{processed_proj_path}{update_season}.parquet")

            # determine weeks to (re)build
            if update_season == find_year_for_season(sport_league):
                current_week = get_current_week(sport_league)
                if processed_df.shape[0] != 0:
                    max_processed_week = 1 if current_week == 1 else current_week - 1
                    # Clear potentially stale data beyond the last complete week
                    processed_df = processed_df[processed_df.week <= max_processed_week].copy()
                else:
                    max_processed_week = 1
                update_weeks = list(range(max_processed_week, 18 + 1))
            else:
                update_weeks = list(range(1, (18 + 1 if update_season >= 2021 else 17 + 1)))

            season_fantasy_players = []

            for update_week in update_weeks:
                week_path = f"{season_raw_proj_path}{update_week}/"
                ensure_dir(week_path)

                weekly_fantasy_players = process_week_data(
                    LEAGUE_ID, update_season, update_week, swid=SWID, espn_s2=espn_s2
                )
                put_json_file(f"{week_path}players.json", weekly_fantasy_players)
                season_fantasy_players.extend(weekly_fantasy_players)

            # merge with previously processed season parquet
            season_df = pd.DataFrame(season_fantasy_players)
            if processed_df.shape[0] == 0 and season_df.shape[0] == 0:
                print(f"[Projections] {sport_league.value} {update_season}: No data to write.")
                continue

            fantasy_df = pd.concat([processed_df, season_df], ignore_index=True).drop_duplicates(
                subset=["season", "week", "player_id"], keep="last"
            )
            put_dataframe(fantasy_df, f"{processed_proj_path}{update_season}.parquet")
            print(f"[Projections] Wrote processed parquet for {update_season} → {processed_proj_path}{update_season}.parquet")