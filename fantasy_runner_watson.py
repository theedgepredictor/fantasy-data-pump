import os
import pandas as pd
import requests
from espn_api_orm.consts import ESPNSportLeagueTypes
from espn_api_orm.league.api import ESPNLeagueAPI

from src.utils import (
    get_seasons_to_update,
    find_year_for_season,
    get_current_week,
    get_dataframe,
    put_dataframe,
)
from src.watson_fantasy import fetch_watson_triplet, flatten_watson_triplet

LEAGUE_ID = 2127  # not used here directly, but keep if helpful elsewhere


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

        processed_proj_path = f"./processed/{sport_str}/{league_str}/projections/"
        processed_watson_path = f"./processed/{sport_str}/{league_str}/watson/"
        ensure_dir(processed_watson_path)

        league_api = ESPNLeagueAPI(sport_str, league_str)
        if not league_api.is_active():
            print("Running in OffSeason (Watson)")

        update_seasons = get_seasons_to_update("./processed", sport_league, suffix='watson')
        print(f"Running Watson Pump for: {sport_league.value} from {min(update_seasons)}-{max(update_seasons)}")

        session = requests.session()

        for update_season in update_seasons:
            # load already-processed watson parquet (may be empty)
            processed_watson_df = get_dataframe(f"{processed_watson_path}{update_season}.parquet")

            # if current season, trim potentially stale weeks beyond the last complete week
            if update_season == find_year_for_season(sport_league) and processed_watson_df.shape[0] != 0:
                current_week = get_current_week(sport_league)
                max_processed_week = 1 if current_week == 1 else current_week - 1
                processed_watson_df = processed_watson_df[processed_watson_df.week <= max_processed_week].copy()

            # load projections parquet for this season (source of truth for which players to fetch)
            projections_file = f"{processed_proj_path}{update_season}.parquet"
            proj_df = get_dataframe(projections_file)

            if proj_df.shape[0] == 0:
                print(f"[Watson] {sport_league.value} {update_season}: No projections parquet found or empty at {projections_file}, skipping.")
                continue

            unique_players_for_watson = proj_df[proj_df.player_id.notnull()].player_id.astype("int64", errors="ignore").unique()

            print(f"[Watson] {update_season}: {len(unique_players_for_watson)} players in projections;  fetching {len(unique_players_for_watson)}.")

            watson_rows = []
            for player_id in unique_players_for_watson:
                try:
                    proj, clf, meta = fetch_watson_triplet(update_season, player_id, session)
                    watson_rows.extend(flatten_watson_triplet(player_id, proj, clf, meta))
                except Exception as e:
                    # Keep going; log and continue
                    print(f"[Watson] season={update_season} player_id={player_id} error: {e}")

            if len(watson_rows) == 0 and processed_watson_df.shape[0] == 0:
                print(f"[Watson] {sport_league.value} {update_season}: Nothing new to write.")
                continue

            season_watson_df = pd.DataFrame(watson_rows)
            if season_watson_df.shape[0] != 0:
                season_watson_df["season"] = update_season

            watson_combined = pd.concat([processed_watson_df, season_watson_df], ignore_index=True).drop_duplicates(
                subset=["season", "week", "player_id"],
                keep="last",
            )
            put_dataframe(watson_combined, f"{processed_watson_path}{update_season}.parquet")
            print(f"[Watson] Wrote processed parquet for {update_season} → {processed_watson_path}{update_season}.parquet")
