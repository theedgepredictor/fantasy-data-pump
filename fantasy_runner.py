import json
import os

import pandas as pd
from espn_api_orm.event.api import ESPNEventAPI
from espn_api_orm.event.schema import Event
from espn_api_orm.league.api import ESPNLeagueAPI
from espn_api_orm.scoreboard.api import ESPNScoreboardAPI
from espn_api_orm.calendar.api import ESPNCalendarAPI
from espn_api_orm.consts import ESPNSportLeagueTypes, ESPNSportSeasonTypes

import requests
import datetime

from src.fantasy_utils import process_week_data
from src.utils import get_seasons_to_update, find_year_for_season, get_current_week, get_dataframe, put_json_file, put_dataframe

LEAGUE_ID = 2127  # Your ESPN Fantasy Football League ID
SWID = "65B6E1BF-68A5-4847-B4CE-6A99AB09082A"
espn_s2 = "AECaUvN6Q0wt3SAgV13KPLN3AQqO/HqFNWFV3Ep2lD8SlD88RedERW8LjTZ8Ui4JHCXWI1J75e9w6OiZI2PLbLvipK8ahZR5ufkFauQiEm/EoiAxzojji/RB1KnF+6/QB2ob+ZhSAbVhiDYOd/90v9N4aj4IUQcHFNziNyfgiqHjvi2/uVwLcHYSdnIDEii+KekYnjEoy0BY5AgaZsyLNH7D9jDR12kifghZWRKa3E9lz/IdXWN2MVK4J8f19QSvxiJ5Y4aBQDpzdp+VBWbG50sndHUBk8bjWO7LfRbPNRJ3wA=="



if __name__ == '__main__':
    root_path = './raw'
    sport_league_pairs = list(ESPNSportLeagueTypes)
    sport_league_pairs = [
        ESPNSportLeagueTypes.FOOTBALL_NFL,
        #ESPNSportLeagueTypes.FOOTBALL_COLLEGE_FOOTBALL,
        #ESPNSportLeagueTypes.BASKETBALL_MENS_COLLEGE_BASKETBALL,
        #ESPNSportLeagueTypes.BASKETBALL_NBA,
        #ESPNSportLeagueTypes.BASKETBALL_WNBA,
        #ESPNSportLeagueTypes.BASKETBALL_WOMENS_COLLEGE_BASKETBALL,
    ]
    for sport_league in sport_league_pairs:
        sport_str, league_str = sport_league.value.split('/')
        path = f'{root_path}/{sport_str}/{league_str}/'
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        league_api = ESPNLeagueAPI(sport_str, league_str)
        ## Check if league is active
        if not league_api.is_active():
            #continue
            print('Running in OffSeason')

        processed_fantasy_path = f'./processed/season/{sport_str}/{league_str}/'
        if not os.path.exists(processed_fantasy_path):
            os.makedirs(processed_fantasy_path, exist_ok=True)

        ## Check what seasons need to get updated
        update_seasons = get_seasons_to_update(root_path, sport_league)

        print(f"Running Raw Pump for: {sport_league.value} from {min(update_seasons)}-{max(update_seasons)}")
        for update_season in update_seasons:
            season_path = f"{path}{update_season}/"
            if not os.path.exists(season_path):
                os.makedirs(season_path, exist_ok=True)

            processed_df = get_dataframe(f"{processed_fantasy_path}{update_season}.parquet")

            if update_season == find_year_for_season(sport_league):
                current_week = get_current_week(sport_league)
                if processed_df.shape[0] != 0:
                    max_processed_week = 1 if current_week == 1 else current_week - 1

                    # Clear potentially stale data
                    processed_df = processed_df[processed_df.week <= max_processed_week].copy()
                else:
                    max_processed_week = 1
                update_weeks = list(range(max_processed_week, 18+1))
            else:
                update_weeks = list(range(1, (18+1 if update_season >= 2021 else 17+1)))

            season_fantasy_players = []

            for update_week in update_weeks:
                week_path = f"{season_path}{update_week}/"
                if not os.path.exists(week_path):
                    os.makedirs(week_path, exist_ok=True)

                weekly_fantasy_players = process_week_data(LEAGUE_ID, update_season, update_week, swid=SWID, espn_s2=espn_s2)
                put_json_file(f"{week_path}players.json", weekly_fantasy_players)

                season_fantasy_players.extend(weekly_fantasy_players)

            fantasy_df = pd.concat([processed_df, pd.DataFrame(season_fantasy_players)]).drop_duplicates(subset=['season', 'week', 'player_id'], keep='last')
            put_dataframe(fantasy_df, f"{processed_fantasy_path}{update_season}.parquet")
