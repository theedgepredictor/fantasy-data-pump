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
from src.watson_fantasy import fetch_watson_triplet, flatten_watson_triplet

LEAGUE_ID = 2127  # Your ESPN Fantasy Football League ID
SWID = None
espn_s2 = None



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
        path = f'{root_path}/{sport_str}/{league_str}/projections/'
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

        watson_path = f'{root_path}/{sport_str}/{league_str}/watson/'
        if not os.path.exists(watson_path):
            os.makedirs(watson_path, exist_ok=True)

        league_api = ESPNLeagueAPI(sport_str, league_str)
        ## Check if league is active
        if not league_api.is_active():
            #continue
            print('Running in OffSeason')

        processed_fantasy_projections_path = f'./processed/{sport_str}/{league_str}/projections/'
        processed_fantasy_watson_path = f'./processed/{sport_str}/{league_str}/watson/'
        if not os.path.exists(processed_fantasy_projections_path):
            os.makedirs(processed_fantasy_projections_path, exist_ok=True)

        if not os.path.exists(processed_fantasy_watson_path):
            os.makedirs(processed_fantasy_watson_path, exist_ok=True)

        ## Check what seasons need to get updated
        update_seasons = get_seasons_to_update(root_path, sport_league)

        print(f"Running Raw Pump for: {sport_league.value} from {min(update_seasons)}-{max(update_seasons)}")
        for update_season in update_seasons:
            season_path = f"{path}{update_season}/"
            if not os.path.exists(season_path):
                os.makedirs(season_path, exist_ok=True)

            watson_season_path = f"{watson_path}{update_season}/"
            if not os.path.exists(watson_season_path):
                os.makedirs(watson_season_path, exist_ok=True)

            processed_df = get_dataframe(f"{processed_fantasy_projections_path}{update_season}.parquet")
            processed_watson_df = get_dataframe(f"{processed_fantasy_watson_path}{update_season}.parquet")

            if update_season == find_year_for_season(sport_league):
                current_week = get_current_week(sport_league)
                if processed_df.shape[0] != 0:
                    max_processed_week = 1 if current_week == 1 else current_week - 1

                    # Clear potentially stale data
                    processed_df = processed_df[processed_df.week <= max_processed_week].copy()
                    processed_watson_df = processed_watson_df[processed_watson_df.week <= max_processed_week].copy()
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
            season_fantasy_players_df = pd.DataFrame(season_fantasy_players)

            fantasy_df = pd.concat([processed_df, season_fantasy_players_df]).drop_duplicates(subset=['season', 'week', 'player_id'], keep='last')
            put_dataframe(fantasy_df, f"{processed_fantasy_projections_path}{update_season}.parquet")

            session = requests.session()
            watson_players = []
            unique_players_for_watson = season_fantasy_players_df[season_fantasy_players_df.player_id.notnull()].copy().player_id.unique()
            print(f"Getting {len(unique_players_for_watson)} players for watson...")
            for player_id in unique_players_for_watson:
                proj, clf, meta = fetch_watson_triplet(update_season, player_id, session)
                watson_obj = {
                    "proj": proj,
                    "clf": clf,
                    "meta": meta,
                }
                put_json_file(f"{watson_season_path}{player_id}.json", watson_obj)
                watson_players.extend(flatten_watson_triplet(proj, clf, meta))

            season_watson_fantasy_players_df = pd.DataFrame(watson_players)
            season_watson_fantasy_players_df['season'] = update_season
            watson_fantasy_df = pd.concat([processed_watson_df, season_watson_fantasy_players_df]).drop_duplicates(subset=['season', 'week', 'player_id'], keep='last')
            put_dataframe(watson_fantasy_df, f"{processed_fantasy_watson_path}{update_season}.parquet")




