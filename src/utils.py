import json
import re
import pandas as pd
from espn_api_orm.consts import ESPNSportLeagueTypes, ESPNSportSeasonTypes
from espn_api_orm.calendar.api import ESPNCalendarAPI

from src.consts import SEASON_START_MONTH, START_SEASONS
import datetime
import os
from typing import List
import pyarrow as pa



def get_json_file(path):
    try:
        with open(path, 'r') as file:
            return json.load(file)
    except Exception as e:
        return {}

# Function to save sport league file
def put_json_file(path, data):
    with open(path, 'w') as file:
        json.dump(data, file, indent=4)

def get_seasons_to_update(root_path, sport):
    """
    Get a list of seasons to update based on the root path and sport.

    Args:
        root_path (str): Root path for the sport data.
        sport (ESPNSportTypes): Type of sport.

    Returns:
        List: List of seasons to update.
    """
    current_season = find_year_for_season(sport)
    if os.path.exists(f'{root_path}/{sport.value}'):
        seasons = os.listdir(f'{root_path}/{sport.value}/projections')
        fs_season = -1
        for season_week in seasons:
            season = season_week.split('/')[0]
            temp = int(season.split('.')[0])
            if temp > fs_season:
                fs_season = temp
        if fs_season == -1:
            fs_season = START_SEASONS[sport]
    else:
        fs_season = START_SEASONS[sport]
    return list(range(fs_season, current_season + 1))


def clean_string(s):
    if isinstance(s, str):
        return re.sub("[^A-Za-z0-9 ]+", '', s)
    else:
        return s


def re_braces(s):
    if isinstance(s, str):
        return re.sub("[\(\[].*?[\)\]]", "", s)
    else:
        return s


def name_filter(s):
    if isinstance(s, str):
        # Adds space to words that
        s = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', s)
        if 'Mary' not in s and ' State' not in s:
            s = s.replace(' St', ' State')
        if 'University' not in s:
            s = s.replace('Univ', 'University')
        if 'zz' in s or 'zzz' in s or 'zzzz' in s:
            s = s.replace('zzzz', '').replace('zzz', '').replace('zz', '')
        s = clean_string(s)
        s = re_braces(s)
        s = str(s)
        s = s.replace(' ', '').lower()
        return s
    else:
        return s


def get_dataframe(path: str, columns: List = None):
    """
    Read a DataFrame from a parquet file.

    Args:
        path (str): Path to the parquet file.
        columns (List): List of columns to select (default is None).

    Returns:
        pd.DataFrame: Read DataFrame.
    """
    try:
        return pd.read_parquet(path, dtype_backend='numpy_nullable', columns=columns)
    except Exception as e:
        print(e)
        return pd.DataFrame()


def put_dataframe(df: pd.DataFrame, path: str, schema: dict = None):
    """
    Write a DataFrame to a parquet file.

    Args:
        df (pd.DataFrame): DataFrame to write.
        path (str): Path to the parquet file.
        schema (dict): Schema dictionary.

    Returns:
        None
    """
    key, file_name = path.rsplit('/', 1)
    if file_name.split('.')[1] != 'parquet':
        raise Exception("Invalid Filetype for Storage (Supported: 'parquet')")
    os.makedirs(key, exist_ok=True)
    if schema:
        for column, dtype in schema.items():
            df[column] = df[column].astype(dtype)
    df.to_parquet(f"{key}/{file_name}", schema=pa.Schema.from_pandas(df))


def create_dataframe(obj, schema: dict):
    """
    Create a DataFrame from an object with a specified schema.

    Args:
        obj: Object to convert to a DataFrame.
        schema (dict): Schema dictionary.

    Returns:
        pd.DataFrame: Created DataFrame.
    """
    df = pd.DataFrame(obj)
    for column, dtype in schema.items():
        df[column] = df[column].astype(dtype)
    return df


def df_rename_fold(df, t1_prefix, t2_prefix):
    """
    Fold two prefixed column types into one generic type in a DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.
        t1_prefix (str): Prefix for the first type of columns.
        t2_prefix (str): Prefix for the second type of columns.

    Returns:
        pd.DataFrame: DataFrame with folded columns.
    """
    try:
        t1_all_cols = [i for i in df.columns if t2_prefix not in i]
        t2_all_cols = [i for i in df.columns if t1_prefix not in i]

        t1_cols = [i for i in df.columns if t1_prefix in i]
        t2_cols = [i for i in df.columns if t2_prefix in i]
        t1_new_cols = [i.replace(t1_prefix, '') for i in df.columns if t1_prefix in i]
        t2_new_cols = [i.replace(t2_prefix, '') for i in df.columns if t2_prefix in i]

        t1_df = df[t1_all_cols].rename(columns=dict(zip(t1_cols, t1_new_cols)))
        t2_df = df[t2_all_cols].rename(columns=dict(zip(t2_cols, t2_new_cols)))

        df_out = pd.concat([t1_df, t2_df]).reset_index().drop(columns='index')
        return df_out
    except Exception as e:
        print("--df_rename_fold-- " + str(e))
        print(f"columns in: {df.columns}")
        print(f"shape: {df.shape}")
        return df


def is_pandas_none(val):
    """
    Check if a value represents a "None" in pandas.

    Args:
        val: Value to check.

    Returns:
        bool: True if the value represents a "None," False otherwise.
    """
    return str(val) in ["nan", "None", "", "none", " ", "<NA>", "NaT", "NaN"]



def get_current_week(sport_league: ESPNSportLeagueTypes, date: datetime.datetime = None):
    if date is None:
        today = datetime.datetime.utcnow()
    else:
        today = date
    sport, league = sport_league.value.split('/')
    season = find_year_for_season(sport_league)
    calendar_api = ESPNCalendarAPI(sport, league, season=season)
    for season_type in [2,3]:
        for week in calendar_api.get_weeks(season_type):
            res = calendar_api.api_request(calendar_api._core_url + f"/{calendar_api.sport.value}/leagues/{calendar_api.league}/seasons/{calendar_api.season}/types/{season_type}/weeks/{week}")
            if today < datetime.datetime.strptime(res['endDate'], "%Y-%m-%dT%H:%MZ"):
                return 18 if season_type == 3 else week
    return 18


def find_year_for_season(league: ESPNSportLeagueTypes, date: datetime.datetime = None):
    """
    Find the year for a specific season based on the league and date.

    Args:
        league (ESPNSportTypes): Type of sport.
        date (datetime.datetime): Date for the sport (default is None).

    Returns:
        int: Year for the season.
    """
    if date is None:
        today = datetime.datetime.utcnow()
    else:
        today = date
    if league not in SEASON_START_MONTH:
        raise ValueError(f'"{league}" league cannot be found!')
    start = SEASON_START_MONTH[league]['start']
    wrap = SEASON_START_MONTH[league]['wrap']
    if wrap and start - 1 <= today.month <= 12:
        return today.year + 1
    elif not wrap and start == 1 and today.month == 12:
        return today.year + 1
    elif not wrap and not start - 1 <= today.month <= 12:
        return today.year - 1
    else:
        return today.year

def get_latest_week_for_season(season_path: str) -> int:
    """Get the latest week that has been processed for a season"""
    try:
        df = get_dataframe(f"{season_path}.parquet")
        if df.empty:
            return 0
        return df['week'].max()
    except:
        return 0


def camel_to_snake(s: str) -> str:
    """Converts camelCase to snake_case"""
    return re.sub(r'((?<=[a-z0-9])[A-Z]|(?!^)(?<!_)[A-Z](?=[a-z]))', r'_\1', s).lower()