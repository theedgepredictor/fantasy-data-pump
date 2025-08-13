import time

import requests
import pandas as pd

def flatten_watson_triplet(player_id,proj, clf, meta):
    """
    Flatten the triplet of Watson Fantasy Football projections, classifiers, and metadata into a single DataFrame.
    """
    flattened_triplets  = []
    for m in meta:
        try:
            set_end = pd.Timestamp(m.get('SET_END', m.get('DATA_TIMESTAMP', None))).strftime('%Y-%m-%d')
        except Exception as e:
            continue
        flattened_proj = [p for p in proj if pd.Timestamp(p['DATA_TIMESTAMP']).strftime('%Y-%m-%d') == set_end]
        breakout_clf = [c for c in clf if pd.Timestamp(c['DATA_TIMESTAMP']).strftime('%Y-%m-%d') == set_end and c.get('MODEL_TYPE') == 'breakout_classifier']
        bust_clf = [c for c in clf if pd.Timestamp(c['DATA_TIMESTAMP']).strftime('%Y-%m-%d') == set_end and c.get('MODEL_TYPE') == 'bust_classifier']
        play_with_injury_clf = [c for c in clf if pd.Timestamp(c['DATA_TIMESTAMP']).strftime('%Y-%m-%d') == set_end and c.get('MODEL_TYPE') == 'play_with_injury_classifier']
        play_without_injury_clf = [c for c in clf if pd.Timestamp(c['DATA_TIMESTAMP']).strftime('%Y-%m-%d') == set_end and c.get('MODEL_TYPE') == 'play_without_injury_classifier']

        flattened = {}
        flattened['actual_points'] = m.get('ACTUAL', None)
        #flattened['next_game_timestamp'] = pd.Timestamp(m.get('NEXT_GAME_TIMESTAMP', None)).strftime('%Y-%m-%d %H:%M:%S') if m.get('NEXT_GAME_TIMESTAMP') else None
        flattened['set_end'] = set_end
        flattened['data_timestamp'] = m.get('DATA_TIMESTAMP', None)
        flattened['week'] = m.get('EVENT_WEEK', None)
        flattened['opponent_name'] = m.get('OPPONENT_NAME', None)
        flattened['opposition_rank'] = m.get('OPPOSITION_RANK', None)
        flattened['player_id'] = player_id
        flattened['full_name'] = m.get('FULL_NAME', None)
        flattened['position'] = m.get('POSITION', None)
        flattened['is_on_injured_reserve'] = m.get('IS_ON_INJURED_RESERVE', None)
        flattened['is_suspended'] = m.get('IS_SUSPENDED', None)
        flattened['is_on_bye'] = m.get('IS_ON_BYE', None)
        flattened['is_free_agent'] = m.get('IS_FREE_AGENT', None)
        flattened['current_rank'] = m.get('CURRENT_RANK', None)
        flattened['injury_status_date'] = pd.Timestamp(m.get('INJURY_STATUS_DATE', None)).strftime('%Y-%m-%d') if m.get('INJURY_STATUS_DATE') else None
        flattened['projection_outside_projection'] = m.get('OUTSIDE_PROJECTION', None)
        flattened['projection_model_type'] = flattened_proj[0].get('MODEL_TYPE', None) if flattened_proj else None
        flattened['projection_score'] = flattened_proj[0].get('SCORE_PROJECTION', None) if flattened_proj else None
        flattened['projection_score_distribution'] = flattened_proj[0].get('SCORE_DISTRIBUTION', None) if flattened_proj else None
        flattened['projection_distribution_name'] = flattened_proj[0].get('DISTRIBUTION_NAME', None) if flattened_proj else None
        flattened['projection_low_score'] = flattened_proj[0].get('LOW_SCORE', None) if flattened_proj else None
        flattened['projection_high_score'] = flattened_proj[0].get('HIGH_SCORE', None) if flattened_proj else None
        flattened['projection_simulation_projection'] = flattened_proj[0].get('SIMULATION_PROJECTION', None) if flattened_proj else None
        flattened['breakout_likelihood'] = breakout_clf[0].get('NORMALIZED_RESULT', None) if breakout_clf else None
        flattened['bust_likelihood'] = bust_clf[0].get('NORMALIZED_RESULT', None) if bust_clf else None
        flattened['play_with_injury_likelihood'] = play_with_injury_clf[0].get('NORMALIZED_RESULT', None) if play_with_injury_clf else None
        flattened['play_without_injury_likelihood'] = play_without_injury_clf[0].get('NORMALIZED_RESULT', None) if play_without_injury_clf else None
        flattened_triplets.append(flattened)
    return flattened_triplets





BASE_WATSON = "https://watsonfantasyfootball.espn.com/espnpartner/dallas"
def _get_json(url: str, session: requests.Session | None = None):
    try:
        s = session or requests.Session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        }
        resp = s.get(url=url, headers=headers)
        return resp.json()
    except:
        return []
def fetch_watson_triplet(season: int, espn_id, session: requests.Session | None = None):
    proj_url = f"{BASE_WATSON}/projections/projections_{espn_id}_ESPNFantasyFootball_{season}.json"
    clf_url = f"{BASE_WATSON}/classifiers/classifiers_{espn_id}_ESPNFantasyFootball_{season}.json"
    meta_url = f"{BASE_WATSON}/players/players_{espn_id}_ESPNFantasyFootball_{season}.json"
    proj = _get_json(proj_url, session=session) or []
    clf  = _get_json(clf_url, session=session) or []
    meta = _get_json(meta_url, session=session) or []
    return proj, clf, meta

def watson_projections(season, player_ids):
    session = requests.Session()
    for espn_id in player_ids:
        proj, clf, meta = fetch_watson_triplet(season, espn_id, session)

        record = flatten_watson_triplet(proj, clf, meta)