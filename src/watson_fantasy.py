import time

import requests
import pandas as pd

def _parse_ts(x):
    # Robust timestamp parsing, normalized to UTC
    return pd.to_datetime(x, errors="coerce", utc=True)

def _closest_by_ts(items, set_end_ts, *, model_type=None, tolerance=None, prefer_past=True):
    """
    Return the item whose DATA_TIMESTAMP is closest to set_end_ts.
    - model_type: filter for classifiers (e.g., 'breakout_classifier')
    - tolerance: e.g. '7D' or '36H' to drop matches that are too far away; None = no limit
    - prefer_past: if two are equally close, prefer the one <= set_end_ts
    """
    best = None
    best_delta = None

    for it in items or []:
        if model_type and it.get("MODEL_TYPE") != model_type:
            continue
        ts = _parse_ts(it.get("DATA_TIMESTAMP"))
        if pd.isna(ts):
            continue

        delta = abs(ts - set_end_ts)
        if best is None or delta < best_delta or (prefer_past and delta == best_delta and ts <= set_end_ts):
            best = it
            best_delta = delta

    if best is None:
        return None

    if tolerance is not None and best_delta > pd.to_timedelta(tolerance):
        return None

    return best

def flatten_watson_triplet(player_id, proj, clf, meta, *, tolerance="7D", prefer_past=True):
    """
    Flatten the triplet so that projections/classifiers are chosen by
    the closest DATA_TIMESTAMP to each meta SET_END (or meta DATA_TIMESTAMP).
    """
    flattened_triplets = []

    for m in meta or []:
        # Choose SET_END if present, else fall back to meta's DATA_TIMESTAMP
        set_end_ts = _parse_ts(m.get("SET_END") or m.get("DATA_TIMESTAMP"))
        if pd.isna(set_end_ts):
            continue

        set_end_str = set_end_ts.tz_convert(None).strftime("%Y-%m-%d") if set_end_ts.tzinfo else set_end_ts.strftime("%Y-%m-%d")

        closest_proj   = _closest_by_ts(proj, set_end_ts, tolerance=tolerance, prefer_past=prefer_past)
        closest_break  = _closest_by_ts(clf,  set_end_ts, model_type="breakout_classifier",           tolerance=tolerance, prefer_past=prefer_past)
        closest_bust   = _closest_by_ts(clf,  set_end_ts, model_type="bust_classifier",               tolerance=tolerance, prefer_past=prefer_past)
        closest_pwi    = _closest_by_ts(clf,  set_end_ts, model_type="play_with_injury_classifier",   tolerance=tolerance, prefer_past=prefer_past)
        closest_pwoi   = _closest_by_ts(clf,  set_end_ts, model_type="play_without_injury_classifier",tolerance=tolerance, prefer_past=prefer_past)

        flattened = {
            "actual_points": m.get("ACTUAL"),
            "set_end": set_end_str,
            "data_timestamp": m.get("DATA_TIMESTAMP"),
            "week": m.get("EVENT_WEEK"),
            "opponent_name": m.get("OPPONENT_NAME"),
            "opposition_rank": m.get("OPPOSITION_RANK"),
            "player_id": player_id,
            "full_name": m.get("FULL_NAME"),
            "position": m.get("POSITION"),
            "is_on_injured_reserve": m.get("IS_ON_INJURED_RESERVE"),
            "is_suspended": m.get("IS_SUSPENDED"),
            "is_on_bye": m.get("IS_ON_BYE"),
            "is_free_agent": m.get("IS_FREE_AGENT"),
            "current_rank": m.get("CURRENT_RANK"),
            "injury_status_date": pd.to_datetime(m.get("INJURY_STATUS_DATE"), errors="coerce").strftime("%Y-%m-%d") if m.get("INJURY_STATUS_DATE") else None,

            # Projection fields (closest to set_end)
            "projection_model_type":          closest_proj.get("MODEL_TYPE") if closest_proj else None,
            "projection_score":               closest_proj.get("SCORE_PROJECTION") if closest_proj else None,
            #"projection_score_distribution":  closest_proj.get("SCORE_DISTRIBUTION") if closest_proj else None, too massive and just not needed
            "projection_distribution_name":   closest_proj.get("DISTRIBUTION_NAME") if closest_proj else None,
            "projection_low_score":           closest_proj.get("LOW_SCORE") if closest_proj else None,
            "projection_high_score":          closest_proj.get("HIGH_SCORE") if closest_proj else None,
            "projection_simulation_projection": closest_proj.get("SIMULATION_PROJECTION") if closest_proj else None,

            # Classifiers (closest to set_end)
            "breakout_likelihood":            closest_break.get("NORMALIZED_RESULT") if closest_break else None,
            "bust_likelihood":                closest_bust.get("NORMALIZED_RESULT") if closest_bust else None,
            "play_with_injury_likelihood":    closest_pwi.get("NORMALIZED_RESULT") if closest_pwi else None,
            "play_without_injury_likelihood": closest_pwoi.get("NORMALIZED_RESULT") if closest_pwoi else None,
        }

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

def _pick_col(df: pd.DataFrame, candidates):
    """Pick the first existing column from candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _norm_pos(p):
    if p is None:
        return None
    p = str(p).upper().strip()
    if p in {"D/ST", "DST", "DEF", "DEFENSE"}:
        return "DST"
    return p

def select_watson_player_ids(proj_df: pd.DataFrame) -> pd.Index:
    """
    Build the Watson fetch set:
      - Top 100 RB, 100 WR, 32 QB, 32 TE, 32 DST, 32 K
      - Plus top 250 overall draft rank (STANDARD preferred) not already included
    Returns a pandas Index of unique player_ids (dtype: int64 where possible).
    """

    if proj_df is None or proj_df.empty:
        return pd.Index([], dtype="int64")

    df = proj_df.loc[proj_df["player_id"].notnull()].copy().drop_duplicates(['player_id'])
    df["position_norm"] = df["position"].map(_norm_pos)

    # Rank columns (be flexible with your schema)
    pos_rank_col = _pick_col(df, [
        "ppr_draft_rank", "PPR_draft_rank",      # from enriched payload
        "ppr_rank_consensus",                    # our derived consensus
        "std_draft_rank", "STANDARD_draft_rank", # fallback if PPR missing
        "current_rank", "rank"                   # last resorts
    ])
    overall_rank_col = _pick_col(df, [
        "std_draft_rank", "STANDARD_draft_rank", # STANDARD preferred for top-250
        "ppr_draft_rank", "PPR_draft_rank",
        "ppr_rank_consensus",
        "current_rank", "rank"
    ])

    if pos_rank_col is None and overall_rank_col is None:
        # No usable ranking columns
        return pd.Index(df["player_id"].drop_duplicates().astype("int64", errors="ignore"))

    # Coerce to numeric for sorting
    if pos_rank_col is not None:
        df[pos_rank_col] = pd.to_numeric(df[pos_rank_col], errors="coerce")
    if overall_rank_col is not None:
        df[overall_rank_col] = pd.to_numeric(df[overall_rank_col], errors="coerce")

    def _top_pos(pos, n):
        base = df.loc[df["position_norm"] == pos]
        if pos_rank_col is None:
            # If no positional rank, fall back to overall
            base = base.dropna(subset=[overall_rank_col]) if overall_rank_col else base
            key = overall_rank_col
        else:
            base = base.dropna(subset=[pos_rank_col])
            key = pos_rank_col
        return base.sort_values(key, ascending=True, kind="mergesort").head(n)["player_id"]

    rb_ids  = _top_pos("RB",  128)
    wr_ids  = _top_pos("WR",  128)
    qb_ids  = _top_pos("QB",   48)
    te_ids  = _top_pos("TE",   48)
    dst_ids = _top_pos("DST",  32)
    k_ids   = _top_pos("K",    32)

    # Start with the union of positional picks
    picked = pd.Index(pd.concat([rb_ids, wr_ids, qb_ids, te_ids, dst_ids, k_ids], ignore_index=True).drop_duplicates())

    # Add any remaining players in the top-350 overall not already picked
    if overall_rank_col is not None:
        top350_ids = (
            df.dropna(subset=[overall_rank_col])
              .sort_values(overall_rank_col, ascending=True, kind="mergesort")
              .head(400)["player_id"]
        )
        # set difference, keep original top-250 order
        extras = top350_ids[~top350_ids.isin(picked)]
        picked = pd.Index(pd.concat([pd.Series(picked), extras], ignore_index=True).drop_duplicates())

    # Return as int64 where possible (ESPN ids are ints)
    return picked.astype("int64")